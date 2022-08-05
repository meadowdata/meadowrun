from __future__ import annotations

import asyncio
import base64
import dataclasses
import io
import os
import pickle
import traceback
import uuid
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    TypeVar,
)

import botocore.exceptions
import cloudpickle
import kubernetes_asyncio.client as kubernetes_client
import kubernetes_asyncio.client.exceptions as kubernetes_client_exceptions
import kubernetes_asyncio.config as kubernetes_config
import kubernetes_asyncio.watch as kubernetes_watch

import meadowrun.func_worker_storage_helper
from meadowrun.credentials import KubernetesSecretRaw
from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_PASSWORD,
    MEADOWRUN_STORAGE_USERNAME,
    get_storage_client_from_args,
)

if TYPE_CHECKING:
    from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import (
    Job,
    ProcessState,
    PyFunctionJob,
    QualifiedFunctionName,
)
from meadowrun.run_job_core import Host, JobCompletion, MeadowrunException
from meadowrun.run_job_local import (
    _get_credentials_for_docker,
    _get_credentials_sources,
    _string_pairs_to_dict,
)

_T = TypeVar("_T")
_U = TypeVar("_U")


# kubernetes_asyncio.watch.Watch looks at the docstring to figure out if the function
# passed to Watch takes a follow or watch argument. This results in the wrong value for
# read_namespaced_pod_log, so we just monkey-patch the function here
_orig_get_watch_argument_name = kubernetes_watch.Watch.get_watch_argument_name


def _new_get_watch_argument_name(watch: kubernetes_watch.Watch, func: Callable) -> str:
    if getattr(func, "__name__") == "read_namespaced_pod_log":
        return "follow"
    return _orig_get_watch_argument_name(watch, func)


kubernetes_watch.watch.Watch.get_watch_argument_name = _new_get_watch_argument_name


def _indexed_map_worker(
    total_num_tasks: int,
    num_workers: int,
    function: Callable[[_T], _U],
    storage_bucket: str,
    file_prefix: str,
    result_highest_pickle_protocol: int,
) -> None:
    """
    This is a worker function to help with running a run_map. This worker assumes that
    JOB_COMPLETION_INDEX is set, which Kubernetes will set for indexed completion jobs.
    This worker assumes task arguments are accessible via
    meadowrun.func_worker_storage_helper.STORAGE_CLIENT and will just complete all of
    the tasks where task_index % num_workers == current worker index.
    """

    current_worker_index = int(os.environ["JOB_COMPLETION_INDEX"])
    storage_client = meadowrun.func_worker_storage_helper.STORAGE_CLIENT
    if storage_client is None:
        raise ValueError("Cannot call _indexed_map_worker without a storage client")

    result_pickle_protocol = min(
        result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
    )

    i = current_worker_index
    while i < total_num_tasks:
        state_filename = f"{file_prefix}.taskstate{i}"
        result_filename = f"{file_prefix}.taskresult{i}"

        with io.BytesIO() as buffer:
            storage_client.download_fileobj(
                Bucket=storage_bucket, Key=f"{file_prefix}.taskarg{i}", Fileobj=buffer
            )
            buffer.seek(0)
            arg = pickle.load(buffer)

        try:
            result = function(arg)
        except Exception as e:
            # first print the exception for the local log file
            traceback.print_exc()

            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))

            # now send back result
            with io.BytesIO() as buffer:
                buffer.write("PYTHON_EXCEPTION".encode("utf-8"))
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=state_filename
                )
            with io.BytesIO() as buffer:
                pickle.dump(
                    (str(type(e)), str(e), tb), buffer, protocol=result_pickle_protocol
                )
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=result_filename
                )
        else:
            # send back results
            with io.BytesIO() as buffer:
                buffer.write("SUCCEEDED".encode("utf-8"))
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=state_filename
                )
            with io.BytesIO() as buffer:
                pickle.dump(result, buffer, protocol=result_pickle_protocol)
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Fileobj=buffer, Bucket=storage_bucket, Key=result_filename
                )

        i += num_workers


def _get_job_completion(
    storage_client: Any,
    storage_bucket: Optional[str],
    file_prefix: str,
    file_suffix: str,
    job_spec_type: str,
    return_code: int,
) -> JobCompletion[Any]:
    """
    Creates a JobCompletion object based on the return code, and .state and .result
    files if they're available.

    file_suffix is used by indexed completion jobs to distinguish between the
    completions of different workers, this should be set to the job completion index.
    """
    if return_code != 0:
        raise MeadowrunException(
            ProcessState(
                state=ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
                return_code=return_code,
            )
        )

    # TODO get the name of the pod that ran the job? It won't be super useful
    # because we delete the pod right away
    public_address = "kubernetes"

    # if we don't have a storage client, there's no way to get results back, so
    # we just return None
    if storage_client is None:
        return JobCompletion(
            None,
            ProcessState.ProcessStateEnum.SUCCEEDED,
            "",
            return_code,
            public_address,
        )

    if storage_bucket is None:
        raise ValueError(
            "Cannot provide a storage_client and not provide a storage_bucket"
        )

    with io.BytesIO() as buffer:
        try:
            storage_client.download_fileobj(
                Bucket=storage_bucket,
                Key=f"{file_prefix}.state{file_suffix}",
                Fileobj=buffer,
            )
        except botocore.exceptions.ClientError as e:
            # if we were expecting a state file but didn't get it we need to
            # throw an exception
            if (
                getattr(e, "response", {}).get("Error", {}).get("Code", None) == "404"
                and job_spec_type == "py_function"
            ):
                raise

            # if we're okay with not getting a state file, just return that we
            # succeeded, and we don't have a result
            return JobCompletion(
                None,
                ProcessState.ProcessStateEnum.SUCCEEDED,
                "",
                return_code,
                public_address,
            )

        buffer.seek(0)
        state_string = buffer.read().decode("utf-8")
        if state_string == "SUCCEEDED":
            state = ProcessState.ProcessStateEnum.SUCCEEDED
        elif state_string == "PYTHON_EXCEPTION":
            state = ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
        else:
            raise ValueError(f"Unknown state string: {state_string}")

    # if we got a state string, we should have a result file
    with io.BytesIO() as buffer:
        storage_client.download_fileobj(
            Bucket=storage_bucket,
            Key=f"{file_prefix}.result{file_suffix}",
            Fileobj=buffer,
        )
        buffer.seek(0)
        result = pickle.load(buffer)

    if state == ProcessState.ProcessStateEnum.PYTHON_EXCEPTION:
        # TODO very weird that we're re-pickling result here. Also, we should
        # raise all of the exceptions if there are more than 1, not just the
        # first one we see
        raise MeadowrunException(
            ProcessState(
                state=state,
                pickled_result=pickle.dumps(result),
                return_code=return_code,
            )
        )

    return JobCompletion(result, state, "", return_code, public_address)


@dataclasses.dataclass(frozen=True)
class Kubernetes(Host):
    """
    Specifies a Kubernetes cluster to run a Meadowrun job on. resources_required is
    optional with the Kubernetes Host.

    Attributes:
        storage_bucket: Together, the storage_* arguments specify an S3-compatible
            object store that Meadowrun will use for sending inputs/outputs from the
            Kubernetes job and back. For run_command, the S3-compatible object store is
            not used (even if it is specified). For run_function on a pickled function
            (e.g. a reference to a python function or a lambda) or where args/kwargs is
            specified, the object store is required, which means that storage_bucket
            must be specified. For run_function on a string referencing a function, the
            object store is optional, which means that if storage_bucket is provided,
            then it will be used to return results. If storage_bucket is not provided,
            the result of the function will not be available.

            In all cases, if storage_bucket is None, all of the other storage_*
            arguments will be ignored.

            Together, the storage_* arguments should be configured so that:

            ```python
            import boto3
            boto3.Session(
                aws_access_key_id=storage_username,
                aws_secret_access_key=storage_password
            ).client(
                "s3", endpoint_url=storage_endpoint_url
            ).download_file(
                Bucket=storage_bucket, Key="test.file", Filename="test.file"
            )
            ```

            works. `storage_username` and `storage_password` should be the values
            provided by `storage_username_password_secret`. (boto3 is built to be used
            with AWS S3, but it should work with any S3-compatible object store like
            Minio, Ceph, etc.)
        storage_file_prefix: Part of the specification of the S3-compatible object store
            to use (see storage_bucket). The prefix will be used when naming the
            Meadowrun-generated files in the object store. Defaults to "". For non empty
            string values, this should usually end with "/". If you leave off the "/",
            the job_id will just be concatenated to the string, e.g. "foo12345..."
            rather than "foo/12345"
        storage_endpoint_url: Part of the specification of the S3-compatible object
            store to use (see storage_bucket).
        storage_endpoint_url_in_cluster: Part of the specification of the S3-compatible
            object store to use (see storage_bucket). Defaults to None which means use
            storage_endpoint_url. You can set this to a different URL if you need to use
            a different URL from inside the Kubernetes cluster to access the storage
            endpoint
        storage_username_password_secret: Part of the specification of the S3-compatible
            object store to use (see storage_bucket). This should be the name of a
            Kubernetes secret that has a "username" and "password" key, where the
            username and password can be used to authenticate with the storage API.
        kube_config_context: Specifies the kube config context to use. Default is None
            which means use the current context (i.e. `kubectl config current-context`)
        kubernetes_namespace: The Kubernetes namespace that Meadowrun will create Jobs
            in. This should usually not be left to the default value ("default") for any
            "real" workloads.
    """

    storage_bucket: Optional[str] = None
    storage_file_prefix: str = ""
    storage_endpoint_url: Optional[str] = None
    storage_endpoint_url_in_cluster: Optional[str] = None
    storage_username_password_secret: Optional[str] = None
    kube_config_context: Optional[str] = None
    kubernetes_namespace: str = "default"

    async def _get_storage_client(self) -> Any:
        # get the storage client

        # this is kind of weird, this should be called before any Kubernetes function,
        # but for now, _get_storage_client is always the first thing that's called
        await kubernetes_config.load_kube_config(context=self.kube_config_context)

        if self.storage_bucket is not None:
            if self.storage_username_password_secret is not None:
                secret_data = await _get_kubernetes_secret(
                    self.kubernetes_namespace,
                    self.storage_username_password_secret,
                )
            else:
                secret_data = {}

            return get_storage_client_from_args(
                self.storage_endpoint_url,
                secret_data.get("username", None),
                secret_data.get("password", None),
            )

        return None

    def _prepare_command(
        self, job: Job, job_spec_type: str, storage_client: Any, file_prefix: str
    ) -> List[str]:
        if job_spec_type == "py_command":
            # TODO we should set all of the storage-related parameters in the
            # environment variables so that the code in result_request can send back a
            # result if our code is "meadowrun-aware" and we can set the context
            # variables
            if job.py_command.pickled_context_variables:
                raise NotImplementedError(
                    "Context variables aren't supported for run_command on Kubernetes "
                    "yet. Please open an issue at "
                    "https://github.com/meadowdata/meadowrun/issues describing your use"
                    " case"
                )
            if not job.py_command.command_line:
                raise ValueError("command_line must have at least one string")
            return list(job.py_command.command_line)
        elif job_spec_type == "py_function":
            # a copy/paste-ish of run_job_local:_prepare_py_function
            function = job.py_function
            function_spec = function.WhichOneof("function_spec")

            command = [
                "python",
                "-m",
                "meadowrun.func_worker_storage",
                "--result-highest-pickle-protocol",
                str(job.result_highest_pickle_protocol),
            ]
            if self.storage_bucket:
                command.extend(["--storage-bucket", self.storage_bucket])
            # a bit sneaky--on the remote side, the storage-file-prefix includes the
            # job_id
            command.extend(["--storage-file-prefix", file_prefix])
            if self.storage_endpoint_url_in_cluster:
                command.extend(
                    ["--storage-endpoint-url", self.storage_endpoint_url_in_cluster]
                )
            elif self.storage_endpoint_url:
                command.extend(["--storage-endpoint-url", self.storage_endpoint_url])

            # prepare function
            if function_spec == "qualified_function_name":
                if self.storage_bucket is None:
                    print(
                        "Warning, storage_bucket was not provided for a function, "
                        "result will always be None"
                    )
                command.extend(
                    [
                        "--module-name",
                        function.qualified_function_name.module_name,
                        "--function-name",
                        function.qualified_function_name.function_name,
                    ]
                )
            elif function_spec == "pickled_function":
                if self.storage_bucket is None:
                    raise ValueError(
                        "Cannot use a pickled function without providing a "
                        "storage_bucket. Please either specify the function to run with"
                        " a string or provide a storage_bucket"
                    )
                if function.pickled_function is None:
                    raise ValueError("pickled_function cannot be None")
                with io.BytesIO() as buffer:
                    buffer.write(function.pickled_function)
                    buffer.seek(0)
                    storage_client.upload_fileobj(
                        Fileobj=buffer,
                        Bucket=self.storage_bucket,
                        Key=f"{file_prefix}.function",
                    )
                command.append("--has-pickled-function")

            # prepare arguments
            if job.py_function.pickled_function_arguments:
                with io.BytesIO() as buffer:
                    buffer.write(function.pickled_function_arguments)
                    buffer.seek(0)
                    storage_client.upload_fileobj(
                        Fileobj=buffer,
                        Bucket=self.storage_bucket,
                        Key=f"{file_prefix}.arguments",
                    )
                command.append("--has-pickled-arguments")

            return command
        else:
            raise ValueError(f"Unknown job_spec {job_spec_type}")

    async def run_job(
        self, resources_required: Optional[ResourcesInternal], job: Job
    ) -> JobCompletion[Any]:
        # TODO add support for all of these features
        if resources_required is not None:
            raise NotImplementedError(
                "Specifying Resources for a Kubernetes job is not yet supported"
            )
        if job.sidecar_containers:
            raise NotImplementedError(
                "Sidecar containers are not yet supported for Kubernetes"
            )
        if job.ports:
            raise NotImplementedError("Ports are not yet supported for Kubernetes")

        job_completions = await self._run_job_helper(
            await self._get_storage_client(), job, None
        )
        if len(job_completions) != 1:
            raise ValueError(
                "Unexpected, requested a single job but got back "
                f"{len(job_completions)} job_completions"
            )
        return job_completions[0]

    async def _run_job_helper(
        self,
        storage_client: Any,
        job: Job,
        indexed_completions: Optional[int],
    ) -> List[JobCompletion[Any]]:
        """
        A wrapper around _run_kubernetes_job. Main tasks at this level are:
        - Interpret the Job object into a container + command that Kubernetes can run
        - Upload arguments to S3 and download results from S3, including for indexed job
          completions.
        """

        # This code encompasses everything that happens in SshHost.run_job and
        # run_job_local

        # detect any unsupported Jobs

        if (
            job.WhichOneof("code_deployment") != "server_available_folder"
            or len(job.server_available_folder.code_paths) > 0
        ):
            raise NotImplementedError(
                "code_deployment is not supported in run_job_container_local"
            )

        interpreter_deployment_type = job.WhichOneof("interpreter_deployment")
        if interpreter_deployment_type == "container_at_digest":
            image_repository_name = job.container_at_digest.repository
            image_name = (
                f"{job.container_at_digest.repository}@{job.container_at_digest.digest}"
            )
        elif interpreter_deployment_type == "container_at_tag":
            image_repository_name = job.container_at_tag.repository
            image_name = f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
        elif interpreter_deployment_type == "server_available_container":
            image_repository_name = None
            image_name = f"{job.server_available_container.image_name}"
        else:
            raise NotImplementedError(
                "Only pre-built containers are supported for interpreter_deployment in"
                " run_job_container_local"
            )

        # get any image pull secrets
        # first, get all available credentials sources from the JobToRun
        image_pull_secret_name = None
        if image_repository_name is not None:
            image_pull_secret = await _get_credentials_for_docker(
                image_repository_name, _get_credentials_sources(job), None
            )
            if image_pull_secret is not None:
                if not isinstance(image_pull_secret, KubernetesSecretRaw):
                    raise NotImplementedError(
                        "Using anything other than KubernetesSecret with a Kubernetes "
                        f"host has not been implemented, {type(image_pull_secret)} was "
                        "provided"
                    )
                image_pull_secret_name = image_pull_secret.secret_name

        file_prefix = f"{self.storage_file_prefix}{job.job_id}"
        try:
            try:
                # run the job

                job_spec_type = job.WhichOneof("job_spec")
                if job_spec_type is None:
                    raise ValueError("Unexpected: job_spec is None")

                command = self._prepare_command(
                    job, job_spec_type, storage_client, file_prefix
                )

                environment_variables = {"PYTHONUNBUFFERED": "1"}
                environment_variables.update(
                    **_string_pairs_to_dict(job.environment_variables)
                )

                return_codes = await _run_kubernetes_job(
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    environment_variables,
                    self.storage_username_password_secret,
                    image_pull_secret_name,
                    indexed_completions,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                raise MeadowrunException(
                    ProcessState(
                        state=ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
                    )
                ) from e

            if indexed_completions:
                file_suffixes: Iterable[str] = (
                    str(i) for i in range(indexed_completions)
                )
            else:
                file_suffixes = [""]

            return [
                _get_job_completion(
                    storage_client,
                    self.storage_bucket,
                    file_prefix,
                    file_suffix,
                    job_spec_type,
                    return_code,
                )
                for file_suffix, return_code in zip(file_suffixes, return_codes)
            ]
        finally:
            # TODO we should separately periodically clean up these files in case we
            # aren't able to execute this finally block
            if storage_client is not None:
                for suffix in ["state", "result", "function", "arguments"]:
                    try:
                        storage_client.delete_object(
                            Bucket=self.storage_bucket, Key=f"{file_prefix}.{suffix}"
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass

    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
    ) -> Sequence[_U]:
        """
        This implementation depends on Kubernetes indexed job completions
        (https://kubernetes.io/docs/tasks/job/indexed-parallel-processing-static/)
        """
        # TODO add support for all of these features
        if resources_required_per_task is not None:
            raise NotImplementedError(
                "Specifying Resources for a Kubernetes job is not yet supported"
            )
        if job_fields["sidecar_containers"]:
            raise NotImplementedError(
                "Sidecar containers are not yet supported for Kubernetes"
            )
        if job_fields["ports"]:
            raise NotImplementedError("Ports are not yet supported for Kubernetes")

        storage_client = await self._get_storage_client()
        if storage_client is None:
            raise ValueError(
                "storage_bucket and other storage_* parameters must be specified to use"
                " Kubernetes with run_map"
            )

        job_id = str(uuid.uuid4())

        file_prefix = f"{self.storage_file_prefix}{job_id}"

        for i, arg in enumerate(args):
            with io.BytesIO() as buffer:
                pickle.dump(arg, buffer, protocol=pickle_protocol)
                buffer.seek(0)
                storage_client.upload_fileobj(
                    Bucket=self.storage_bucket,
                    Key=f"{file_prefix}.taskarg{i}",
                    Fileobj=buffer,
                )

        indexed_map_worker_args = (
            len(args),
            num_concurrent_tasks,
            function,
            self.storage_bucket,
            file_prefix,
            pickle.HIGHEST_PROTOCOL,
        )

        # we don't care about the worker completions--if they had an error, an Exception
        # will be raised, and the workers just return None
        await self._run_job_helper(
            storage_client,
            Job(
                job_id=job_id,
                py_function=PyFunctionJob(
                    qualified_function_name=QualifiedFunctionName(
                        module_name=__name__, function_name=_indexed_map_worker.__name__
                    ),
                    pickled_function_arguments=cloudpickle.dumps(
                        (indexed_map_worker_args, None), protocol=pickle_protocol
                    ),
                ),
                **job_fields,
            ),
            num_concurrent_tasks,
        )

        results = []
        for i in range(len(args)):
            with io.BytesIO() as buffer:
                storage_client.download_fileobj(
                    Bucket=self.storage_bucket,
                    Key=f"{file_prefix}.taskstate{i}",
                    Fileobj=buffer,
                )
                buffer.seek(0)
                state_string = buffer.read().decode("utf-8")
                if state_string == "SUCCEEDED":
                    state = ProcessState.ProcessStateEnum.SUCCEEDED
                elif state_string == "PYTHON_EXCEPTION":
                    state = ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
                else:
                    raise ValueError(f"Unknown state string: {state_string}")

            with io.BytesIO() as buffer:
                storage_client.download_fileobj(
                    Bucket=self.storage_bucket,
                    Key=f"{file_prefix}.taskresult{i}",
                    Fileobj=buffer,
                )
                buffer.seek(0)
                result = pickle.load(buffer)

            if state == ProcessState.ProcessStateEnum.PYTHON_EXCEPTION:
                # TODO very weird that we're re-pickling result here. Also, we should
                # raise all of the exceptions if there are more than 1, not just the
                # first one we see
                raise MeadowrunException(
                    ProcessState(
                        state=state, pickled_result=pickle.dumps(result), return_code=0
                    )
                )

            results.append(result)

        return results


async def _get_pods_for_job(
    core_api: kubernetes_client.CoreV1Api,
    kubernetes_namespace: str,
    job_id: str,
    pod_generate_names: List[str],
) -> List[kubernetes_client.V1Pod]:
    """
    When you launch a Kubernetes job, one or more pods get created. In our case, we
    should only ever get one pod for regular jobs, and one pod for each index in indexed
    completion jobs, because we always set parallelism = completions, and we've
    configured 0 retries.

    For regular jobs, the pod created will be named <job_id>-<random string>. The pod's
    metadata has a generate_name, which will be equal to <job_id>-.

    For indexed completion jobs, there will be a pod for each index with a generate_name
    of <job_id>-<index>-.

    pod_generate_names should have the list of generate_names that we expect to see for
    the specified job.

    Returns a pod object corresponding to each pod_generate_names, in the same order as
    pod_generate_names.
    """

    pod_generate_names_set = set(pod_generate_names)
    results: Dict[str, kubernetes_client.V1Pod] = {}
    i = 0

    while True:
        pods = await core_api.list_namespaced_pod(
            kubernetes_namespace, label_selector=f"job-name={job_id}"
        )
        for pod in pods.items:
            generate_name = pod.metadata.generate_name

            if generate_name not in pod_generate_names_set:
                raise ValueError(
                    f"Unexpected pod {pod.metadata.name} with generate name "
                    f"{generate_name} found"
                )

            if (
                generate_name in results
                and results[generate_name].metadata.name != pod.metadata.name
            ):
                # TODO we may need to change this if we add e.g. retries
                raise ValueError(
                    "Unexpected multiple pods with the same generate name "
                    f"{generate_name} found: {results[generate_name].metadata.name}, "
                    f"{pod.metadata.name}"
                )

            results[generate_name] = pod

        if len(results) >= len(pod_generate_names):
            break

        if i > 15:
            raise TimeoutError(
                "Waited >15s, but pods with the following generate names were not "
                "created: "
                + ", ".join(p for p in pod_generate_names if p not in results)
            )

        if i == 0:
            print(f"Waiting for pods to be created for the job {job_id}")

        await asyncio.sleep(1.0)

        i += 1

    return [results[generate_name] for generate_name in pod_generate_names]


def _get_main_container_state(
    container_statuses: Optional[List[kubernetes_client.V1ContainerStatus]],
    job_id: str,
    pod_name: str,
) -> Optional[kubernetes_client.V1ContainerState]:
    if container_statuses is None or len(container_statuses) == 0:
        return None

    main_container_statuses = [s for s in container_statuses if s.name == "main"]
    if len(main_container_statuses) == 0:
        raise ValueError(
            f"The job {job_id} has a pod {pod_name} but there is no `main` container"
        )
    if len(main_container_statuses) > 1:
        raise ValueError(
            f"The job {job_id} has a pod {pod_name} but there is more than one `main` "
            f"container"
        )
    return main_container_statuses[0].state


async def _stream_pod_logs_and_get_exit_code(
    core_api: kubernetes_client.CoreV1Api,
    job_id: str,
    kubernetes_namespace: str,
    pod: kubernetes_client.V1Pod,
) -> int:
    """
    This function waits for the specified pod to start running, streams the logs from
    that pod into our local stdout, and then waits for the pod to terminate. Then we
    return the exit code of the pod.
    """
    pod_name = pod.metadata.name

    # The first step is to wait for the pod to start running, because we can't stream
    # logs until the pod is in a running state. The happy path is that our pod is in the
    # "waiting" state because we're either waiting for the image to get pulled or we're
    # waiting for nodes to become available to run our job. In that case, we'll wait up
    # to 7 minutes.
    #
    # The unhappy path is that something has gone wrong which Kubernetes expresses as
    # waiting infinitely, rather than a failure. E.g. if our image spec is invalid. In
    # that case we'll only wait 15 seconds, as it doesn't make sense to expect that that
    # would change.

    i = 0
    wait_until = 15
    max_wait_until = 60 * 7
    main_container_state: kubernetes_client.V1ContainerState = (
        _get_main_container_state(pod.status.container_statuses, job_id, pod_name)
    )
    prev_additional_info = None
    while main_container_state is None or (
        main_container_state.running is None and main_container_state.terminated is None
    ):
        is_happy_path = False
        if (
            main_container_state is not None
            and main_container_state.waiting is not None
        ):
            additional_info = f": {main_container_state.waiting.reason}"
            if main_container_state.waiting.message is not None:
                additional_info = (
                    f"{additional_info} {main_container_state.waiting.message}"
                )
            elif main_container_state.waiting.reason == "ContainerCreating":
                # TODO Kubernetes unfortunately doesn't distinguish between waiting for
                # an image to get pulled vs waiting for a free node in this field, we
                # need to use the Events API to get that information. At some point it
                # may come through in this waiting.reason field, though:
                # https://github.com/kubernetes/kubernetes/issues/19077
                additional_info = (
                    f"{additional_info} (pulling image or waiting for available nodes)"
                )
                is_happy_path = True
        else:
            additional_info = ""
        if additional_info != prev_additional_info:
            print(f"Waiting for pod {pod_name} to start running{additional_info}")
            prev_additional_info = additional_info
        await asyncio.sleep(1.0)
        i += 1
        if is_happy_path:
            wait_until += 1
        if i > wait_until or i > max_wait_until:
            raise TimeoutError(
                f"Waited >{i} seconds for the container of job {job_id} in pod "
                f"{pod_name} to start running"
            )

        pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        main_container_state = _get_main_container_state(
            pod.status.container_statuses, job_id, pod_name
        )

    # Now our pod is running, so we can stream the logs

    async with kubernetes_watch.Watch() as w:
        async for line in w.stream(
            core_api.read_namespaced_pod_log,
            name=pod_name,
            namespace=kubernetes_namespace,
        ):
            print(line, end="")

    # Once this stream ends, we know the pod is completed, but sometimes it takes some
    # time for Kubernetes to report that the pod has completed. So we poll until the pod
    # is reported as terminated.

    i = 0
    pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
    main_container_state = _get_main_container_state(
        pod.status.container_statuses, job_id, pod_name
    )
    while main_container_state is None or main_container_state.running is not None:
        print("Waiting for the pod to stop running")
        await asyncio.sleep(1.0)
        i += 1
        if i > 15:
            raise TimeoutError(
                f"Unexpected. The job {job_id} has a pod {pod_name}, and there "
                "are no more logs for this pod, but the pod still seems to be "
                "running"
            )
        pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        main_container_state = _get_main_container_state(
            pod.status.container_statuses, job_id, pod_name
        )

    return main_container_state.terminated.exit_code


async def _get_kubernetes_secret(
    kubernetes_namespace: str, secret_name: str
) -> Dict[str, str]:
    async with kubernetes_client.ApiClient() as api_client:
        core_api = kubernetes_client.CoreV1Api(api_client)
        result = await core_api.read_namespaced_secret(
            secret_name, kubernetes_namespace
        )

    return {
        key: base64.b64decode(value).decode("utf-8")
        for key, value in result.data.items()
    }


async def _run_kubernetes_job(
    job_id: str,
    kubernetes_namespace: str,
    image: str,
    args: List[str],
    environment_variables: Dict[str, str],
    storage_username_password_secret: Optional[str],
    image_pull_secret_name: Optional[str],
    indexed_completions: Optional[int],
) -> Sequence[int]:
    """
    Runs the specified job on Kubernetes, waits for it to complete, and returns the exit
    code
    """

    # create the job

    environment = [
        kubernetes_client.V1EnvVar(name=key, value=value)
        for key, value in environment_variables.items()
    ]

    if storage_username_password_secret is not None:
        if MEADOWRUN_STORAGE_USERNAME not in environment_variables:
            environment.append(
                kubernetes_client.V1EnvVar(
                    name=MEADOWRUN_STORAGE_USERNAME,
                    value_from=kubernetes_client.V1EnvVarSource(
                        secret_key_ref=kubernetes_client.V1SecretKeySelector(
                            key="username",
                            name=storage_username_password_secret,
                            optional=False,
                        )
                    ),
                )
            )
        if MEADOWRUN_STORAGE_PASSWORD not in environment_variables:
            environment.append(
                kubernetes_client.V1EnvVar(
                    name=MEADOWRUN_STORAGE_PASSWORD,
                    value_from=kubernetes_client.V1EnvVarSource(
                        secret_key_ref=kubernetes_client.V1SecretKeySelector(
                            key="password",
                            name=storage_username_password_secret,
                            optional=False,
                        )
                    ),
                )
            )

    if indexed_completions:
        additional_job_spec_parameters = {
            "completions": indexed_completions,
            "parallelism": indexed_completions,
            "completion_mode": "Indexed",
        }
    else:
        additional_job_spec_parameters = {}

    if image_pull_secret_name:
        additional_pod_spec_parameters = {
            "image_pull_secrets": [
                kubernetes_client.V1LocalObjectReference(image_pull_secret_name)
            ]
        }
    else:
        additional_pod_spec_parameters = {}

    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    body = kubernetes_client.V1Job(
        metadata=kubernetes_client.V1ObjectMeta(name=job_id),
        spec=kubernetes_client.V1JobSpec(
            backoff_limit=0,
            template=kubernetes_client.V1PodTemplateSpec(
                spec=kubernetes_client.V1PodSpec(
                    containers=[
                        kubernetes_client.V1Container(
                            name="main", image=image, args=args, env=environment
                        )
                    ],
                    restart_policy="Never",
                    **additional_pod_spec_parameters,
                )
            ),
            **additional_job_spec_parameters,
        ),
    )

    async with kubernetes_client.ApiClient() as api_client:
        batch_api = kubernetes_client.BatchV1Api(api_client)
        try:
            if indexed_completions:
                version = await kubernetes_client.VersionApi(api_client).get_code()
                if (int(version.major), int(version.minor)) < (1, 21):
                    raise ValueError(
                        "run_map with Kubernetes is only supported on version 1.21 or "
                        "higher"
                    )

            # this returns a result, but we don't really need anything from it
            await batch_api.create_namespaced_job(kubernetes_namespace, body)

            core_api = kubernetes_client.CoreV1Api(api_client)

            # Now that we've created the job, wait for the pods to be created

            if indexed_completions:
                pod_generate_names = [
                    f"{job_id}-{i}-" for i in range(indexed_completions)
                ]
            else:
                pod_generate_names = [f"{job_id}-"]

            pods = await _get_pods_for_job(
                core_api, kubernetes_namespace, job_id, pod_generate_names
            )

            pod_names = [pod.metadata.name for pod in pods]
            print(f"Created pod(s) {', '.join(pod_names)} for job {job_id}")

            # Now that the pods ahve been created, stream their logs and get their exit
            # codes

            return await asyncio.gather(
                *[
                    asyncio.create_task(
                        _stream_pod_logs_and_get_exit_code(
                            core_api, job_id, kubernetes_namespace, pod
                        )
                    )
                    for pod in pods
                ]
            )
        finally:
            # TODO we should separately periodically clean up these jobs/pods in case we
            # aren't able to execute this finally block
            try:
                await batch_api.delete_namespaced_job(
                    job_id, kubernetes_namespace, propagation_policy="Foreground"
                )
            except kubernetes_client_exceptions.ApiException as e:
                print(f"Warning, error cleaning up job: {e}")
