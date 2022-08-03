from __future__ import annotations

import asyncio
import base64
import dataclasses
import io
import pickle
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import botocore.exceptions
import kubernetes.client

from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_PASSWORD,
    MEADOWRUN_STORAGE_USERNAME,
    get_storage_client_from_args,
)

if TYPE_CHECKING:
    from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import Job, ProcessState
from meadowrun.run_job_core import Host, JobCompletion, MeadowrunException
from meadowrun.run_job_local import _string_pairs_to_dict


_T = TypeVar("_T")
_U = TypeVar("_U")


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
        # This code encompasses everything that happens in SshHost.run_job and
        # run_job_local

        # TODO add support for resources
        if resources_required is not None:
            raise NotImplementedError(
                "Specifying Resources for a Kubernetes job is not yet supported"
            )

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
            image_name = (
                f"{job.container_at_digest.repository}@{job.container_at_digest.digest}"
            )
        elif interpreter_deployment_type == "container_at_tag":
            image_name = f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
        elif interpreter_deployment_type == "server_available_container":
            image_name = f"{job.server_available_container.image_name}"
        else:
            raise NotImplementedError(
                "Only pre-built containers are supported for interpreter_deployment in"
                " run_job_container_local"
            )

        storage_client = None
        file_prefix = f"{self.storage_file_prefix}{job.job_id}"
        try:
            try:
                kubernetes.config.load_kube_config(context=self.kube_config_context)

                # get the storage client

                if self.storage_bucket is not None:
                    if self.storage_username_password_secret is not None:
                        secret_data = _get_kubernetes_secret(
                            self.kubernetes_namespace,
                            self.storage_username_password_secret,
                        )
                    else:
                        secret_data = {}

                    storage_client = get_storage_client_from_args(
                        self.storage_endpoint_url,
                        secret_data.get("username", None),
                        secret_data.get("password", None),
                    )

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

                return_code = await _run_kubernetes_job(
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    environment_variables,
                    self.storage_username_password_secret,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                raise MeadowrunException(
                    ProcessState(
                        state=ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
                    )
                ) from e

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

            with io.BytesIO() as buffer:
                try:
                    storage_client.download_fileobj(
                        Bucket=self.storage_bucket,
                        Key=f"{file_prefix}.state",
                        Fileobj=buffer,
                    )
                except botocore.exceptions.ClientError as e:
                    # if we were expecting a state file but didn't get it we need to
                    # throw an exception
                    if (
                        getattr(e, "response", {}).get("Error", {}).get("Code", None)
                        == "404"
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
                    Bucket=self.storage_bucket,
                    Key=f"{file_prefix}.result",
                    Fileobj=buffer,
                )
                buffer.seek(0)
                result = pickle.load(buffer)

            return JobCompletion(result, state, "", return_code, public_address)
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
        # TODO add support for resources
        if resources_required_per_task is not None:
            raise NotImplementedError(
                "Specifying Resources for a Kubernetes job is not yet supported"
            )

        raise NotImplementedError("run_map is not implemented for Kubernetes yet")


async def _retry(
    function: Callable[[], _T],
    exception_types: Union[Type, Tuple[Type, ...]],
    message: str = "Retrying",
    max_num_attempts: int = 5,
    delay_seconds: float = 1,
) -> _T:
    i = 0
    while True:
        try:
            return function()
        except exception_types:
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(message)
                await asyncio.sleep(delay_seconds)


def _get_pod_for_job(
    core_api: kubernetes.client.CoreV1Api, kubernetes_namespace: str, job_id: str
) -> kubernetes.client.V1Pod:
    """
    When you launch a Kubernetes job, a pod gets created, but in general Kubernetes jobs
    can end up creating many pods (in our case, we always just create one pod), which
    means that the job creation logic doesn't give us the pod namespace.
    """
    # It seems a bit weird that this is the best way to do it:
    # https://github.com/kubernetes/kubernetes/issues/24709

    pods: kubernetes.client.V1PodList = core_api.list_namespaced_pod(
        kubernetes_namespace, label_selector=f"job-name={job_id}"
    )

    if len(pods.items) == 0:
        raise ValueError(
            f"The job {job_id} was created successfully, but cannot find the "
            "corresponding pod"
        )
    elif len(pods.items) > 1:
        # TODO we may need to change this if we add e.g. retries
        raise ValueError(
            f"The job {job_id} has multiple corresponding pods which is unexpected:"
            + ", ".join(p.metadata.name for p in pods.items)
        )
    else:
        return pods.items[0]


def _get_main_container_state(
    container_statuses: Optional[List[kubernetes.client.V1ContainerStatus]],
    job_id: str,
    pod_name: str,
) -> Optional[kubernetes.client.V1ContainerState]:
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


async def _wait_until_pod_is_running(
    core_api: kubernetes.client.CoreV1Api,
    job_id: str,
    pod_name: str,
    kubernetes_namespace: str,
    pod: kubernetes.client.V1Pod,
) -> None:
    """
    The idea here is to wait until a pod is running. For the happy path is that our pod
    is in the "waiting" state because we're either waiting for the image to get pulled
    or we're waiting for nodes to become available to run our job. In that case, we'll
    wait up to 7 minutes.

    The unhappy path is that something has gone wrong which Kubernetes expresses as
    waiting infinitely, rather than a failure. E.g. if our image spec is invalid. In
    that case we'll only wait 15 seconds, as it doesn't make sense to expect that that
    would change.
    """
    i = 0
    wait_until = 15
    max_wait_until = 60 * 7
    main_container_state: kubernetes.client.V1ContainerState = (
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
            print(f"Waiting for container to start running{additional_info}")
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

        pod = core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        main_container_state = _get_main_container_state(
            pod.status.container_statuses, job_id, pod_name
        )


def _get_kubernetes_secret(
    kubernetes_namespace: str, secret_name: str
) -> Dict[str, str]:
    with kubernetes.client.ApiClient() as api_client:
        core_api = kubernetes.client.CoreV1Api(api_client)
        result = core_api.read_namespaced_secret(secret_name, kubernetes_namespace)

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
) -> int:
    """
    Runs the specified job on Kubernetes, waits for it to complete, and returns the exit
    code
    """

    # create the job

    environment = [
        kubernetes.client.V1EnvVar(name=key, value=value)
        for key, value in environment_variables.items()
    ]

    if storage_username_password_secret is not None:
        if MEADOWRUN_STORAGE_USERNAME not in environment_variables:
            environment.append(
                kubernetes.client.V1EnvVar(
                    name=MEADOWRUN_STORAGE_USERNAME,
                    value_from=kubernetes.client.V1EnvVarSource(
                        secret_key_ref=kubernetes.client.V1SecretKeySelector(
                            key="username",
                            name=storage_username_password_secret,
                            optional=False,
                        )
                    ),
                )
            )
        if MEADOWRUN_STORAGE_PASSWORD not in environment_variables:
            environment.append(
                kubernetes.client.V1EnvVar(
                    name=MEADOWRUN_STORAGE_PASSWORD,
                    value_from=kubernetes.client.V1EnvVarSource(
                        secret_key_ref=kubernetes.client.V1SecretKeySelector(
                            key="password",
                            name=storage_username_password_secret,
                            optional=False,
                        )
                    ),
                )
            )

    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    body = kubernetes.client.V1Job(
        metadata=kubernetes.client.V1ObjectMeta(name=job_id),
        spec=kubernetes.client.V1JobSpec(
            backoff_limit=0,
            template=kubernetes.client.V1PodTemplateSpec(
                spec=kubernetes.client.V1PodSpec(
                    containers=[
                        kubernetes.client.V1Container(
                            name="main", image=image, args=args, env=environment
                        )
                    ],
                    restart_policy="Never",
                )
            ),
        ),
    )

    with kubernetes.client.ApiClient() as api_client:
        batch_api = kubernetes.client.BatchV1Api(api_client)
        try:
            # this returns a result, but we don't really need anything from it
            batch_api.create_namespaced_job(kubernetes_namespace, body)

            core_api = kubernetes.client.CoreV1Api(api_client)

            # Once we've created the job, we have to figure out the name of the pod for
            # this job. The pod won't be available immediately, so we need to wait for
            # it

            pod = await _retry(
                lambda: _get_pod_for_job(core_api, kubernetes_namespace, job_id),
                ValueError,
                f"Waiting for Kubernetes to create the pod for job {job_id}",
                15,
            )

            pod_name = pod.metadata.name
            print(f"Created pod {pod_name} for job {job_id}")

            # Next, wait for the pod to be running

            await _wait_until_pod_is_running(
                core_api, job_id, pod_name, kubernetes_namespace, pod
            )

            # Now our pod is running, so we can tail the logs

            # https://github.com/kubernetes-client/python/issues/199
            for line in kubernetes.watch.Watch().stream(
                core_api.read_namespaced_pod_log,
                name=pod_name,
                namespace=kubernetes_namespace,
            ):
                print(line)

            # Once this stream ends, we know the pod is completed, but sometimes it
            # takes some time for Kubernetes to report that the pod has completed. So we
            # poll until the pod is reported completed.

            i = 0
            pod = core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
            main_container_state = _get_main_container_state(
                pod.status.container_statuses, job_id, pod_name
            )
            while (
                main_container_state is None or main_container_state.running is not None
            ):
                print("Waiting for the pod to stop running")
                await asyncio.sleep(1.0)
                i += 1
                if i > 15:
                    raise TimeoutError(
                        f"Unexpected. The job {job_id} has a pod {pod_name}, and there "
                        "are no more logs for this pod, but the pod still seems to be "
                        "running"
                    )
                pod = core_api.read_namespaced_pod_status(
                    pod_name, kubernetes_namespace
                )
                main_container_state = _get_main_container_state(
                    pod.status.container_statuses, job_id, pod_name
                )

            return main_container_state.terminated.exit_code
        finally:
            # TODO we should separately periodically clean up these jobs/pods in case we
            # aren't able to execute this finally block
            try:
                batch_api.delete_namespaced_job(
                    job_id, kubernetes_namespace, propagation_policy="Foreground"
                )
            except kubernetes.client.exceptions.ApiException as e:
                print(f"Warning, error cleaning up job: {e}")
