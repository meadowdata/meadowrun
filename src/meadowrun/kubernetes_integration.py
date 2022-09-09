from __future__ import annotations

import asyncio
import base64
import dataclasses
import math
import os
import pickle
import time
import traceback
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import botocore.exceptions
import cloudpickle
import kubernetes_asyncio.client as kubernetes_client
import kubernetes_asyncio.client.exceptions as kubernetes_client_exceptions
import kubernetes_asyncio.config as kubernetes_config
import kubernetes_asyncio.watch as kubernetes_watch

import meadowrun.func_worker_storage_helper
from meadowrun.config import GPU, LOGICAL_CPU, MEMORY_GB
from meadowrun.credentials import KubernetesSecretRaw
from meadowrun.docker_controller import expand_ports
from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_PASSWORD,
    MEADOWRUN_STORAGE_USERNAME,
    FuncWorkerClientObjectStorage,
    get_storage_client_from_args,
    read_storage_bytes,
    read_storage_pickle,
    write_storage_bytes,
    write_storage_pickle,
)
from meadowrun.meadowrun_pb2 import (
    Job,
    ProcessState,
    PyFunctionJob,
    QualifiedFunctionName,
)
from meadowrun.run_job_core import (
    Host,
    JobCompletion,
    MeadowrunException,
    ObjectStorage,
    WaitOption,
)
from meadowrun.run_job_local import (
    _get_credentials_for_docker,
    _get_credentials_sources,
    _string_pairs_to_dict,
)
from meadowrun.version import __version__

if TYPE_CHECKING:
    from typing_extensions import Literal

    from meadowrun.instance_selection import ResourcesInternal
    from meadowrun.run_job_core import TaskResult


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
    storage_endpoint_url: Optional[str],
    result_highest_pickle_protocol: int,
) -> None:
    """
    This is a worker function to help with running a run_map. This worker assumes that
    JOB_COMPLETION_INDEX is set, which Kubernetes will set for indexed completion jobs.
    This worker assumes task arguments are accessible via
    meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT and will just
    complete all of the tasks where task_index % num_workers == current worker index.
    """

    current_worker_index = int(os.environ["JOB_COMPLETION_INDEX"])
    # if we're the main process launched in the container (via
    # _run_job_helper_custom_container) then FUNC_WORKER_STORAGE_CLIENT will already be
    # set by func_worker_storage. If we're a child process launched by
    # run_job_local_storage_main (via _run_job_helper_generic_container) then we need to
    # use the arguments passed to this function to create the storage client in this
    # function.
    if meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT is None:
        storage_username = os.environ.get(MEADOWRUN_STORAGE_USERNAME, None)
        storage_password = os.environ.get(MEADOWRUN_STORAGE_PASSWORD, None)
        if storage_username is None and storage_password is None:
            raise ValueError("Cannot call _indexed_map_worker without a storage client")

        meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT = (
            get_storage_client_from_args(
                storage_endpoint_url, storage_username, storage_password
            )
        )
        meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET = storage_bucket

    storage_client = meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT

    result_pickle_protocol = min(
        result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
    )

    i = current_worker_index
    while i < total_num_tasks:
        state_filename = f"{file_prefix}.taskstate{i}"
        result_filename = f"{file_prefix}.taskresult{i}"

        arg = read_storage_pickle(
            storage_client, storage_bucket, f"{file_prefix}.taskarg{i}"
        )

        try:
            result = function(arg)
        except Exception as e:
            # first print the exception for the local log file
            traceback.print_exc()

            tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))

            # now send back result
            write_storage_bytes(
                storage_client,
                storage_bucket,
                state_filename,
                "PYTHON_EXCEPTION".encode("utf-8"),
            )
            write_storage_pickle(
                storage_client,
                storage_bucket,
                result_filename,
                (str(type(e)), str(e), tb),
                result_pickle_protocol,
            )
        else:
            # send back results
            write_storage_bytes(
                storage_client,
                storage_bucket,
                state_filename,
                "SUCCEEDED".encode("utf-8"),
            )
            write_storage_pickle(
                storage_client,
                storage_bucket,
                result_filename,
                result,
                result_pickle_protocol,
            )

        i += num_workers


def _get_job_completion_from_state_result(
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

    try:
        state_bytes = read_storage_bytes(
            storage_client, storage_bucket, f"{file_prefix}.state{file_suffix}"
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

    state_string = state_bytes.decode("utf-8")
    if state_string == "SUCCEEDED":
        state = ProcessState.ProcessStateEnum.SUCCEEDED
    elif state_string == "PYTHON_EXCEPTION":
        state = ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
    else:
        raise ValueError(f"Unknown state string: {state_string}")

    # if we got a state string, we should have a result file
    result = read_storage_pickle(
        storage_client, storage_bucket, f"{file_prefix}.result{file_suffix}"
    )

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


def _get_job_completion_from_process_state(
    storage_client: Any,
    storage_bucket: str,
    file_prefix: str,
    file_suffix: str,
    job_spec_type: Literal["py_command", "py_function"],
    return_code: int,
) -> JobCompletion[Any]:
    """
    Creates a JobCompletion object based on the return code, and .process_state file if
    it's available

    file_suffix is used by indexed completion jobs to distinguish between the
    completions of different workers, this should be set to the job completion index.
    """

    # kind of copy/pasted from SshHost.run_job

    # TODO get the name of the pod that ran the job? It won't be super useful
    # because we delete the pod right away
    public_address = "kubernetes"

    if return_code != 0:
        raise MeadowrunException(
            ProcessState(
                state=ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
                return_code=return_code,
            )
        )

    process_state = ProcessState.FromString(
        read_storage_bytes(
            storage_client, storage_bucket, f"{file_prefix}.process_state{file_suffix}"
        )
    )

    if process_state.state == ProcessState.ProcessStateEnum.SUCCEEDED:
        # we must have a result from functions, in other cases we can optionally have a
        # result
        if job_spec_type == "py_function" or process_state.pickled_result:
            result = pickle.loads(process_state.pickled_result)
        else:
            result = None
        return JobCompletion(
            result,
            process_state.state,
            process_state.log_file_name,
            process_state.return_code,
            public_address,
        )
    else:
        raise MeadowrunException(process_state)


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

                write_storage_bytes(
                    storage_client,
                    self.storage_bucket,
                    f"{file_prefix}.function",
                    function.pickled_function,
                )
                command.append("--has-pickled-function")

            # prepare arguments
            if job.py_function.pickled_function_arguments:
                if self.storage_bucket is None:
                    raise ValueError(
                        "Cannot use pickled function arguments without providing a "
                        "storage_bucket. Please either specify the function to run with"
                        " a string or provide a storage_bucket"
                    )
                write_storage_bytes(
                    storage_client,
                    self.storage_bucket,
                    f"{file_prefix}.arguments",
                    function.pickled_function_arguments,
                )
                command.append("--has-pickled-arguments")

            return command
        else:
            raise ValueError(f"Unknown job_spec {job_spec_type}")

    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        # TODO add support for this feature
        if job.sidecar_containers:
            raise NotImplementedError(
                "Sidecar containers are not yet supported for Kubernetes"
            )

        job_completions = await self._run_job_helper(
            await self._get_storage_client(),
            job,
            resources_required,
            None,
            wait_for_result,
        )
        if len(job_completions) != 1:
            raise ValueError(
                "Unexpected, requested a single job but got back "
                f"{len(job_completions)} job_completions"
            )
        return job_completions[0]

    async def _run_job_helper_custom_container(
        self,
        storage_client: Any,
        job: Job,
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        interpreter_deployment_type: str,
        file_prefix: str,
        wait_for_result: WaitOption,
    ) -> List[JobCompletion[Any]]:
        # This function is for jobs where job.interpreter_deployment is a user-specified
        # container (i.e. not something we need to build ourselves). For these kinds of
        # jobs, this function is effectively functioning as "the agent", i.e.
        # run_job_local_main. The container we launch will only have a single process.
        # That means we send (via S3-compatible object store) e.g. the pickled function,
        # or we might not need to send anything at all if we're just running a command
        # or a function based on the name of the function. The result comes back as a
        # pair of .state and .result files (the same way run_job_local_main.py interacts
        # with its child process).
        #
        # Right now we only use this approach for when we have a user-specified
        # container. We could theoretically use this approach with environment specs by
        # building the container locally or with various techniques for building
        # containers from inside of a container.
        try:
            if interpreter_deployment_type == "container_at_digest":
                image_repository_name = job.container_at_digest.repository
                image_name = (
                    f"{job.container_at_digest.repository}@"
                    f"{job.container_at_digest.digest}"
                )
            elif interpreter_deployment_type == "container_at_tag":
                image_repository_name = job.container_at_tag.repository
                image_name = (
                    f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
                )
            elif interpreter_deployment_type == "server_available_container":
                image_repository_name = None
                image_name = f"{job.server_available_container.image_name}"
            else:
                raise Exception(
                    "Programming error in caller. Called "
                    "_run_job_helper_prebuilt_container with "
                    f"interpreter_deployment_type {interpreter_deployment_type}"
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
                            "Using anything other than KubernetesSecret with a "
                            "Kubernetes host has not been implemented, "
                            f"{type(image_pull_secret)} was provided"
                        )
                    image_pull_secret_name = image_pull_secret.secret_name

            # code deployment
            command_suffixes = []
            code_deployment_type = job.WhichOneof("code_deployment")
            if code_deployment_type == "code_zip_file":
                if self.storage_bucket is None:
                    raise ValueError(
                        "Cannot use mirror_local without providing a storage_bucket. "
                        "Please either use a different Deployment or provide a "
                        "storage_bucket"
                    )
                write_storage_bytes(
                    storage_client,
                    self.storage_bucket,
                    f"{file_prefix}.codezipfile",
                    job.code_zip_file.SerializeToString(),
                )

                command_suffixes.append("--has-code-zip-file")
            elif (
                code_deployment_type != "server_available_folder"
                or len(job.server_available_folder.code_paths) > 0
            ):
                raise NotImplementedError(
                    f"code_deployment_type {code_deployment_type} is not supported for"
                    " prebuilt containers on Kubernetes"
                )

            # run the job

            job_spec_type = job.WhichOneof("job_spec")
            if job_spec_type is None:
                raise ValueError("Unexpected: job_spec is None")

            command = self._prepare_command(
                job, job_spec_type, storage_client, file_prefix
            )
            command.extend(command_suffixes)

            environment_variables = {"PYTHONUNBUFFERED": "1"}
            environment_variables.update(
                **_string_pairs_to_dict(job.environment_variables)
            )

            try:
                return_codes = await _run_kubernetes_job(
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    environment_variables,
                    self.storage_username_password_secret,
                    image_pull_secret_name,
                    indexed_completions,
                    [int(p) for p in expand_ports(job.ports)],
                    resources_required,
                    wait_for_result,
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
                _get_job_completion_from_state_result(
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
                for suffix in [
                    "state",
                    "result",
                    "function",
                    "arguments",
                    "codezipfile",
                ]:
                    try:
                        storage_client.delete_object(
                            Bucket=self.storage_bucket, Key=f"{file_prefix}.{suffix}"
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass

    async def _run_job_helper_generic_container(
        self,
        storage_client: Any,
        job: Job,
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        interpreter_deployment_type: str,
        file_prefix: str,
        wait_for_result: WaitOption,
    ) -> List[JobCompletion[Any]]:
        # This function is for jobs where job.interpreter_deployment is an "environment
        # spec" of some sort that requires us to build an environment. For these kinds
        # of jobs, this function will send over a Job object (via S3-compatible object
        # store) and run_job_local_storage_main.py will read the Job object, create the
        # environment in the container, and then run the specified job. The result comes
        # back as a .process_state file.
        #
        # This will always run using a "generic container image" (i.e. the prebuilt
        # Meadowrun image) because the user did not specify a custom container.
        #
        # This approach can only be used with an "environment spec", it cannot be used
        # with a user-defined container as containers in Kubernetes can't launch other
        # containers.
        try:
            if interpreter_deployment_type == "environment_spec_in_code":
                python_version = job.environment_spec_in_code.python_version
            elif interpreter_deployment_type == "environment_spec":
                python_version = job.environment_spec.python_version
            elif interpreter_deployment_type == "server_available_interpreter":
                python_version = None
            else:
                raise Exception(
                    "Programming error in caller. Called "
                    "_run_job_helper_generic_container with "
                    f"interpreter_deployment_type {interpreter_deployment_type}"
                )
            if not python_version:
                # conda environments and raw server_available_interpreter won't have a
                # python version
                python_version = "3.10"

            if self.storage_bucket is None or self.storage_endpoint_url is None:
                raise ValueError(
                    "Cannot use an environment_spec without providing a storage_bucket."
                    " Please either provide a pre-built container image for the "
                    "interpreter or provide a storage_bucket"
                )

            write_storage_bytes(
                storage_client,
                self.storage_bucket,
                f"{file_prefix}.job_to_run",
                job.SerializeToString(),
            )

            command = [
                "python",
                "-m",
                "meadowrun.run_job_local_storage_main",
                "--storage-bucket",
                self.storage_bucket,
                "--storage-file-prefix",
                file_prefix,
            ]
            if self.storage_endpoint_url_in_cluster:
                command.extend(
                    ["--storage-endpoint-url", self.storage_endpoint_url_in_cluster]
                )
            else:
                command.extend(["--storage-endpoint-url", self.storage_endpoint_url])

            # TODO use meadowrun-cuda if we need cuda. Also choose based on python
            # version in environment_spec
            image_name = f"meadowrun/meadowrun:{__version__}-py{python_version}"
            # uncomment this for development
            image_name = f"meadowrun/meadowrun-dev:py{python_version}"

            try:
                return_codes = await _run_kubernetes_job(
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    {"PYTHONUNBUFFERED": "1"},
                    self.storage_username_password_secret,
                    None,
                    indexed_completions,
                    [int(p) for p in expand_ports(job.ports)],
                    resources_required,
                    wait_for_result,
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

            job_spec_type = job.WhichOneof("job_spec")
            if job_spec_type is None:
                raise ValueError("Unexpected, job.job_spec is None")
            return [
                _get_job_completion_from_process_state(
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
            for suffix in ["job_to_run", "process_state"]:
                try:
                    storage_client.delete_object(
                        Bucket=self.storage_bucket, Key=f"{file_prefix}.{suffix}"
                    )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    pass

    async def _run_job_helper(
        self,
        storage_client: Any,
        job: Job,
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        wait_for_result: WaitOption,
    ) -> List[JobCompletion[Any]]:
        file_prefix = f"{self.storage_file_prefix}{job.job_id}"

        # interpreter deployment
        interpreter_deployment_type = job.WhichOneof("interpreter_deployment")
        if interpreter_deployment_type in (
            "container_at_digest",
            "container_at_tag",
            "server_available_container",
        ):
            return await self._run_job_helper_custom_container(
                storage_client,
                job,
                resources_required,
                indexed_completions,
                interpreter_deployment_type,
                file_prefix,
                wait_for_result,
            )
        elif interpreter_deployment_type in (
            "environment_spec_in_code",
            "environment_spec",
            "server_available_interpreter",
        ):
            return await self._run_job_helper_generic_container(
                storage_client,
                job,
                resources_required,
                indexed_completions,
                interpreter_deployment_type,
                file_prefix,
                wait_for_result,
            )
        else:
            raise NotImplementedError(
                f"interpreter_deployment_type {interpreter_deployment_type} is not "
                "implemented yet on Kubernetes"
            )

    async def run_map(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
    ) -> Optional[Sequence[_U]]:
        # TODO add support for this feature
        if job_fields["sidecar_containers"]:
            raise NotImplementedError(
                "Sidecar containers are not yet supported for Kubernetes"
            )
        if max_num_task_attempts != 1:
            raise NotImplementedError("max_num_task_attempts must be 1 on Kubernetes")

        storage_client = await self._get_storage_client()
        # extra storage_bucket check is for mypy
        if storage_client is None or self.storage_bucket is None:
            raise ValueError(
                "storage_bucket and other storage_* parameters must be specified to use"
                " Kubernetes with run_map"
            )

        job_id = str(uuid.uuid4())

        file_prefix = f"{self.storage_file_prefix}{job_id}"

        for i, arg in enumerate(args):
            write_storage_pickle(
                storage_client,
                self.storage_bucket,
                f"{file_prefix}.taskarg{i}",
                arg,
                pickle_protocol,
            )

        indexed_map_worker_args = (
            len(args),
            num_concurrent_tasks,
            function,
            self.storage_bucket,
            file_prefix,
            self.storage_endpoint_url_in_cluster,
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
            resources_required_per_task,
            num_concurrent_tasks,
            wait_for_result,
        )

        results = []
        for i in range(len(args)):
            state_string = read_storage_bytes(
                storage_client, self.storage_bucket, f"{file_prefix}.taskstate{i}"
            ).decode("utf-8")
            if state_string == "SUCCEEDED":
                state = ProcessState.ProcessStateEnum.SUCCEEDED
            elif state_string == "PYTHON_EXCEPTION":
                state = ProcessState.ProcessStateEnum.PYTHON_EXCEPTION
            else:
                raise ValueError(f"Unknown state string: {state_string}")

            result = read_storage_pickle(
                storage_client, self.storage_bucket, f"{file_prefix}.taskresult{i}"
            )

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

    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
    ) -> AsyncIterable[TaskResult[_U]]:
        raise NotImplementedError(
            "run_map_as_completed is not implemented for Kubernetes"
        )

    async def get_object_storage(self) -> ObjectStorage:
        storage_client = await self._get_storage_client()

        if storage_client is None or self.storage_bucket is None:
            raise ValueError(
                "Cannot use mirror_local without providing a storage_bucket. Please "
                "either use a different Deployment method or provide a storage_bucket"
            )
        return FuncWorkerClientObjectStorage(storage_client, self.storage_bucket)


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
    pod: kubernetes_client.V1Pod, job_id: str, pod_name: str
) -> Tuple[Optional[kubernetes_client.V1ContainerState], Optional[str]]:
    # first get main container state
    container_statuses = pod.status.container_statuses

    if container_statuses is None or len(container_statuses) == 0:
        main_container_state = None
    else:
        main_container_statuses = [s for s in container_statuses if s.name == "main"]
        if len(main_container_statuses) == 0:
            raise ValueError(
                f"The job {job_id} has a pod {pod_name} but there is no `main` "
                "container"
            )
        if len(main_container_statuses) > 1:
            raise ValueError(
                f"The job {job_id} has a pod {pod_name} but there is more than one "
                "`main` container"
            )
        main_container_state = main_container_statuses[0].state

    # then get the latest condition's reason, this is where Kubernetes will tell us that
    # e.g. the pod is unschedulable
    if pod.status.conditions:
        latest_condition = pod.status.conditions[-1]
        result_builder = []
        if latest_condition.reason:
            result_builder.append(latest_condition.reason)
        if latest_condition.message:
            result_builder.append(latest_condition.message)
        latest_condition_reason = ", ".join(result_builder)
    else:
        latest_condition_reason = None

    return main_container_state, latest_condition_reason


async def _wait_for_pod_running(
    core_api: kubernetes_client.CoreV1Api,
    job_id: str,
    kubernetes_namespace: str,
    pod: kubernetes_client.V1Pod,
) -> None:
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
    main_container_state, latest_condition_reason = _get_main_container_state(
        pod, job_id, pod_name
    )
    prev_additional_info = None
    while main_container_state is None or (
        main_container_state.running is None and main_container_state.terminated is None
    ):
        is_happy_path = False
        additional_info_builder = [":"]
        if (
            main_container_state is not None
            and main_container_state.waiting is not None
        ):
            additional_info_builder.append(str(main_container_state.waiting.reason))
            if main_container_state.waiting.message is not None:
                additional_info_builder.append(
                    str(main_container_state.waiting.message)
                )
            elif main_container_state.waiting.reason == "ContainerCreating":
                # TODO Kubernetes unfortunately doesn't distinguish between waiting for
                # an image to get pulled vs waiting for a free node in this field, we
                # need to use the Events API to get that information. At some point it
                # may come through in this waiting.reason field, though:
                # https://github.com/kubernetes/kubernetes/issues/19077
                additional_info_builder.append(
                    "(pulling image or waiting for available nodes)"
                )
                is_happy_path = True
        if latest_condition_reason:
            additional_info_builder.append(latest_condition_reason)

        if len(additional_info_builder) == 1:
            additional_info = ""
        else:
            additional_info = " ".join(additional_info_builder)
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
        main_container_state, latest_condition_reason = _get_main_container_state(
            pod, job_id, pod_name
        )


async def _stream_pod_logs(
    core_api: kubernetes_client.CoreV1Api, kubernetes_namespace: str, pod_name: str
) -> None:
    # Now our pod is running, so we can stream the logs

    async with kubernetes_watch.Watch() as w:
        async for line in w.stream(
            core_api.read_namespaced_pod_log,
            name=pod_name,
            namespace=kubernetes_namespace,
        ):
            print(line, end="")


async def _wait_for_pod_exit(
    core_api: kubernetes_client.CoreV1Api,
    job_id: str,
    kubernetes_namespace: str,
    pod_name: str,
    timeout_seconds: int,
    streamed_logs: bool,
) -> int:
    # Once this stream ends, we know the pod is completed, but sometimes it takes some
    # time for Kubernetes to report that the pod has completed. So we poll until the pod
    # is reported as terminated.

    pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
    main_container_state, _ = _get_main_container_state(pod, job_id, pod_name)
    t0 = time.time()
    while main_container_state is None or main_container_state.running is not None:
        await asyncio.sleep(1.0)
        if time.time() > t0 + timeout_seconds:
            if streamed_logs:
                raise TimeoutError(
                    f"Unexpected. The job {job_id} has a pod {pod_name}, and the pod "
                    f"still seems to be running {timeout_seconds} seconds after the log"
                    " stream ended"
                )
            else:
                raise TimeoutError(
                    f"The job {job_id} timed out, the pod {pod_name} as been running "
                    f"for {timeout_seconds} seconds"
                )
        pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        main_container_state, _ = _get_main_container_state(pod, job_id, pod_name)

    return main_container_state.terminated.exit_code


async def _wait_for_pod(
    core_api: kubernetes_client.CoreV1Api,
    job_id: str,
    kubernetes_namespace: str,
    pod: kubernetes_client.V1Pod,
    wait_for_result: WaitOption,
) -> int:
    """
    This function waits for the specified pod to start running, streams the logs from
    that pod into our local stdout, and then waits for the pod to terminate. Then we
    return the exit code of the pod.
    """
    await _wait_for_pod_running(core_api, job_id, kubernetes_namespace, pod)
    if wait_for_result == WaitOption.DO_NOT_WAIT:
        # TODO maybe return None instead? Currently this code path is not used, requires
        # support in the caller
        return 0

    if wait_for_result == WaitOption.WAIT_AND_TAIL_STDOUT:
        await _stream_pod_logs(core_api, kubernetes_namespace, pod.metadata.name)

        return await _wait_for_pod_exit(
            core_api, job_id, kubernetes_namespace, pod.metadata.name, 15, True
        )
    else:
        # TODO this timeout should be configurable and the default should be smaller
        # than 2 days
        wait_for_pod_exit_timeout_seconds = 60 * 60 * 24 * 2
        return await _wait_for_pod_exit(
            core_api,
            job_id,
            kubernetes_namespace,
            pod.metadata.name,
            wait_for_pod_exit_timeout_seconds,
            False,
        )


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


def _resources_to_kubernetes(resources: ResourcesInternal) -> Dict[str, int]:
    result = {}

    if LOGICAL_CPU in resources.consumable:
        result["cpu"] = math.ceil(resources.consumable[LOGICAL_CPU])
    if MEMORY_GB in resources.consumable:
        result["memory"] = math.ceil(resources.consumable[MEMORY_GB] * (1024**3))
    if GPU in resources.consumable:
        num_gpus = math.ceil(resources.consumable[GPU])
        if "nvidia" in resources.non_consumable:
            result["nvidia.com/gpu"] = num_gpus
        else:
            raise ValueError(
                "Must specify a type of GPU (e.g. nvidia) if a GPU resource is "
                "requested"
            )

    # TODO maybe warn if people are trying to use resources that we don't know how to
    # interpret
    # TODO maybe turn max_eviction_rate into a pod disruption budget?

    return result


async def _run_kubernetes_job(
    job_id: str,
    kubernetes_namespace: str,
    image: str,
    args: List[str],
    environment_variables: Dict[str, str],
    storage_username_password_secret: Optional[str],
    image_pull_secret_name: Optional[str],
    indexed_completions: Optional[int],
    ports: List[int],
    resources: Optional[ResourcesInternal],
    wait_for_result: WaitOption,
) -> Sequence[int]:
    """
    Runs the specified job on Kubernetes, waits for it to complete, and returns the exit
    code
    """

    # TODO add support for DO_NOT_WAIT
    if wait_for_result == WaitOption.DO_NOT_WAIT:
        raise NotImplementedError(
            f"{wait_for_result} is not supported for Kubernetes yet"
        )

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

    additional_container_parameters = {}

    if ports:
        additional_container_parameters["ports"] = [
            kubernetes_client.V1ContainerPort(container_port=port) for port in ports
        ]
        service = kubernetes_client.V1Service(
            metadata=kubernetes_client.V1ObjectMeta(name=f"svc-{job_id}"),
            spec=kubernetes_client.V1ServiceSpec(
                # The job-name label is automatically set by Kubernetes for pods created
                # by jobs
                selector={"job-name": job_id},
                ports=[
                    kubernetes_client.V1ServicePort(port=port, name=f"port{port}")
                    for port in ports
                ],
            ),
        )
    else:
        service = None

    if resources is not None:
        additional_container_parameters[
            "resources"
        ] = kubernetes_client.V1ResourceRequirements(
            requests=_resources_to_kubernetes(resources)
        )

    # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
    body = kubernetes_client.V1Job(
        metadata=kubernetes_client.V1ObjectMeta(name=job_id),
        spec=kubernetes_client.V1JobSpec(
            backoff_limit=0,
            template=kubernetes_client.V1PodTemplateSpec(
                spec=kubernetes_client.V1PodSpec(
                    containers=[
                        kubernetes_client.V1Container(
                            name="main",
                            image=image,
                            args=args,
                            env=environment,
                            **additional_container_parameters,
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

            if service:
                await core_api.create_namespaced_service(kubernetes_namespace, service)
                print(f"Created service {service.metadata.name}")

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

            # Now that the pods have been created, stream their logs and get their exit
            # codes

            return await asyncio.gather(
                *[
                    asyncio.create_task(
                        _wait_for_pod(
                            core_api, job_id, kubernetes_namespace, pod, wait_for_result
                        )
                    )
                    for pod in pods
                ]
            )
        finally:
            # TODO we should separately periodically clean up these jobs/pods services
            # in case we aren't able to execute this finally block
            if service is not None:
                try:
                    await core_api.delete_namespaced_service(
                        service.metadata.name,
                        kubernetes_namespace,
                        propagation_policy="Foreground",
                    )
                except kubernetes_client_exceptions.ApiException as e:
                    print(f"Warning, error cleaning up service: {e}")
            try:
                await batch_api.delete_namespaced_job(
                    job_id, kubernetes_namespace, propagation_policy="Foreground"
                )
            except kubernetes_client_exceptions.ApiException as e:
                print(f"Warning, error cleaning up job: {e}")
