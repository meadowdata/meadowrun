from __future__ import annotations

import asyncio
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
    AsyncContextManager,
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

import cloudpickle
import kubernetes_asyncio.client as kubernetes_client
import kubernetes_asyncio.client.exceptions as kubernetes_client_exceptions
import kubernetes_asyncio.config as kubernetes_config
import kubernetes_asyncio.stream as kubernetes_stream

import meadowrun.func_worker_storage_helper
from meadowrun.config import GPU, LOGICAL_CPU, MEMORY_GB, MEADOWRUN_INTERPRETER
from meadowrun.credentials import KubernetesSecretRaw
from meadowrun.docker_controller import expand_ports
from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_PASSWORD,
    MEADOWRUN_STORAGE_USERNAME,
    FuncWorkerClientObjectStorage,
)
from meadowrun.k8s_integration.k8s_core import (
    get_kubernetes_secret,
    get_main_container_is_ready,
    get_pods_for_job,
    run_command_on_pod,
    set_main_container_ready,
    wait_for_pod,
    wait_for_pod_running,
)
from meadowrun.s3_grid_job import (
    complete_task,
    download_task_arg,
    get_storage_client_from_args,
    read_storage,
    receive_results,
    upload_task_args,
)
from meadowrun.meadowrun_pb2 import (
    Job,
    ProcessState,
    PyFunctionJob,
    QualifiedFunctionName,
    ServerAvailableInterpreter,
)
from meadowrun.run_job_core import (
    Host,
    JobCompletion,
    MeadowrunException,
    TaskResult,
    WaitOption,
    _PRINT_RECEIVED_TASKS_SECONDS,
)
from meadowrun.run_job_local import (
    _get_credentials_for_docker,
    _get_credentials_sources,
    _string_pairs_to_dict,
)
from meadowrun.shared import none_async_context
from meadowrun.storage_keys import (
    storage_key_job_to_run,
    storage_key_process_state,
    storage_key_ranges,
    storage_key_task_args,
)
from meadowrun.version import __version__

if TYPE_CHECKING:
    from typing_extensions import Literal
    import types_aiobotocore_s3

    from meadowrun.instance_selection import ResourcesInternal
    from meadowrun.object_storage import ObjectStorage
    from meadowrun.run_job_core import WorkerProcessState, TaskProcessState


_T = TypeVar("_T")
_U = TypeVar("_U")


def _indexed_map_worker(
    total_num_tasks: int,
    num_workers: int,
    function: Callable[[_T], _U],
    storage_bucket: str,
    job_id: str,
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

    # TODO add additional error handling a la asynico.run?
    worker_event_loop = asyncio.new_event_loop()

    # WORKER_INDEX will be available in reusable pods. In non-reusable pods we have to
    # use JOB_COMPLETION_INDEX
    current_worker_index = int(
        os.environ.get("MEADOWRUN_WORKER_INDEX", os.environ["JOB_COMPLETION_INDEX"])
    )
    # if we're the main process launched in the container (via
    # _run_direct_command) then FUNC_WORKER_STORAGE_CLIENT will already be
    # set by func_worker_storage. If we're a child process launched by
    # run_job_local_storage_main (via _run_job_per_pod) then we need to
    # use the arguments passed to this function to create the storage client in this
    # function.
    if meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT is None:
        storage_username = os.environ.get(MEADOWRUN_STORAGE_USERNAME, None)
        storage_password = os.environ.get(MEADOWRUN_STORAGE_PASSWORD, None)
        if storage_username is None and storage_password is None:
            raise ValueError("Cannot call _indexed_map_worker without a storage client")

        storage_client_needs_exit = True
        storage_client = worker_event_loop.run_until_complete(
            get_storage_client_from_args(
                storage_endpoint_url, storage_username, storage_password
            ).__aenter__()
        )
        meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT = storage_client
        meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET = storage_bucket
    else:
        storage_client_needs_exit = False
        storage_client = meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT

    try:
        result_pickle_protocol = min(
            result_highest_pickle_protocol, pickle.HIGHEST_PROTOCOL
        )

        byte_ranges = pickle.loads(
            worker_event_loop.run_until_complete(
                read_storage(storage_client, storage_bucket, storage_key_ranges(job_id))
            )
        )

        i = current_worker_index
        while i < total_num_tasks:
            arg = worker_event_loop.run_until_complete(
                download_task_arg(
                    storage_client, storage_bucket, job_id, byte_ranges[i]
                )
            )

            try:
                arg = pickle.loads(arg)
                result = function(*arg[0], **arg[1])
            except Exception as e:
                # first print the exception for the local log file
                traceback.print_exc()

                tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))

                process_state = ProcessState(
                    state=ProcessState.PYTHON_EXCEPTION,
                    pickled_result=pickle.dumps(
                        (str(type(e)), str(e), tb), protocol=result_pickle_protocol
                    ),
                )
            else:
                process_state = ProcessState(
                    state=ProcessState.SUCCEEDED,
                    pickled_result=pickle.dumps(
                        result, protocol=result_pickle_protocol
                    ),
                )

            # we don't support retries yet so we're always on attempt 1
            worker_event_loop.run_until_complete(
                complete_task(
                    storage_client, storage_bucket, job_id, i, 1, process_state
                )
            )

            i += num_workers
    finally:
        if storage_client_needs_exit:
            # TODO should pass in the exception if we have one
            worker_event_loop.run_until_complete(
                storage_client.__aexit__(None, None, None)
            )
        worker_event_loop.close()


async def _get_job_completion_from_process_state(
    storage_client: Any,
    storage_bucket: str,
    job_id: str,
    worker_index: str,
    job_spec_type: Literal["py_command", "py_function", "py_agent"],
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
        await read_storage(
            storage_client,
            storage_bucket,
            storage_key_process_state(job_id, worker_index),
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


def _python_version_from_environment_spec(job: Job) -> str:
    interpreter_deployment_type = job.WhichOneof("interpreter_deployment")

    if interpreter_deployment_type == "environment_spec_in_code":
        python_version = job.environment_spec_in_code.python_version
    elif interpreter_deployment_type == "environment_spec":
        python_version = job.environment_spec.python_version
    elif interpreter_deployment_type == "server_available_interpreter":
        python_version = None
    else:
        raise Exception(
            "Programming error in caller. Called "
            "_run_job_per_pod with "
            f"interpreter_deployment_type {interpreter_deployment_type}"
        )

    if not python_version:
        # conda environments and raw server_available_interpreter won't have a
        # python version
        return "3.10"
    else:
        return python_version


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
        resuable_pods: Experimental feature: rather than always starting a new pod for
            each job, starts generic long-lived pods that can be reused for multiple
            jobs
    """

    storage_bucket: Optional[str] = None
    storage_endpoint_url: Optional[str] = None
    storage_endpoint_url_in_cluster: Optional[str] = None
    storage_username_password_secret: Optional[str] = None
    kube_config_context: Optional[str] = None
    kubernetes_namespace: str = "default"
    reusable_pods: bool = False

    def get_storage_endpoint_url_in_cluster(self) -> Optional[str]:
        if self.storage_endpoint_url_in_cluster is not None:
            return self.storage_endpoint_url_in_cluster
        return self.storage_endpoint_url

    async def _get_storage_client(
        self,
    ) -> AsyncContextManager[Optional[types_aiobotocore_s3.S3Client]]:
        # get the storage client

        # this is kind of weird, this should be called before any Kubernetes function,
        # but for now, _get_storage_client is always the first thing that's called
        await kubernetes_config.load_kube_config(context=self.kube_config_context)

        if self.storage_bucket is not None:
            if self.storage_username_password_secret is not None:
                secret_data = await get_kubernetes_secret(
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

        return none_async_context()

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

        async with await self._get_storage_client() as storage_client:
            job_completions = await self._run_job_helper(
                storage_client,
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

    async def _run_direct_command_if_possible(
        self,
        job: Job,
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        image_name: str,
        image_pull_secret_name: Optional[str],
        wait_for_result: WaitOption,
    ) -> Optional[JobCompletion[None]]:
        # The normal way to run something via Meadowrun on Kubernetes is to call
        # run_local_storage_main in a pod, which will then start another process. For
        # very simple command jobs, we take a simpler approach of running a command
        # directly in a container rather than using run_local_storage_main. The main
        # advantage of this approach is that it doesn't require that Meadowrun is
        # installed in the container.

        if job.WhichOneof("job_spec") != "py_command":
            return None  # this obviously only works with commands

        if job.py_command.pickled_context_variables:
            return None  # pickled_context_variables requires Meadowrun is installed

        if indexed_completions is not None and indexed_completions != 1:
            # we should never get a py_command with indexed_completions, but just in
            # case
            return None

        if (
            job.WhichOneof("code_deployment") != "server_available_folder"
            or len(job.server_available_folder.code_paths) > 0
        ):
            # We don't support running a direct command with any sort of code
            # deployment. We could support a little more by adding
            # server_available_folder.code_paths to PATH and PYTHONPATH, but it's not
            # completely trivial to do that without interfering with whatever
            # environment variables have been set up in the container
            return None

        # run the job
        if not job.py_command.command_line:
            raise ValueError("command_line must have at least one string")
        command = list(job.py_command.command_line)

        environment_variables = {"PYTHONUNBUFFERED": "1"}
        environment_variables.update(**_string_pairs_to_dict(job.environment_variables))

        try:
            return_codes = await _run_kubernetes_job(
                job.job_id,
                self.kubernetes_namespace,
                image_name,
                command,
                environment_variables,
                self.storage_username_password_secret,
                image_pull_secret_name,
                1,
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

        if len(return_codes) != 1:
            raise ValueError(
                "Programming error: requested 1 job but got back "
                f"{len(return_codes)}"
            )

        if return_codes[0] != 0:
            raise MeadowrunException(
                ProcessState(
                    state=ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE,
                    return_code=return_codes[0],
                )
            )

        # TODO get the name of the pod that ran the job? It won't be super useful
        # because we delete the pod right away
        return JobCompletion(
            None,
            ProcessState.ProcessStateEnum.SUCCEEDED,
            "",
            0,
            "kubernetes",
        )

    async def _run_job_per_pod(
        self,
        storage_client: Any,
        job: Job,
        image_name: str,
        image_pull_secret_name: Optional[str],
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        wait_for_result: WaitOption,
    ) -> List[JobCompletion[Any]]:
        # This is the "normal" way to run jobs on Kubernetes using a pod-per-job model.
        # We assume that the image specified by image_name has Meadowrun set up in it.
        # We use the storage_client to send a Job object via S3-compatible object
        # storage and run run_job_local_storage_main which will actually run our job.
        # The results will also come back via the object storage.

        # keeps track of most of what we write to object storage so we can clean it up
        # when we're done
        storage_keys_used = []

        try:
            storage_endpoint_url = self.get_storage_endpoint_url_in_cluster()
            if self.storage_bucket is None or storage_endpoint_url is None:
                raise ValueError(
                    "Cannot use an environment_spec without providing a storage_bucket."
                    " Please either provide a pre-built container image for the "
                    "interpreter or provide a storage_bucket"
                )

            job_to_run_key = storage_key_job_to_run(job.job_id)
            storage_keys_used.append(job_to_run_key)
            await storage_client.put_object(
                Bucket=self.storage_bucket,
                Key=job_to_run_key,
                Body=job.SerializeToString(),
            )

            command = [
                "python",
                "-m",
                "meadowrun.run_job_local_storage_main",
                "--storage-bucket",
                self.storage_bucket,
                "--job-id",
                job.job_id,
                "--storage-endpoint-url",
                storage_endpoint_url,
            ]

            if indexed_completions:
                worker_indexes: Iterable[str] = (
                    str(i) for i in range(indexed_completions)
                )
            else:
                worker_indexes = [""]
            for worker_index in worker_indexes:
                storage_keys_used.append(
                    storage_key_process_state(job.job_id, worker_index)
                )

            try:
                return_codes = await _run_kubernetes_job(
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    {"PYTHONUNBUFFERED": "1"},
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

            job_spec_type = job.WhichOneof("job_spec")
            if job_spec_type is None:
                raise ValueError("Unexpected, job.job_spec is None")
            return [
                await _get_job_completion_from_process_state(
                    storage_client,
                    self.storage_bucket,
                    job.job_id,
                    worker_index,
                    job_spec_type,
                    return_code,
                )
                for worker_index, return_code in zip(worker_indexes, return_codes)
            ]
        finally:
            # TODO we should separately periodically clean up these files in case we
            # aren't able to execute this finally block
            if storage_client is not None:
                await asyncio.gather(
                    *(
                        storage_client.delete_object(
                            Bucket=self.storage_bucket, Key=storage_key
                        )
                        for storage_key in storage_keys_used
                    ),
                    return_exceptions=True,
                )

    async def _run_job_helper(
        self,
        storage_client: Any,
        job: Job,
        resources_required: Optional[ResourcesInternal],
        indexed_completions: Optional[int],
        wait_for_result: WaitOption,
    ) -> List[JobCompletion[Any]]:
        # First, some configuration logic that's shared between
        # _run_direct_command_if_possible and _run_job_per_pod

        # get the image_repository_name and image_name
        interpreter_deployment_type = job.WhichOneof("interpreter_deployment")

        if interpreter_deployment_type == "container_at_digest":
            custom_container_image = True
            image_repository_name = job.container_at_digest.repository
            image_name = (
                f"{job.container_at_digest.repository}@"
                f"{job.container_at_digest.digest}"
            )
        elif interpreter_deployment_type == "container_at_tag":
            custom_container_image = True
            image_repository_name = job.container_at_tag.repository
            image_name = f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
        elif interpreter_deployment_type == "server_available_container":
            custom_container_image = True
            image_repository_name = None
            image_name = f"{job.server_available_container.image_name}"
        else:
            custom_container_image = False
            image_repository_name = None
            # This else block implies that the user specified an environment spec rather
            # than a container. Our approach is to default to a "generic container
            # image" and build the environment inside of the container. We could imagine
            # other approaches like building the container locally and uploading it to
            # Kubernetes.

            python_version = _python_version_from_environment_spec(job)
            # TODO use meadowrun-cuda if we need cuda
            image_name = f"meadowrun/meadowrun:{__version__}-py{python_version}"

        # next, get an image pull secrets if it's been provided
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

        # now, if we have a custom container, try to run the job as a direct command. If
        # we don't have a custom container we "can't" run a direct command. We
        # theoretically could with a generic container if we asked for a preinstalled
        # interpreter, but there doesn't seem to be any real point in doing that.
        if custom_container_image:
            result = await self._run_direct_command_if_possible(
                job,
                resources_required,
                indexed_completions,
                image_name,
                image_pull_secret_name,
                wait_for_result,
            )
            if result is not None:
                return [result]

            # if we have a custom container and we're going to run a job on it via
            # run_job_local_storage_main, we need to tell run_job_local_storage_main to
            # just use the container's interpreter (rather than trying to go fetch
            # another container)

            # it's a little paranoid to make a copy here, this could probably be
            # optimized if we need to for performance
            modified_job = Job()
            modified_job.CopyFrom(job)
            # MEADOWRUN_INTERPRETER in this case will refer to whatever interpreter is
            # running Meadowrun which will be whatever interpreter is on the path in the
            # custom container
            modified_job.server_available_interpreter.CopyFrom(
                ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER)
            )
        else:
            modified_job = job

        # if _run_direct_command_if_possible returns None, our job can't be run as a
        # direct command, so we need to run via run_job_local_storage_main.py
        return await self._run_job_per_pod(
            storage_client,
            modified_job,
            image_name,
            image_pull_secret_name,
            resources_required,
            indexed_completions,
            wait_for_result,
        )

    async def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult[_U]]:
        # TODO add support for this feature
        if job_fields["sidecar_containers"]:
            raise NotImplementedError(
                "Sidecar containers are not yet supported for Kubernetes"
            )
        if max_num_task_attempts != 1:
            raise NotImplementedError("max_num_task_attempts must be 1 on Kubernetes")

        async with await self._get_storage_client() as storage_client:
            # extra storage_bucket check is for mypy
            if storage_client is None or self.storage_bucket is None:
                raise ValueError(
                    "storage_bucket and other storage_* parameters must be specified to"
                    " use Kubernetes with run_map"
                )

            # pretty much copied from AllocVM.run_map_as_completed

            driver = KubernetesGridJobDriver(self, num_concurrent_tasks, storage_client)

            # this should be in get_results, but with indexed workers we need to make
            # sure the tasks are uploaded before we can start workers
            await driver._add_tasks(args)

            try:
                if not self.reusable_pods:
                    run_worker_functions = driver.run_worker_functions
                else:
                    run_worker_functions = driver.run_worker_functions_reusable_pods
                run_worker_loops = asyncio.create_task(
                    run_worker_functions(
                        function,
                        len(args),
                        resources_required_per_task,
                        job_fields,
                        pickle_protocol,
                        wait_for_result,
                    )
                )

                num_tasks_done = 0
                async for result in driver.get_results(args):
                    yield result
                    num_tasks_done += 1

                await run_worker_loops

            finally:
                keys_to_delete = [
                    storage_key_task_args(driver._job_id),
                    storage_key_ranges(driver._job_id),
                ]
                await asyncio.gather(
                    *(
                        storage_client.delete_object(
                            Bucket=self.storage_bucket, Key=key
                        )
                        for key in keys_to_delete
                    ),
                    return_exceptions=True,
                )

        # this is for extra safety--the only case where we don't get all of our results
        # back should be if run_worker_loops throws an exception because there were
        # worker failures
        if num_tasks_done < len(args):
            raise ValueError(
                "Gave up retrieving task results, most likely due to worker failures. "
                f"Received {num_tasks_done}/{len(args)} task results."
            )

    async def get_object_storage(self) -> ObjectStorage:
        storage_client = await self._get_storage_client()

        if storage_client is None or self.storage_bucket is None:
            raise ValueError(
                "Cannot use mirror_local without providing a storage_bucket. Please "
                "either use a different Deployment method or provide a storage_bucket"
            )
        return FuncWorkerClientObjectStorage(
            await storage_client.__aenter__(), self.storage_bucket
        )


class KubernetesGridJobDriver:
    """
    Similar to GridJobDriver, should potentially be merged with that code at some point
    """

    def __init__(
        self,
        kubernetes: Kubernetes,
        num_concurrent_tasks: int,
        storage_client: types_aiobotocore_s3.S3Client,
    ):
        self._kubernetes = kubernetes

        # properties of the job
        self._num_concurrent_tasks = num_concurrent_tasks
        self._storage_client = storage_client

        self._job_id = str(uuid.uuid4())

        # run_worker_functions will set this to indicate to get_results
        # that there all of our workers have either exited unexpectedly (and we have
        # given up trying to restore them), or have been told to shutdown normally
        self._no_workers_available = asyncio.Event()

        # these events aren't actually used right now, but for now we're keeping this
        # code similar to GridJobDriver with the goal of eventually merging these
        # classes
        self._workers_needed = num_concurrent_tasks
        self._workers_needed_changed = asyncio.Event()

        self._abort_launching_new_workers = asyncio.Event()

        self._worker_process_states: List[List[WorkerProcessState]] = []
        self._worker_process_state_received = asyncio.Event()

    # these three functions are effectively the GridJobCloudInterface

    async def _add_tasks(self, args: Sequence[Any]) -> None:
        assert self._kubernetes.storage_bucket is not None  # just for mypy

        ranges = await upload_task_args(
            self._storage_client, self._kubernetes.storage_bucket, self._job_id, args
        )
        # this is a hack--"normally" this would get sent with the "task assignment"
        # message, but we don't have the infrastructure for that in the case of Indexed
        # Jobs (static task-to-worker assignment)
        await self._storage_client.put_object(
            Bucket=self._kubernetes.storage_bucket,
            Key=storage_key_ranges(self._job_id),
            Body=pickle.dumps(ranges),
        )

    async def _receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
        assert self._kubernetes.storage_bucket is not None  # just for mypy

        return receive_results(
            self._storage_client,
            self._kubernetes.storage_bucket,
            self._job_id,
            stop_receiving=stop_receiving,
            all_workers_exited=workers_done,
            initial_wait_seconds=2,
            read_worker_process_states=self._kubernetes.reusable_pods,
        )

    async def _retry_task(self, task_id: int, attempts_so_far: int) -> None:
        raise NotImplementedError("Retries are not implemented for Kubernetes")

    def _worker_function_job(
        self,
        function: Callable[[_T], _U],
        num_args: int,
        job_fields: Dict[str, Any],
        pickle_protocol: int,
    ) -> Job:

        indexed_map_worker_args = (
            num_args,
            self._num_concurrent_tasks,
            function,
            self._kubernetes.storage_bucket,
            self._job_id,
            self._kubernetes.get_storage_endpoint_url_in_cluster(),
            pickle.HIGHEST_PROTOCOL,
        )

        return Job(
            job_id=self._job_id,
            py_function=PyFunctionJob(
                qualified_function_name=QualifiedFunctionName(
                    module_name=__name__,
                    function_name=_indexed_map_worker.__name__,
                ),
                pickled_function_arguments=cloudpickle.dumps(
                    (indexed_map_worker_args, None), protocol=pickle_protocol
                ),
            ),
            **job_fields,
        )

    async def run_worker_functions(
        self,
        function: Callable[[_T], _U],
        num_args: int,
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        pickle_protocol: int,
        wait_for_result: WaitOption,
    ) -> None:
        try:
            # we don't care about the worker completions--if they had an error, an
            # Exception will be raised, and the workers just return None
            await self._kubernetes._run_job_helper(
                self._storage_client,
                self._worker_function_job(
                    function, num_args, job_fields, pickle_protocol
                ),
                resources_required_per_task,
                self._num_concurrent_tasks,
                wait_for_result,
            )
            # TODO we should propagate a SIGINT here if we get it
        finally:
            self._no_workers_available.set()

    async def run_worker_functions_reusable_pods(
        self,
        function: Callable[[_T], _U],
        num_args: int,
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        pickle_protocol: int,
        wait_for_result: WaitOption,
    ) -> None:
        # TODO implement wait_for_result options

        all_pods = []
        pods_and_pids = []
        worker_process_state_received_task = asyncio.create_task(
            self._worker_process_state_received.wait()
        )
        workers_needed_changed_task = asyncio.create_task(
            self._workers_needed_changed.wait()
        )

        async with kubernetes_client.ApiClient() as api_client, kubernetes_stream.WsApiClient() as ws_api_client:  # noqa: E501
            try:
                if (
                    self._kubernetes.storage_bucket is None
                    or self._kubernetes.storage_endpoint_url is None
                ):
                    raise ValueError(
                        "Cannot use an environment_spec without providing a "
                        "storage_bucket. Please either provide a pre-built container "
                        "image for the interpreter or provide a storage_bucket"
                    )

                job = self._worker_function_job(
                    function, num_args, job_fields, pickle_protocol
                )

                interpreter_deployment_type = job.WhichOneof("interpreter_deployment")
                if interpreter_deployment_type in (
                    "container_at_digest",
                    "container_at_tag",
                    "server_available_container",
                ):
                    # TODO we would need to require that Meadowrun is installed in the
                    # custom container, so then we could just implement this by
                    # translating to ServerAvailableInterpreter(sys.executable)
                    raise NotImplementedError(
                        "Specifying a container image when running on Kubernetes with "
                        "reusable pods is not yet supported"
                    )

                await self._storage_client.put_object(
                    Bucket=self._kubernetes.storage_bucket,
                    Key=storage_key_job_to_run(self._job_id),
                    Body=job.SerializeToString(),
                )

                storage_endpoint = (
                    self._kubernetes.get_storage_endpoint_url_in_cluster()
                )
                if storage_endpoint is None:
                    raise ValueError("Storage endpoint must be specified")
                # TODO it's possible that a job will run more than once on the same
                # container, although not in parallel
                inner_command = (
                    "PYTHONUNBUFFERED=1 MEADOWRUN_WORKER_INDEX=%WORKER_INDEX% python -m"
                    " meadowrun.run_job_local_storage_main --storage-bucket "
                    f"{self._kubernetes.storage_bucket} --job-id {self._job_id} "
                    f"--storage-endpoint-url {storage_endpoint} "
                    f">/var/meadowrun/job_logs/{self._job_id}.log 2>&1 & echo $$"
                )
                # echo $$ gets the process id of the current bash session (we will run
                # inner_command in a bash session). We assume that all processes started
                # in this session will get this id as their process group id (it's not
                # clear if this assumption is bulletproof) so that we can kill them all
                # quickly. (100% reliable would be to do echo $! to get the
                # run_job_local_storage_main PID and then explicitly ask for its group
                # id and kill all processes in that group)

                all_tasks = []

                core_api = kubernetes_client.CoreV1Api(api_client)
                ws_core_api = kubernetes_client.CoreV1Api(ws_api_client)
                batch_api = kubernetes_client.BatchV1Api(api_client)

                worker_index = 0
                async for pods in _get_meadowrun_reusable_pods(
                    self._kubernetes.kubernetes_namespace,
                    _python_version_from_environment_spec(job),
                    [int(p) for p in expand_ports(job_fields["ports"])],
                    resources_required_per_task,
                    self._num_concurrent_tasks,
                    core_api,
                    batch_api,
                    self._kubernetes.storage_username_password_secret,
                ):
                    for pod in pods:
                        all_pods.append(pod)
                        all_tasks.append(
                            asyncio.create_task(
                                run_command_on_pod(
                                    pod.metadata.name,
                                    self._kubernetes.kubernetes_namespace,
                                    [
                                        "/bin/bash",
                                        "-c",
                                        inner_command.replace(
                                            "%WORKER_INDEX%", str(worker_index)
                                        ),
                                    ],
                                    ws_core_api,
                                )
                            )
                        )
                        worker_index += 1

                pods_and_pids = await asyncio.gather(*all_tasks, return_exceptions=True)

                # wait for all of the tasks to complete. If a worker fails, raise an
                # exception
                # TODO we should try to replace workers rather than just throwing an
                # exception immediately
                while self._workers_needed > 0:
                    await asyncio.wait(
                        [
                            worker_process_state_received_task,
                            workers_needed_changed_task,
                        ],
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if worker_process_state_received_task.done():
                        while self._worker_process_states:
                            for (
                                worker_process_state
                            ) in self._worker_process_states.pop():
                                # TODO periodically also check for our pods disappearing
                                if (
                                    worker_process_state.result.state
                                    != ProcessState.ProcessStateEnum.SUCCEEDED
                                ):
                                    raise MeadowrunException(
                                        worker_process_state.result
                                    )
                                # TODO keep track of successful worker exits?

                        self._worker_process_state_received.clear()
                        worker_process_state_received_task = asyncio.create_task(
                            self._worker_process_state_received.wait()
                        )

                    if workers_needed_changed_task.done():
                        self._workers_needed_changed.clear()
                        workers_needed_changed_task = asyncio.create_task(
                            self._workers_needed_changed.wait()
                        )
            except BaseException:
                # TODO no_workers_available should only be set if all of the workers
                # failed to start
                self._no_workers_available.set()

                # try to kill all of the worker processes
                await asyncio.gather(
                    *[
                        run_command_on_pod(
                            pod_name,
                            self._kubernetes.kubernetes_namespace,
                            ["kill", "-9", "--", f"-{pid}"],
                            ws_core_api,
                        )
                        for pod_name, pid in pods_and_pids
                    ],
                    return_exceptions=False,
                )
                raise
            finally:
                try:
                    # now mark those pods as ready--this is an optimization--the
                    # automated readiness probe will mark the pods as ready eventually,
                    # but this is faster
                    await asyncio.gather(
                        *(
                            set_main_container_ready(core_api, pod, True)
                            for pod in all_pods
                        ),
                        return_exceptions=True,
                    )

                    worker_process_state_received_task.cancel()
                    workers_needed_changed_task.cancel()

                    if self._kubernetes.storage_bucket is not None:
                        await self._storage_client.delete_object(
                            Bucket=self._kubernetes.storage_bucket,
                            Key=storage_key_job_to_run(self._job_id),
                        )
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    pass

    async def get_results(self, args: Sequence[_T]) -> AsyncIterable[TaskResult]:
        """Yields TaskResult objects as soon as tasks complete."""

        # semi-copy/paste from GridJobDriver.add_tasks_and_get_results

        num_tasks_done = 0
        # stop_receiving tells _cloud_interface.receive_task_results that there are no
        # more results to get
        stop_receiving = asyncio.Event()
        if len(args) == num_tasks_done:
            stop_receiving.set()
        last_printed_update = time.time()
        print(
            f"Waiting for task results. Requested: {len(args)}, "
            f"Done: {num_tasks_done}"
        )
        async for task_batch, worker_batch in await self._receive_task_results(
            stop_receiving=stop_receiving, workers_done=self._no_workers_available
        ):
            for task_process_state in task_batch:
                task_result = TaskResult.from_process_state(task_process_state)
                if task_result.is_success:
                    num_tasks_done += 1
                    yield task_result
                else:
                    print(f"Task {task_process_state.task_id} failed")
                    num_tasks_done += 1
                    yield task_result

            if worker_batch:
                self._worker_process_states.append(worker_batch)
                self._worker_process_state_received.set()

            if num_tasks_done >= len(args):
                stop_receiving.set()
            else:
                t0 = time.time()
                if t0 - last_printed_update > _PRINT_RECEIVED_TASKS_SECONDS:
                    print(
                        f"Waiting for task results. Requested: {len(args)}, "
                        f"Done: {num_tasks_done}"
                    )
                    last_printed_update = t0

            # reduce the number of workers needed if we have more workers than
            # outstanding tasks
            num_workers_needed = max(len(args) - num_tasks_done, 0)
            if num_workers_needed < self._workers_needed:
                self._workers_needed = num_workers_needed
                self._workers_needed_changed.set()

        if num_tasks_done < len(args):
            # It would make sense for this to raise an exception, but it's more helpful
            # to see the actual worker failures, and run_worker_functions should always
            # raise an exception in that case. The caller should still check though that
            # we returned all of the task results we were expecting.
            print(
                "Gave up retrieving task results, most likely due to worker failures. "
                f"Received {num_tasks_done}/{len(args)} task results."
            )
        else:
            print(f"Received all {len(args)} task results.")


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


def _add_storage_username_password_to_environment(
    storage_username_password_secret: Optional[str],
    environment: List[kubernetes_client.V1EnvVar],
    environment_variables: Dict[str, str],
) -> None:
    """
    Modifies environment in place!! environment_variables is just to make sure we don't
    overwrite an existing environment variable
    """

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

    _add_storage_username_password_to_environment(
        storage_username_password_secret, environment, environment_variables
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
            # we also try to delete manually. This field could be ignored if the TTL
            # controller is not available
            ttl_seconds_after_finished=10,
            backoff_limit=0,
            template=kubernetes_client.V1PodTemplateSpec(
                spec=kubernetes_client.V1PodSpec(
                    containers=[
                        kubernetes_client.V1Container(
                            name="main",
                            image=image,
                            args=args,
                            env=environment,
                            **_get_additional_container_parameters(ports, resources),
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

            pods = await get_pods_for_job(
                core_api, kubernetes_namespace, job_id, pod_generate_names
            )

            pod_names = [pod.metadata.name for pod in pods]
            print(f"Created pod(s) {', '.join(pod_names)} for job {job_id}")

            # Now that the pods have been created, stream their logs and get their exit
            # codes

            return await asyncio.gather(
                *[
                    asyncio.create_task(
                        wait_for_pod(
                            core_api, job_id, kubernetes_namespace, pod, wait_for_result
                        )
                    )
                    for pod in pods
                ]
            )
        finally:
            try:
                await batch_api.delete_namespaced_job(
                    job_id, kubernetes_namespace, propagation_policy="Foreground"
                )
            except kubernetes_client_exceptions.ApiException as e:
                print(f"Warning, error cleaning up job: {e}")


def _get_additional_container_parameters(
    ports: List[int], resources: Optional[ResourcesInternal]
) -> Dict[str, Any]:
    additional_container_parameters = {}
    if ports:
        additional_container_parameters["ports"] = [
            kubernetes_client.V1ContainerPort(container_port=port) for port in ports
        ]
    if resources is not None:
        additional_container_parameters[
            "resources"
        ] = kubernetes_client.V1ResourceRequirements(
            requests=_resources_to_kubernetes(resources)
        )
    return additional_container_parameters


def _pod_meets_requirements(
    pod: kubernetes_client.V1Pod,
    ports: List[int],
    resources: Optional[ResourcesInternal],
) -> bool:
    main_containers = [
        container for container in pod.spec.containers if container.name == "main"
    ]
    if len(main_containers) != 1:
        print(
            f"Warning: pod {pod.metadata.name} has {len(main_containers)} containers "
            f"named main, which was unexpected"
        )
        return False
    main_container = main_containers[0]

    if ports:
        if not main_container.ports:
            return False
        required_ports = set(ports)
        for port in main_container.ports:
            required_ports.discard(port.container_port)
        if required_ports:
            return False

    if resources:
        existing_resources = main_container.resources.requests
        for key, value in _resources_to_kubernetes(resources).items():
            if key not in existing_resources:
                return False
            # this str conversion is a bit sketchy. Also, we could do a >= check here,
            # but it seems better to just get the exact same resource requirements--the
            # existing pod will just disappear on its own
            if existing_resources[key] != str(value):
                return False

    return True


async def _get_meadowrun_reusable_pods(
    kubernetes_namespace: str,
    python_version: str,
    ports: List[int],
    resources: Optional[ResourcesInternal],
    number_of_pods: int,
    core_api: kubernetes_client.CoreV1Api,
    batch_api: kubernetes_client.BatchV1Api,
    storage_username_password_secret: Optional[str],
) -> AsyncIterable[List[kubernetes_client.V1Pod]]:

    pods_response = await core_api.list_namespaced_pod(
        kubernetes_namespace,
        # TODO also add ports and resources in selector
        label_selector=f"meadowrun.io/reusable-pod-python-version={python_version}",
        # Available fields:
        # https://github.com/kubernetes/kubernetes/blob/f01c9e8683adacbfbad58e5153dfac9ebf954c4b/pkg/registry/core/pod/strategy.go#L301
        field_selector="status.phase=Running",
    )
    additional_container_parameters = _get_additional_container_parameters(
        ports, resources
    )
    existing_pods: List[kubernetes_client.V1Pod] = []
    for pod in pods_response.items:
        if len(existing_pods) >= number_of_pods:
            break
        if get_main_container_is_ready(pod) and _pod_meets_requirements(
            pod, ports, resources
        ):
            # This is an "optimistic concurrency"-style check. It's possible that the
            # automated readiness probe will run between now and when we're able to
            # launch our job, remarking this pod as ready and allowing another job to
            # swoop in and steal the pod. In that case, the job will fail to start up
            # because it won't be able to get the _JOB_IS_RUNNING file lock, and we can
            # request another pod
            await set_main_container_ready(core_api, pod, False)
            existing_pods.append(pod)

    # limit may be ignored according to the Kubernetes API spec
    # TODO pods that say "terminating" in the dashboard show up as "running" in this
    # query
    if len(existing_pods) > number_of_pods:
        existing_pods = existing_pods[:number_of_pods]
    if existing_pods:
        print(f"Reusing {len(existing_pods)} existing pods")
        yield existing_pods

    remaining_pods_to_launch = number_of_pods - len(existing_pods)

    if remaining_pods_to_launch > 0:
        image_name = f"meadowrun/meadowrun:{__version__}-py{python_version}"

        environment: List[kubernetes_client.V1EnvVar] = []

        _add_storage_username_password_to_environment(
            storage_username_password_secret, environment, {}
        )

        job_name = f"mdr-reusable-{uuid.uuid4()}"
        # it would be way more convenient to do -m
        # meadowrun.k8s_integration.is_job_running, but that is way slower because it
        # imports everything (i.e. boto3)
        is_job_running_path = (
            f"/usr/local/lib/python{python_version}/site-packages/meadowrun/"
            "k8s_integration/is_job_running.py"
        )
        body = kubernetes_client.V1Job(
            metadata=kubernetes_client.V1ObjectMeta(name=job_name),
            spec=kubernetes_client.V1JobSpec(
                ttl_seconds_after_finished=10,
                backoff_limit=0,
                template=kubernetes_client.V1PodTemplateSpec(
                    metadata=kubernetes_client.V1ObjectMeta(
                        labels={
                            "meadowrun.io/reusable-pod-python-version": python_version
                        }
                    ),
                    spec=kubernetes_client.V1PodSpec(
                        containers=[
                            kubernetes_client.V1Container(
                                name="main",
                                image=image_name,
                                args=[
                                    "python",
                                    "-m",
                                    "meadowrun.k8s_integration.k8s_main",
                                ],
                                env=environment,
                                readiness_probe=kubernetes_client.V1Probe(
                                    _exec=kubernetes_client.V1ExecAction(
                                        command=["python", is_job_running_path]
                                    ),
                                    initial_delay_seconds=15,
                                    period_seconds=15,
                                    timeout_seconds=5,
                                    failure_threshold=1,
                                    success_threshold=1,
                                ),
                                **additional_container_parameters,
                            )
                        ],
                        restart_policy="Never",
                    ),
                ),
                completion_mode="Indexed",
                completions=remaining_pods_to_launch,
                parallelism=remaining_pods_to_launch,
            ),
        )

        await batch_api.create_namespaced_job(kubernetes_namespace, body)

        pods = await get_pods_for_job(
            core_api,
            kubernetes_namespace,
            job_name,
            [f"{job_name}-{i}-" for i in range(remaining_pods_to_launch)],
        )
        for pod_future in asyncio.as_completed(
            [
                wait_for_pod_running(core_api, job_name, kubernetes_namespace, pod)
                for pod in pods
            ]
        ):
            yield [await pod_future]

        print(f"Started {remaining_pods_to_launch} new pods")
