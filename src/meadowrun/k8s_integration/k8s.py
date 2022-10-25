from __future__ import annotations

import abc
import argparse
import asyncio
import dataclasses
import math
import os
import pickle
import platform
import shlex
import time
import traceback
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Coroutine,
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
from meadowrun.config import (
    EPHEMERAL_STORAGE_GB,
    GPU,
    LOGICAL_CPU,
    MEADOWRUN_INTERPRETER,
    MEMORY_GB,
)
from meadowrun.credentials import KubernetesSecretRaw
from meadowrun.docker_controller import expand_ports
from meadowrun.k8s_integration.k8s_core import (
    get_main_container_is_ready,
    get_pods_for_job,
    run_command_on_pod,
    run_command_on_pod_and_stream,
    set_main_container_ready,
    wait_for_pods,
    wait_for_pods_running,
)
from meadowrun.k8s_integration.k8s_main import IS_JOB_RUNNING_COMMAND
from meadowrun.meadowrun_pb2 import (
    Job,
    ProcessState,
    PyAgentJob,
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
    TaskWorkerServer,
    WorkerMonitor,
    _get_credentials_for_docker,
    _get_credentials_sources,
    _string_pairs_to_dict,
    restart_worker,
)
from meadowrun.shared import b32_encoded_uuid
from meadowrun.storage_grid_job import (
    complete_task,
    download_task_arg,
    get_job_completion_from_process_state,
    receive_results,
    upload_task_args,
)
from meadowrun.storage_keys import (
    storage_key_job_to_run,
    storage_key_ranges,
    storage_key_task_args,
)
from meadowrun.version import __version__

if TYPE_CHECKING:
    from asyncio import Task

    from meadowrun.abstract_storage_bucket import AbstractStorageBucket
    from meadowrun.instance_selection import ResourcesInternal
    from meadowrun.run_job_core import WorkerProcessState, TaskProcessState

MEADOWRUN_POD_NAME = "MEADOWRUN_POD_NAME"


_T = TypeVar("_T")
_U = TypeVar("_U")


async def _indexed_map_worker(
    total_num_tasks: int,
    num_workers: int,
    job_id: str,
    worker_server: TaskWorkerServer,
    worker_monitor: WorkerMonitor,
) -> None:
    """
    This is a worker function to help with running a run_map. This worker assumes that
    JOB_COMPLETION_INDEX is set, which Kubernetes will set for indexed completion jobs.
    This worker assumes task arguments are accessible via
    meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET and will just
    complete all of the tasks where task_index % num_workers == current worker index.
    """

    # WORKER_INDEX will be available in reusable pods. In non-reusable pods we have to
    # use JOB_COMPLETION_INDEX
    current_worker_index = int(
        os.environ.get("MEADOWRUN_WORKER_INDEX", os.environ["JOB_COMPLETION_INDEX"])
    )

    # we're always being called from run_job_local_storage_main which sets these
    # variables for us
    storage_bucket = meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET
    if storage_bucket is None:
        raise ValueError(
            "Programming error--_indexed_map_worker must be called from "
            "run_job_local_storage_main"
        )

    # only the first option (MEADOWRUN_POD_NAME) will give the actual name of the pod.
    # The hostname is similar but slightly different, and we'll fall back on that if
    # something has gone wrong and MEADOWRUN_POD_NAME is not available
    pod_name = os.environ.get(
        MEADOWRUN_POD_NAME, os.environ.get("HOSTNAME", platform.node())
    )
    log_file_name = f"{pod_name}:/var/meadowrun/job_logs/{job_id}.log"

    byte_ranges = pickle.loads(
        await storage_bucket.get_bytes(storage_key_ranges(job_id))
    )

    i = current_worker_index
    while i < total_num_tasks:
        arg = await download_task_arg(storage_bucket, job_id, byte_ranges[i])

        try:
            worker_monitor.start_stats()
            await worker_server.send_message(arg)
            state, result_bytes = await worker_server.receive_message()
            stats = await worker_monitor.stop_stats()

            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED
                if state == "SUCCEEDED"
                else ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pickled_result=result_bytes,
                return_code=0,
                max_memory_used_gb=stats.max_memory_used_gb,
                log_file_name=log_file_name,
            )
        except BaseException:
            stats = await worker_monitor.stop_stats()
            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.UNEXPECTED_WORKER_EXIT,
                return_code=(await worker_monitor.try_get_return_code()) or 0,
                max_memory_used_gb=stats.max_memory_used_gb,
                log_file_name=log_file_name,
            )
            await restart_worker(worker_server, worker_monitor)

        # we don't support retries yet so we're always on attempt 1
        await complete_task(storage_bucket, job_id, i, 1, process_state)

        print(
            f"Meadowrun agent: Completed task #{i}, "
            f"state {ProcessState.ProcessStateEnum.Name(process_state.state)}, max "
            f"memory {process_state.max_memory_used_gb}GB "
        )

        i += num_workers


async def _image_name_from_job(job: Job) -> Tuple[bool, str, Optional[str], Job]:
    """
    Returns is_custom_container_image, image_name, image_pull_secret_name, and
    modified_job. modified_job should NOT be used with run_direct_command--it has been
    modified to run inside of a custom container.
    """

    # get the image_repository_name and image_name
    interpreter_deployment_type = job.WhichOneof("interpreter_deployment")

    if interpreter_deployment_type == "container_at_digest":
        is_custom_container_image = True
        image_repository_name = job.container_at_digest.repository
        image_name = (
            f"{job.container_at_digest.repository}@" f"{job.container_at_digest.digest}"
        )
    elif interpreter_deployment_type == "container_at_tag":
        is_custom_container_image = True
        image_repository_name = job.container_at_tag.repository
        image_name = f"{job.container_at_tag.repository}:{job.container_at_tag.tag}"
    elif interpreter_deployment_type == "server_available_container":
        is_custom_container_image = True
        image_repository_name = None
        image_name = f"{job.server_available_container.image_name}"
    else:
        is_custom_container_image = False
        image_repository_name = None
        # This else block implies that the user specified an environment spec rather
        # than a container. Our approach is to default to a "generic container image"
        # and build the environment inside of the container. We could imagine other
        # approaches like building the container locally and uploading it to Kubernetes.

        if interpreter_deployment_type == "environment_spec_in_code":
            python_version = job.environment_spec_in_code.python_version
        elif interpreter_deployment_type == "environment_spec":
            python_version = job.environment_spec.python_version
        elif interpreter_deployment_type == "server_available_interpreter":
            python_version = None
        else:
            raise ValueError(
                "Programming error: unknown interpreter deployment type "
                f"{interpreter_deployment_type}"
            )

        if not python_version:
            # conda environments and raw server_available_interpreter won't have a
            # python version
            python_version = "3.10"

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

    if is_custom_container_image:
        # if we have a custom container and we're going to run a job on it via
        # run_job_local_storage_main, we need to tell run_job_local_storage_main to just
        # use the container's interpreter (rather than trying to go fetch another
        # container)

        # it's a little paranoid to make a copy here, this could probably be optimized
        # if we need to for performance
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

    return is_custom_container_image, image_name, image_pull_secret_name, modified_job


class StorageBucketSpec(abc.ABC):
    """
    An abstract class that specifies an object storage system that Meadowrun can use to
    send data back and forth from remote workers
    """

    @abc.abstractmethod
    async def get_storage_bucket(
        self, kubernetes_namespace: str
    ) -> AbstractStorageBucket:
        ...

    @abc.abstractmethod
    def get_command_line_arguments(self) -> List[str]:
        # Returns command line arguments that can be parsed via add_arguments_to_parser
        # and from_parsed_args to re-create the storage spec in a remote process
        ...

    @abc.abstractmethod
    def get_environment_variables(self) -> Iterable[kubernetes_client.V1EnvVar]:
        # Environment variables returned here will be added to the pod spec
        ...

    @classmethod
    @abc.abstractmethod
    def get_storage_type(cls) -> str:
        # This is used together with get_command_line_arguments so that a remote process
        # knows what sub-class of StorageBucketSpec to use to re-create the storage spec
        ...

    @classmethod
    @abc.abstractmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        # See get_command_line_arguments
        ...

    @classmethod
    @abc.abstractmethod
    async def from_parsed_args(cls, args: argparse.Namespace) -> AbstractStorageBucket:
        # See get_command_line_arguments
        ...


@dataclasses.dataclass(frozen=True)
class Kubernetes(Host):
    """
    Specifies a Kubernetes cluster to run a Meadowrun job on. resources_required is
    optional with the Kubernetes Host.

    Attributes:
        storage_spec: Specifies the object storage system to use. See derived classes of
            [StorageBucketSpec][meadowrun.StorageBucketSpec]. This can only be omitted
            if you are only using run_command and you've specified a specific container
            image to run on (i.e. rather than an EnvironmentSpec of some sort)
        kube_config_context: Specifies the kube config context to use. Default is None
            which means use the current context (i.e. `kubectl config current-context`)
        kubernetes_namespace: The Kubernetes namespace that Meadowrun will create Jobs
            in. This should usually not be left to the default value ("default") for any
            "real" workloads.
        resuable_pods: When set to True, starts generic long-lived pods that can be
            reused for multiple jobs. When set to False, starts a new pod(s) for every
            job
        pod_customization: A function like pod_customization(pod_template_spec) that
            will be called on the PodTemplateSpec just before we submit it to
            Kubernetes. You can make changes like specifying a serviceAccountName,
            adding ephemeral storage, etc. You can either modify the pod_template_spec
            argument in place and return it as the result, or construct a new
            [V1PodTemplateSpec](https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodTemplateSpec.md)
            and return that.
    """

    storage_spec: Optional[StorageBucketSpec] = None
    kube_config_context: Optional[str] = None
    kubernetes_namespace: str = "default"
    reusable_pods: bool = True
    pod_customization: Optional[
        Callable[
            [kubernetes_client.V1PodTemplateSpec], kubernetes_client.V1PodTemplateSpec
        ]
    ] = None

    async def set_defaults(self) -> None:
        # this function needs to be called before anything else happens with the
        # Kubernetes object
        await kubernetes_config.load_kube_config(context=self.kube_config_context)

    async def get_storage_bucket(self) -> AbstractStorageBucket:
        if self.storage_spec is None:
            raise ValueError(
                "The functionality you are trying to use requires specifying a "
                "storage_bucket. Please specify a storage_spec."
            )

        return await self.storage_spec.get_storage_bucket(self.kubernetes_namespace)

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
        (
            is_custom_container_image,
            image_name,
            image_pull_secret_name,
            modified_job,
        ) = await _image_name_from_job(job)

        # now, if we have a custom container, try to run the job as a direct command. If
        # we don't have a custom container we "can't" run a direct command. We
        # theoretically could with a generic container if we asked for a preinstalled
        # interpreter, but there doesn't seem to be any real point in doing that.
        if is_custom_container_image:
            result = await self._run_direct_command_if_possible(
                job,
                resources_required,
                1,
                image_name,
                image_pull_secret_name,
                wait_for_result,
            )
            if result is not None:
                return result

        # if _run_direct_command_if_possible returns None, our job can't be run as a
        # direct command, so we need to run via run_job_local_storage_main.py
        return await self._run_regular_job(
            modified_job,
            image_name,
            image_pull_secret_name,
            resources_required,
            wait_for_result,
        )

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
            async with kubernetes_client.ApiClient() as api_client:
                return_codes = await _run_kubernetes_job(
                    kubernetes_client.CoreV1Api(api_client),
                    kubernetes_client.BatchV1Api(api_client),
                    job.job_id,
                    self.kubernetes_namespace,
                    image_name,
                    command,
                    environment_variables,
                    self.storage_spec,
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

    async def _run_regular_job(
        self,
        job: Job,
        image_name: str,
        image_pull_secret_name: Optional[str],
        resources_required: Optional[ResourcesInternal],
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        # This is the "normal" way to run jobs on Kubernetes. We assume that the image
        # specified by image_name has Meadowrun set up in it. We use the storage_bucket
        # to send a Job object via S3-compatible object storage and run
        # run_job_local_storage_main which will actually run our job. The results will
        # also come back via the object storage.

        # keeps track of most of what we write to object storage so we can clean it up
        # when we're done
        storage_keys_used = []

        if self.reusable_pods:
            process: KubernetesRemoteProcesses = ReusablePodRemoteProcesses()
        else:
            process = SingleUsePodRemoteProcesses()

        async with await self.get_storage_bucket() as storage_bucket, kubernetes_client.ApiClient() as api_client, kubernetes_stream.WsApiClient() as ws_api_client:  # noqa: E501
            # just for mypy, get_storage_bucket requires this
            assert self.storage_spec is not None

            core_api = kubernetes_client.CoreV1Api(api_client)
            ws_core_api = kubernetes_client.CoreV1Api(ws_api_client)
            batch_api = kubernetes_client.BatchV1Api(api_client)

            try:
                job_to_run_key = storage_key_job_to_run(job.job_id)
                storage_keys_used.append(job_to_run_key)
                await storage_bucket.write_bytes(
                    job.SerializeToString(), job_to_run_key
                )

                try:
                    return_codes = await (
                        await process.run(
                            core_api,
                            batch_api,
                            ws_core_api,
                            self.kubernetes_namespace,
                            image_name,
                            image_pull_secret_name,
                            [int(p) for p in expand_ports(job.ports)],
                            resources_required,
                            self.storage_spec,
                            job.job_id,
                            1,
                            wait_for_result,
                            self.pod_customization,
                        )
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException as e:
                    raise MeadowrunException(
                        ProcessState(
                            state=ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED,
                        )
                    ) from e

                if return_codes is not None:
                    if len(return_codes) != 1:
                        raise ValueError(
                            "Programming error, expected 1 return_codes but got "
                            f"{len(return_codes)}"
                        )
                    return_code = return_codes[0]
                    if return_code != 0:
                        raise MeadowrunException(
                            ProcessState(
                                state=(
                                    ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
                                ),
                                return_code=return_code,
                            )
                        )
                    # this should be 0 in an ideal world, but seems worth it to have one
                    # retry
                    timeout_seconds = 1
                else:
                    # TODO make this configurable
                    timeout_seconds = 60 * 60 * 24

                job_spec_type = job.WhichOneof("job_spec")
                if job_spec_type is None:
                    raise ValueError("Unexpected, job.job_spec is None")
                result = await get_job_completion_from_process_state(
                    storage_bucket,
                    job.job_id,
                    "0",
                    job_spec_type,
                    timeout_seconds,
                    "kubernetes",  # TODO get the name of the pod that ran the job?
                )
                process.received_result(0)
                return result
            finally:
                await process.kill_all()
                # TODO we should separately periodically clean up these files in case we
                # aren't able to execute this finally block
                if storage_bucket is not None:
                    await asyncio.gather(
                        *(
                            storage_bucket.delete_object(storage_key)
                            for storage_key in storage_keys_used
                        ),
                        return_exceptions=True,
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

        async with await self.get_storage_bucket() as storage_bucket:
            # pretty much copied from AllocVM.run_map_as_completed

            driver = KubernetesGridJobDriver(self, num_concurrent_tasks, storage_bucket)

            # this should be in get_results, but with indexed workers we need to make
            # sure the tasks are uploaded before we can start workers
            await driver._add_tasks(args)

            try:
                run_worker_loops = asyncio.create_task(
                    driver.run_worker_functions(
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
                    *(storage_bucket.delete_object(key) for key in keys_to_delete),
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


class KubernetesGridJobDriver:
    """
    Similar to GridJobDriver, should potentially be merged with that code at some point
    """

    def __init__(
        self,
        kubernetes: Kubernetes,
        num_concurrent_tasks: int,
        storage_bucket: AbstractStorageBucket,
    ):
        self._kubernetes = kubernetes

        # properties of the job
        self._num_concurrent_tasks = num_concurrent_tasks
        self._storage_bucket = storage_bucket

        self._job_id = str(uuid.uuid4())
        print(f"GridJob id is {self._job_id}")

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
        ranges = await upload_task_args(self._storage_bucket, self._job_id, args)
        # this is a hack--"normally" this would get sent with the "task assignment"
        # message, but we don't have the infrastructure for that in the case of Indexed
        # Jobs (static task-to-worker assignment)
        await self._storage_bucket.write_bytes(
            pickle.dumps(ranges),
            storage_key_ranges(self._job_id),
        )

    async def _receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
        return receive_results(
            self._storage_bucket,
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

        indexed_map_worker_args = num_args, self._num_concurrent_tasks, self._job_id

        return Job(
            job_id=self._job_id,
            py_agent=PyAgentJob(
                pickled_function=cloudpickle.dumps(function, protocol=pickle_protocol),
                pickled_agent_function=cloudpickle.dumps(
                    _indexed_map_worker, protocol=pickle_protocol
                ),
                pickled_agent_function_arguments=cloudpickle.dumps(
                    (indexed_map_worker_args, {}), protocol=pickle_protocol
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
        # TODO implement wait_for_result options

        if self._kubernetes.reusable_pods:
            all_processes: KubernetesRemoteProcesses = ReusablePodRemoteProcesses()
        else:
            all_processes = SingleUsePodRemoteProcesses()

        worker_launch_task = None
        worker_process_state_received_task = asyncio.create_task(
            self._worker_process_state_received.wait()
        )
        workers_needed_changed_task = asyncio.create_task(
            self._workers_needed_changed.wait()
        )

        async with kubernetes_client.ApiClient() as api_client, kubernetes_stream.WsApiClient() as ws_api_client:  # noqa: E501
            try:
                # just for mypy, get_storage_bucket requires this
                assert self._kubernetes.storage_spec is not None

                job = self._worker_function_job(
                    function, num_args, job_fields, pickle_protocol
                )
                (
                    is_custom_container_image,
                    image_name,
                    image_pull_secret_name,
                    modified_job,
                ) = await _image_name_from_job(job)

                await self._storage_bucket.write_bytes(
                    modified_job.SerializeToString(),
                    storage_key_job_to_run(self._job_id),
                )

                core_api = kubernetes_client.CoreV1Api(api_client)
                ws_core_api = kubernetes_client.CoreV1Api(ws_api_client)
                batch_api = kubernetes_client.BatchV1Api(api_client)

                worker_launch_task = await all_processes.run(
                    core_api,
                    batch_api,
                    ws_core_api,
                    self._kubernetes.kubernetes_namespace,
                    image_name,
                    image_pull_secret_name,
                    [int(p) for p in expand_ports(job_fields["ports"])],
                    resources_required_per_task,
                    self._kubernetes.storage_spec,
                    self._job_id,
                    self._num_concurrent_tasks,
                    wait_for_result,
                    self._kubernetes.pod_customization,
                )

                # wait for all of the tasks to complete. If a worker fails, raise an
                # exception
                while self._workers_needed > 0:
                    tasks_to_wait: List[Task] = [
                        worker_process_state_received_task,
                        workers_needed_changed_task,
                    ]
                    if worker_launch_task is not None:
                        tasks_to_wait.append(worker_launch_task)
                    await asyncio.wait(
                        tasks_to_wait,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if worker_process_state_received_task.done():
                        while self._worker_process_states:
                            for (
                                worker_process_state
                            ) in self._worker_process_states.pop():
                                all_processes.received_result(
                                    int(worker_process_state.worker_index)
                                )

                                # TODO periodically also check for our pods disappearing
                                if (
                                    worker_process_state.result.state
                                    != ProcessState.ProcessStateEnum.SUCCEEDED
                                ):
                                    # TODO we should try to replace workers rather than
                                    # just throwing an exception immediately
                                    raise MeadowrunException(
                                        worker_process_state.result
                                    )

                        self._worker_process_state_received.clear()
                        worker_process_state_received_task = asyncio.create_task(
                            self._worker_process_state_received.wait()
                        )

                    if workers_needed_changed_task.done():
                        self._workers_needed_changed.clear()
                        workers_needed_changed_task = asyncio.create_task(
                            self._workers_needed_changed.wait()
                        )

                    if worker_launch_task is not None and worker_launch_task.done():
                        # first, this will raise any exceptions from this task. Then, we
                        # check the result. If the result is not None, that means the
                        # workers launched and ran to completion and we got back a
                        # return code, so we should exit this loop. If we got back a
                        # None, that means the workers launched successfully (as far as
                        # we can tell), and we're not planning on getting notified when
                        # they finish. If we got rid of the single-use pods option, we
                        # could get rid of the break, but we would still want to call
                        # .result() here to make sure we see any exceptions
                        if worker_launch_task.result() is not None:
                            break

                        # don't keep including this in asyncio.wait
                        worker_launch_task = None
            finally:
                try:
                    self._no_workers_available.set()

                    # make sure we kill any remote processes and finish awaiting any
                    # deallocation tasks
                    await all_processes.kill_all()

                    worker_process_state_received_task.cancel()
                    workers_needed_changed_task.cancel()
                    if worker_launch_task is not None:
                        worker_launch_task.cancel()

                    await self._storage_bucket.delete_object(
                        storage_key_job_to_run(self._job_id)
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


def _resources_to_kubernetes(resources: ResourcesInternal) -> Dict[str, str]:
    result = {}

    if LOGICAL_CPU in resources.consumable:
        # The smallest unit of CPU available is 1m which means 1/1000 of a CPU, aka 1
        # millicpu:
        # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
        millicpu = math.ceil(resources.consumable[LOGICAL_CPU] * 1000)
        if millicpu > 0:
            result["cpu"] = f"{millicpu}m"
    if MEMORY_GB in resources.consumable:
        # The smallest unit of memory available is 1 byte
        # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
        memory = math.ceil(resources.consumable[MEMORY_GB] * (1024**3))
        if memory > 0:
            result["memory"] = str(memory)
    if EPHEMERAL_STORAGE_GB in resources.non_consumable:
        # The smallest unit of ephemeral storage available is 1 byte
        # https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#setting-requests-and-limits-for-local-ephemeral-storage
        ephemeral_storage = math.ceil(
            resources.non_consumable[EPHEMERAL_STORAGE_GB] * (1024**3)
        )
        if ephemeral_storage > 0:
            result["ephemeral-storage"] = str(ephemeral_storage)
    if GPU in resources.consumable:
        num_gpus = math.ceil(resources.consumable[GPU])
        if "nvidia" in resources.non_consumable:
            result["nvidia.com/gpu"] = str(num_gpus)
        else:
            raise ValueError(
                "Must specify a type of GPU (e.g. nvidia) if a GPU resource is "
                "requested"
            )

    # TODO maybe warn if people are trying to use resources that we don't know how to
    # interpret
    # TODO maybe turn max_eviction_rate into a pod disruption budget?

    return result


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


_RESOURCE_VALUE_SUFFIXES = {
    "E": 1000**6,
    "P": 1000**5,
    "T": 1000**4,
    "G": 1000**3,
    "M": 1000**2,
    "k": 1000,
    "m": 1000**-1,
    "Ei": 1024**6,
    "Pi": 1024**5,
    "Ti": 1024**4,
    "Gi": 1024**3,
    "Mi": 1024**2,
    "Ki": 1024,
}


def _parse_resource_value(value: str) -> float:
    for suffix, factor in _RESOURCE_VALUE_SUFFIXES.items():
        if value.endswith(suffix):
            return float(value[: -len(suffix)]) * factor
    return float(value)


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
            if (
                abs(
                    _parse_resource_value(existing_resources[key])
                    - _parse_resource_value(value)
                )
                > 1e-3
            ):
                return False
            # TODO we should also consider not reusing any pods that have "extra"
            # resources. I.e. don't run a job that doesn't need a GPU on a pod that has
            # a GPU

    return True


def _add_meadowrun_variables_to_environment(
    environment: List[kubernetes_client.V1EnvVar],
    environment_variables: Dict[str, str],
    storage_spec: Optional[StorageBucketSpec],
) -> None:
    """
    Modifies environment in place!! environment_variables is just to make sure we don't
    overwrite an existing environment variable
    """

    if storage_spec is not None:
        for env_var in storage_spec.get_environment_variables():
            if env_var.name not in environment_variables:
                environment.append(env_var)

    if MEADOWRUN_POD_NAME not in environment_variables:
        environment.append(
            kubernetes_client.V1EnvVar(
                name=MEADOWRUN_POD_NAME,
                value_from=kubernetes_client.V1EnvVarSource(
                    field_ref=kubernetes_client.V1ObjectFieldSelector(
                        field_path="metadata.name"
                    )
                ),
            )
        )


class KubernetesRemoteProcesses(abc.ABC):
    """An interface for running a job on Kubernetes"""

    @abc.abstractmethod
    async def run(
        self,
        core_api: kubernetes_client.CoreV1Api,
        batch_api: kubernetes_client.BatchV1Api,
        ws_core_api: kubernetes_client.CoreV1Api,
        kubernetes_namespace: str,
        image_name: str,
        image_pull_secret_name: Optional[str],
        ports: List[int],
        resources: Optional[ResourcesInternal],
        storage_spec: StorageBucketSpec,
        job_id: str,
        # TODO this will probably eventually need to be replaced with an explicit list
        # of worker indexes if we want to be able to restart workers
        num_executions: int,
        wait_for_result: WaitOption,
        pod_customization: Optional[
            Callable[
                [kubernetes_client.V1PodTemplateSpec],
                kubernetes_client.V1PodTemplateSpec,
            ]
        ] = None,
    ) -> Task[Optional[Sequence[int]]]:
        """
        Assumes that the Job has already been uploaded to object storage.

        If this returns a Sequence[int], that means the jobs ran to completion and we
        are returning the exit codes. If this returns None, that means we just launched
        the jobs and then detached, and the caller is responsible for figuring out when
        the job completes.
        """
        ...

    @abc.abstractmethod
    def received_result(self, worker_index: int) -> None:
        """
        This notifies us that the specified worker completed. We will do any
        deallocation necessary and mark this as a worker that we don't need to
        explicitly kill.
        """
        ...

    @abc.abstractmethod
    def kill_all(self) -> Awaitable[Any]:
        """
        Kills all of the workers. This function MUST be called after run in a finally
        block, even if we've successfully received results for all of the workers.

        Return type should be None but it's more trouble than it's worth to make that
        work in the implementation
        """
        ...


class SingleUsePodRemoteProcesses(KubernetesRemoteProcesses):
    def __init__(self) -> None:
        self.run_task: Optional[Task[Optional[Sequence[int]]]] = None
        self.job_has_finished: bool = False

    async def run(
        self,
        core_api: kubernetes_client.CoreV1Api,
        batch_api: kubernetes_client.BatchV1Api,
        ws_core_api: kubernetes_client.CoreV1Api,
        kubernetes_namespace: str,
        image_name: str,
        image_pull_secret_name: Optional[str],
        ports: List[int],
        resources: Optional[ResourcesInternal],
        storage_spec: StorageBucketSpec,
        job_id: str,
        num_executions: int,
        wait_for_result: WaitOption,
        pod_customization: Optional[
            Callable[
                [kubernetes_client.V1PodTemplateSpec],
                kubernetes_client.V1PodTemplateSpec,
            ]
        ] = None,
    ) -> Task[Optional[Sequence[int]]]:
        command = [
            "python",
            "-m",
            "meadowrun.run_job_local_storage_main",
            "--job-id",
            job_id,
            STORAGE_TYPE,
            storage_spec.get_storage_type(),
        ] + storage_spec.get_command_line_arguments()

        self.run_task = asyncio.create_task(
            _run_kubernetes_job(
                core_api,
                batch_api,
                job_id,
                kubernetes_namespace,
                image_name,
                command,
                {"PYTHONUNBUFFERED": "1"},
                storage_spec,
                image_pull_secret_name,
                num_executions,
                ports,
                resources,
                wait_for_result,
                pod_customization,
            )
        )

        return self.run_task

    def received_result(self, worker_index: int) -> None:
        # this is fine for now, but when we implement kill_all correctly, we will want
        # to keep track of these completions
        self.job_has_finished = True
        if self.run_task is not None:
            self.run_task.cancel()

    async def kill_all(self) -> None:
        if not self.job_has_finished:
            if self.run_task is not None:
                # this works right now because _run_kubernetes_job has a finally block
                # that always deletes the job it creates
                self.run_task.cancel()


async def _run_kubernetes_job(
    core_api: kubernetes_client.CoreV1Api,
    batch_api: kubernetes_client.BatchV1Api,
    job_id: str,
    kubernetes_namespace: str,
    image: str,
    args: List[str],
    environment_variables: Dict[str, str],
    storage_spec: Optional[StorageBucketSpec],
    image_pull_secret_name: Optional[str],
    indexed_completions: Optional[int],
    ports: List[int],
    resources: Optional[ResourcesInternal],
    wait_for_result: WaitOption,
    pod_customization: Optional[
        Callable[
            [kubernetes_client.V1PodTemplateSpec],
            kubernetes_client.V1PodTemplateSpec,
        ]
    ] = None,
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

    _add_meadowrun_variables_to_environment(
        environment, environment_variables, storage_spec
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
    pod_template_spec = kubernetes_client.V1PodTemplateSpec(
        metadata=kubernetes_client.V1ObjectMeta(labels={"meadowrun.io": "true"}),
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
        ),
    )
    if pod_customization:
        pod_template_spec = pod_customization(pod_template_spec)
    job_name = f"mdr-{job_id}"
    body = kubernetes_client.V1Job(
        metadata=kubernetes_client.V1ObjectMeta(
            name=job_name, labels={"meadowrun.io": "true"}
        ),
        spec=kubernetes_client.V1JobSpec(
            # we also try to delete manually. This field could be ignored if the TTL
            # controller is not available
            ttl_seconds_after_finished=10,
            backoff_limit=0,
            template=pod_template_spec,
            **additional_job_spec_parameters,
        ),
    )

    try:
        # this returns a result, but we don't really need anything from it
        await batch_api.create_namespaced_job(kubernetes_namespace, body)

        # Now that we've created the job, wait for the pods to be created

        if indexed_completions:
            pod_generate_names = [
                f"{job_name}-{i}-" for i in range(indexed_completions)
            ]
        else:
            pod_generate_names = [f"{job_name}-"]

        pods = await get_pods_for_job(
            core_api, kubernetes_namespace, job_name, pod_generate_names
        )

        pod_names = [pod.metadata.name for pod in pods]
        print(f"Created pod(s) {', '.join(pod_names)} for job {job_name}")

        # Now that the pods have been created, stream their logs and get their exit
        # codes
        return await wait_for_pods(
            core_api, job_name, kubernetes_namespace, pods, wait_for_result
        )
    finally:
        try:
            await batch_api.delete_namespaced_job(
                job_name, kubernetes_namespace, propagation_policy="Foreground"
            )
        except kubernetes_client_exceptions.ApiException:
            print("Warning, error cleaning up job:\n" + traceback.format_exc())


class ReusablePodRemoteProcesses(KubernetesRemoteProcesses):
    def __init__(self) -> None:
        self.all_processes: List[ReusablePodRemoteProcess] = []
        self.deallocation_tasks: List[Task[None]] = []

    async def run(
        self,
        core_api: kubernetes_client.CoreV1Api,
        batch_api: kubernetes_client.BatchV1Api,
        ws_core_api: kubernetes_client.CoreV1Api,
        kubernetes_namespace: str,
        image_name: str,
        image_pull_secret_name: Optional[str],
        ports: List[int],
        resources: Optional[ResourcesInternal],
        storage_spec: StorageBucketSpec,
        job_id: str,
        num_executions: int,
        wait_for_result: WaitOption,
        pod_customization: Optional[
            Callable[
                [kubernetes_client.V1PodTemplateSpec],
                kubernetes_client.V1PodTemplateSpec,
            ]
        ] = None,
    ) -> Task[Optional[Sequence[int]]]:

        storage_spec_args = (
            f"{STORAGE_TYPE} {storage_spec.get_storage_type()} "
            + " ".join(
                shlex.quote(arg) for arg in storage_spec.get_command_line_arguments()
            )
        )
        # TODO it's possible that a job will run more than once on the same container,
        # although not in parallel, in that case the job log will be overwritten
        _COMMAND_TEMPLATE = (
            "PYTHONUNBUFFERED=1 MEADOWRUN_WORKER_INDEX={worker_index} python -m "
            f"meadowrun.run_job_local_storage_main --job-id {job_id} "
            f"{storage_spec_args} >/var/meadowrun/job_logs/{job_id}.log 2>&1 "
            f"& echo $$ $!"
        )
        # echo $$ gets the process id of the current bash session (we will run
        # inner_command in a bash session). We assume that all processes started in this
        # session will get this id as their process group id (it's not clear if this
        # assumption is bulletproof) so that we can kill them all quickly. (100%
        # reliable would be to use the actual pid of run_job_local_storage_main returned
        # by $! and then explicitly ask for its group id and kill all processes in that
        # group)

        pids_tasks = []
        last_pod = None

        worker_index = 0
        async for pods in _get_meadowrun_reusable_pods(
            kubernetes_namespace,
            image_name,
            image_pull_secret_name,
            ports,
            resources,
            num_executions,
            core_api,
            batch_api,
            storage_spec,
            pod_customization,
        ):

            for pod in pods:
                command = [
                    "/bin/bash",
                    "-c",
                    _COMMAND_TEMPLATE.format(worker_index=worker_index),
                ]

                pids_task = asyncio.create_task(
                    run_command_on_pod(
                        pod.metadata.name, kubernetes_namespace, command, ws_core_api
                    )
                )

                # pids_task will return "{process group id} {process id}"
                pids_tasks.append(pids_task)
                self.all_processes.append(
                    ReusablePodRemoteProcess(
                        core_api,
                        ws_core_api,
                        kubernetes_namespace,
                        pod,
                        pids_task,
                    )
                )
                worker_index += 1
                last_pod = pod

        # we silently transform WAIT_AND_TAIL_STDOUT to WAIT_SILENTLY if there are more
        # than one processes
        if (
            wait_for_result != WaitOption.WAIT_AND_TAIL_STDOUT
            or len(pids_tasks) > 1
            or last_pod is None
        ):
            return asyncio.create_task(_gather_return_none(pids_tasks))
        else:
            pids = await pids_tasks[0]
            pid = pids.split(" ")[1].strip()
            return asyncio.create_task(
                run_command_on_pod_and_stream(
                    last_pod.metadata.name,
                    kubernetes_namespace,
                    [
                        "tail",
                        "--pid",
                        pid,
                        "--retry",
                        "-F",
                        f"/var/meadowrun/job_logs/{job_id}.log",
                    ],
                    ws_core_api,
                )
            )

    def received_result(self, worker_index: int) -> None:
        self.deallocation_tasks.append(
            asyncio.create_task((self.all_processes[worker_index]).received_result())
        )

    def kill_all(self) -> Awaitable:
        return asyncio.gather(
            *(process.kill() for process in self.all_processes),
            # executions where we've gotten the result already don't need to be killed
            # (because the fact that we received a result means they're presumably dead)
            # but we need to make sure their deallocation tasks are awaited
            *self.deallocation_tasks,
            return_exceptions=True,
        )


class ReusablePodRemoteProcess:
    def __init__(
        self,
        core_api: kubernetes_client.CoreV1Api,
        ws_core_api: kubernetes_client.CoreV1Api,
        kubernetes_namespace: str,
        pod: kubernetes_client.V1Pod,
        pids_task: Awaitable[str],
    ):
        self._pod = pod
        self._kubernetes_namespace = kubernetes_namespace
        self.pids_task = pids_task
        self._core_api = core_api
        self._ws_core_api = ws_core_api

        self._has_exited = False

    def _deallocate(self) -> Coroutine[Any, Any, None]:
        # now mark those pods as ready--this is an optimization--the automated readiness
        # probe will mark the pods as ready eventually, but this is faster
        return set_main_container_ready(self._core_api, self._pod, True)

    async def received_result(self) -> None:
        # we got a result through some other means
        self._has_exited = True
        await self._deallocate()

    async def kill(self) -> None:
        if not self._has_exited:
            self._has_exited = True
            pids = await self.pids_task
            process_group_id = pids.split(" ")[0].strip()
            await run_command_on_pod(
                self._pod.metadata.name,
                self._kubernetes_namespace,
                ["kill", "-9", "--", f"-{process_group_id}"],
                self._ws_core_api,
            )
            await self._deallocate()


async def _get_meadowrun_reusable_pods(
    kubernetes_namespace: str,
    image_name: str,
    image_pull_secret_name: Optional[str],
    ports: List[int],
    resources: Optional[ResourcesInternal],
    number_of_pods: int,
    core_api: kubernetes_client.CoreV1Api,
    batch_api: kubernetes_client.BatchV1Api,
    storage_spec: Optional[StorageBucketSpec],
    pod_customization: Optional[
        Callable[
            [kubernetes_client.V1PodTemplateSpec],
            kubernetes_client.V1PodTemplateSpec,
        ]
    ],
) -> AsyncIterable[List[kubernetes_client.V1Pod]]:

    # some potential confusion between different images, would it be better to hash
    # instead?
    image_name_label = image_name.replace("/", ".").replace(":", ".")

    pods_response = await core_api.list_namespaced_pod(
        kubernetes_namespace,
        # it would be nice to put ports and resources in the labels, but we need some
        # sort of string serialization format for that. For now we just filter them out
        # on the client side
        label_selector=f"meadowrun.io/image-name={image_name_label}",
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
        if (
            get_main_container_is_ready(pod)
            and _pod_meets_requirements(pod, ports, resources)
            and pod.metadata.deletion_timestamp is None
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
        print(
            f"Reusing {len(existing_pods)} existing pods: "
            + ", ".join(pod.metadata.name for pod in existing_pods)
        )
        yield existing_pods

    remaining_pods_to_launch = number_of_pods - len(existing_pods)

    if remaining_pods_to_launch > 0:
        environment: List[kubernetes_client.V1EnvVar] = []

        _add_meadowrun_variables_to_environment(environment, {}, storage_spec)

        if image_pull_secret_name:
            additional_pod_spec_parameters = {
                "image_pull_secrets": [
                    kubernetes_client.V1LocalObjectReference(image_pull_secret_name)
                ]
            }
        else:
            additional_pod_spec_parameters = {}

        job_name = f"mdr-reusable-{b32_encoded_uuid().replace('=', '').lower()}"
        pod_template_spec = kubernetes_client.V1PodTemplateSpec(
            metadata=kubernetes_client.V1ObjectMeta(
                labels={
                    "meadowrun.io/image-name": image_name_label,
                    "meadowrun.io": "true",
                }
            ),
            spec=kubernetes_client.V1PodSpec(
                containers=[
                    kubernetes_client.V1Container(
                        name="main",
                        image=image_name,
                        args=["python", "-m", "meadowrun.k8s_integration.k8s_main"],
                        env=environment,
                        readiness_probe=kubernetes_client.V1Probe(
                            _exec=kubernetes_client.V1ExecAction(
                                command=IS_JOB_RUNNING_COMMAND
                            ),
                            initial_delay_seconds=15,
                            # TODO this "should" be longer like 15s or so, because jobs
                            # that exit cleanly will manually update the status
                            # appropriately. However, there is weird behavior where the
                            # manually updated status will take effect for a few seconds
                            # but then flip back to whatever it was before for no
                            # apparent reason. There's very little documentation on the
                            # patch_namespaced_pod_status call which is what we're using
                            # to manually set the status, so it's hard to know if this
                            # is the correct behavior on the part of Kubernetes or not.
                            # As a result, we want to run the readiness probe as often
                            # as possible.
                            period_seconds=1,
                            timeout_seconds=1,
                            failure_threshold=1,
                            success_threshold=1,
                        ),
                        **additional_container_parameters,
                    )
                ],
                restart_policy="Never",
                **additional_pod_spec_parameters,
            ),
        )
        if pod_customization:
            pod_template_spec = pod_customization(pod_template_spec)
        body = kubernetes_client.V1Job(
            metadata=kubernetes_client.V1ObjectMeta(
                name=job_name, labels={"meadowrun.io": "true"}
            ),
            spec=kubernetes_client.V1JobSpec(
                ttl_seconds_after_finished=10,
                backoff_limit=0,
                template=pod_template_spec,
                completion_mode="Indexed",
                completions=remaining_pods_to_launch,
                parallelism=remaining_pods_to_launch,
            ),
        )

        await batch_api.create_namespaced_job(kubernetes_namespace, body)

        try:
            pods = await get_pods_for_job(
                core_api,
                kubernetes_namespace,
                job_name,
                [f"{job_name}-{i}-" for i in range(remaining_pods_to_launch)],
            )
        except BaseException:
            # usually the ttl_seconds_after_finished will take care of deleting the job,
            # but there's a case where the pods can't be created (e.g. if you provide an
            # invalid serviceAccountName), and the job will stick around forever
            try:
                await batch_api.delete_namespaced_job(job_name, kubernetes_namespace)
            except kubernetes_client_exceptions.ApiException:
                print("Warning, error cleaning up job:\n" + traceback.format_exc())
            raise

        async for pod_results in wait_for_pods_running(
            core_api, job_name, kubernetes_namespace, pods
        ):
            yield pod_results

        print(f"Started {remaining_pods_to_launch} new pods")


async def _gather_return_none(tasks: List[Task]) -> None:
    """A workaround because you can't do asyncio.create_task(asyncio.gather(*tasks))"""
    # consider throwing all exceptions not just the first one we get
    await asyncio.gather(*tasks, return_exceptions=False)


STORAGE_TYPE = "--storage-type"
