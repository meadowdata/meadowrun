from __future__ import annotations

import abc
import asyncio
import collections
import dataclasses
import itertools
import pickle
import time
import traceback
import uuid
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
    TYPE_CHECKING,
)

import cloudpickle

from meadowrun.config import MEMORY_GB
from meadowrun.instance_allocation import InstanceRegistrar, allocate_jobs_to_instances
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.meadowrun_pb2 import QualifiedFunctionName, Job, PyAgentJob, ProcessState
from meadowrun.run_job_core import (
    Host,
    MeadowrunException,
    SshHost,
    TaskResult,
    get_log_path,
)
from meadowrun.storage_keys import construct_job_object_id

if TYPE_CHECKING:
    from types import TracebackType
    from meadowrun.run_job_core import (
        WaitOption,
        JobCompletion,
        TaskProcessState,
        CloudProviderType,
        WorkerProcessState,
    )


_T = TypeVar("_T")
_U = TypeVar("_U")


class AllocVM(Host, abc.ABC):
    """
    An abstract class that provides shared implementation for
    [AllocEC2Instance][meadowrun.AllocEC2Instance] and
    [AllocAzureVM][meadowrun.AllocAzureVM]
    """

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
        if resources_required_per_task is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocEC2Instance and AllocAzureVM"
            )
        if job_fields["ports"] and self.get_cloud_provider() == "AzureVM":
            raise NotImplementedError(
                "Opening ports on Azure is not implemented, please comment on "
                "https://github.com/meadowdata/meadowrun/issues/126"
            )

        base_job_id = str(uuid.uuid4())
        async with self._create_grid_job_worker_launcher(
            base_job_id, function, pickle_protocol, job_fields, wait_for_result
        ) as worker_launcher, self._create_grid_job_cloud_interface(
            base_job_id
        ) as cloud_interface:
            driver = GridJobDriver(
                cloud_interface,
                worker_launcher,
                num_concurrent_tasks,
                resources_required_per_task,
            )
            run_worker_loops = asyncio.create_task(driver.run_worker_functions())
            num_tasks_done = 0
            async for result in driver.add_tasks_and_get_results(
                args, max_num_task_attempts, retry_with_more_memory
            ):
                yield result
                num_tasks_done += 1

            await run_worker_loops

        # this is for extra safety--the only case where we don't get all of our results
        # back should be if run_worker_loops throws an exception because there were
        # worker failures
        if num_tasks_done < len(args):
            raise ValueError(
                "Gave up retrieving task results, most likely due to worker failures. "
                f"Received {num_tasks_done}/{len(args)} task results."
            )

    @abc.abstractmethod
    def _create_grid_job_cloud_interface(
        self, base_job_id: str
    ) -> GridJobCloudInterface:
        pass

    @abc.abstractmethod
    def _create_grid_job_worker_launcher(
        self,
        base_job_id: str,
        user_function: Callable[[_T], _U],
        pickle_protocol: int,
        job_fields: Dict[str, Any],
        wait_for_result: WaitOption,
    ) -> GridJobWorkerLauncher:
        pass

    @abc.abstractmethod
    def get_cloud_provider(self) -> CloudProviderType:
        pass

    @abc.abstractmethod
    def get_runtime_resources(self) -> ResourcesInternal:
        # "Runtime resources" are resources that aren't tied to a particular instance
        # type. Instance type resources are things like CPU and memory. Runtime
        # resources are things like an AMI id or subnet id. Runtime resources should NOT
        # be considered when choosing an instance type, but need to be considered when
        # deciding whether an existing instance can run a new job.
        pass


class GridJobCloudInterface(abc.ABC, Generic[_T, _U]):
    """
    See also GridJobDriver. The GridJobDriver is a concrete class that handles the
    cloud-independent logic for running a grid job, e.g. replacing workers that exit
    unexpectedly (not implemented yet), retrying vs giving up on tasks. The
    GridJobCloudInterface is an interface that we can implement for different cloud
    providers. The GridJobDriver always has a single GridJobCloudInterface
    implementation that it uses to actually "do things" in the real world like launch
    instances and start workers.
    """

    async def __aenter__(self) -> GridJobCloudInterface:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    @abc.abstractmethod
    def create_queue(self) -> int:
        ...

    @abc.abstractmethod
    async def setup_and_add_tasks(self, tasks: Sequence[_T]) -> None:
        """
        GridJobDriver will always call this exactly once before any other functions on
        this class are called.
        """
        ...

    @abc.abstractmethod
    async def get_agent_function(
        self, queue_index: int
    ) -> Tuple[QualifiedFunctionName, Sequence[Any]]:
        """
        Returns a function that will poll/wait for tasks, and communicate to task worker
        via the given streamreader and -writer. The returned function will also exit in
        response to shutdown_workers.
        """
        ...

    @abc.abstractmethod
    async def receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[List[TaskProcessState], List[WorkerProcessState]]]:
        ...

    @abc.abstractmethod
    async def retry_task(
        self, task_id: int, attempts_so_far: int, queue_index: int
    ) -> None:
        ...

    @abc.abstractmethod
    async def shutdown_workers(self, num_workers: int, queue_index: int) -> None:
        ...


class GridJobWorkerLauncher(abc.ABC):
    async def __aenter__(self) -> GridJobWorkerLauncher:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        pass

    @abc.abstractmethod
    async def launch_workers(
        self,
        agent_function_task: asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]],
        num_workers_to_launch: int,
        resources_required_per_task: ResourcesInternal,
        queue_index: int,
        abort_launching_new_workers: asyncio.Event,
    ) -> AsyncIterable[List[WorkerTask]]:
        # https://github.com/python/mypy/issues/5070
        if False:
            yield


class GridJobSshWorkerLauncher(GridJobWorkerLauncher):
    def __init__(
        self,
        alloc_vm: AllocVM,
        base_job_id: str,
        user_function: Callable[[_T], _U],
        pickle_protocol: int,
        job_fields: Dict[str, Any],
        wait_for_result: WaitOption,
    ) -> None:
        self._alloc_vm = alloc_vm
        self._base_job_id = base_job_id
        self._next_worker_suffix = 0
        self._user_function = user_function
        self._pickle_protocol = pickle_protocol
        self._job_fields = job_fields
        self._wait_for_result = wait_for_result

        self._instance_registrar = self.create_instance_registrar()

        self._address_to_ssh_host: Dict[str, SshHost] = {}

    async def __aenter__(self) -> GridJobSshWorkerLauncher:
        self._instance_registrar = await self._instance_registrar.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self._instance_registrar.__aexit__(exc_type, exc_val, exc_tb)

        await asyncio.gather(
            *[
                ssh_host.close_connection()
                for ssh_host in self._address_to_ssh_host.values()
            ],
            return_exceptions=True,
        )

    @abc.abstractmethod
    def create_instance_registrar(self) -> InstanceRegistrar:
        ...

    @abc.abstractmethod
    async def ssh_host_from_address(self, address: str, instance_name: str) -> SshHost:
        ...

    async def launch_worker(
        self,
        ssh_host: SshHost,
        worker_job_id: str,
        agent_function_task: asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]],
    ) -> JobCompletion:

        (
            qualified_agent_function_name,
            agent_function_arguments,
        ) = await agent_function_task

        job = Job(
            base_job_id=self._base_job_id,
            py_agent=PyAgentJob(
                pickled_function=cloudpickle.dumps(
                    self._user_function, protocol=self._pickle_protocol
                ),
                qualified_agent_function_name=qualified_agent_function_name,
                pickled_agent_function_arguments=pickle.dumps(
                    (agent_function_arguments, {}),
                    protocol=self._pickle_protocol,
                ),
            ),
            **self._job_fields,
        )

        async def deallocator() -> None:
            if ssh_host.instance_name is not None:
                await self._instance_registrar.deallocate_job_from_instance(
                    await self._instance_registrar.get_registered_instance(
                        ssh_host.instance_name
                    ),
                    worker_job_id,
                )

        return await ssh_host.run_cloud_job(
            job, worker_job_id, self._wait_for_result, deallocator
        )

    async def launch_workers(
        self,
        agent_function_task: asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]],
        num_workers_to_launch: int,
        resources_required_per_task: ResourcesInternal,
        queue_index: int,
        abort_launching_new_workers: asyncio.Event,
    ) -> AsyncIterable[List[WorkerTask]]:
        async for allocated_hosts in allocate_jobs_to_instances(
            self._instance_registrar,
            resources_required_per_task,
            self._base_job_id,
            self._next_worker_suffix,
            num_workers_to_launch,
            self._alloc_vm,
            self._job_fields["ports"],
            abort_launching_new_workers,
        ):
            worker_tasks = []
            for ((public_address, instance_name), job_ids) in allocated_hosts.items():
                ssh_host = self._address_to_ssh_host.get(public_address)
                if ssh_host is None:
                    ssh_host = await self.ssh_host_from_address(
                        public_address, instance_name
                    )
                    self._address_to_ssh_host[public_address] = ssh_host
                for job_id in job_ids:
                    worker_tasks.append(
                        WorkerTask(
                            f"{public_address} {get_log_path(job_id)}",
                            queue_index,
                            asyncio.create_task(
                                self.launch_worker(
                                    ssh_host, job_id, agent_function_task
                                )
                            ),
                            # the worker_ids concept is for when we launch workers via
                            # queues and need to correlate WorkerProcessStates with the
                            # WorkerQueue that the worker was for. In the case of
                            # SshWorkerLauncher, we won't get WorkerProcessStates
                            [""],
                        )
                    )
            yield worker_tasks
        self._next_worker_suffix += num_workers_to_launch


class GridJobQueueWorkerLauncher(GridJobWorkerLauncher):
    def __init__(
        self,
        alloc_vm: AllocVM,
        base_job_id: str,
        user_function: Callable[[_T], _U],
        pickle_protocol: int,
        job_fields: Dict[str, Any],
        wait_for_result: WaitOption,
    ) -> None:
        self._alloc_vm = alloc_vm
        self._base_job_id = base_job_id
        self._next_worker_suffix = 0
        self._user_function = user_function
        self._pickle_protocol = pickle_protocol
        self._job_fields = job_fields
        self._wait_for_result = wait_for_result

        self._instance_registrar = self.create_instance_registrar()

        self._job_object_uploads: Dict[str, asyncio.Task] = {}

    async def __aenter__(self) -> GridJobQueueWorkerLauncher:
        self._instance_registrar = await self._instance_registrar.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self._instance_registrar.__aexit__(exc_type, exc_val, exc_tb)

        for job_object_id, upload_task in self._job_object_uploads.items():
            upload_task.cancel()

    @abc.abstractmethod
    def create_instance_registrar(self) -> InstanceRegistrar:
        ...

    @abc.abstractmethod
    async def launch_jobs(
        self,
        public_address: str,
        instance_name: str,
        job_object_id: str,
        job_ids: List[str],
    ) -> None:
        ...

    @abc.abstractmethod
    async def kill_jobs(self, instance_name: str, job_ids: List[str]) -> None:
        ...

    @abc.abstractmethod
    async def _upload_job_object(self, job_object_id: str, job: Job) -> None:
        """
        This is a bit janky, but GridJobCloudInterface will take care of deleting the
        job objects
        """
        ...

    async def _upload_job_object_wrapper(
        self,
        agent_function_task: asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]],
        job_object_id: str,
    ) -> None:
        (
            qualified_agent_function_name,
            agent_function_arguments,
        ) = await agent_function_task

        job = Job(
            base_job_id=self._base_job_id,
            py_agent=PyAgentJob(
                pickled_function=cloudpickle.dumps(
                    self._user_function, protocol=self._pickle_protocol
                ),
                qualified_agent_function_name=qualified_agent_function_name,
                pickled_agent_function_arguments=pickle.dumps(
                    (agent_function_arguments, {}),
                    protocol=self._pickle_protocol,
                ),
            ),
            **self._job_fields,
        )

        await self._upload_job_object(job_object_id, job)

    async def _on_cancel_kill_jobs(
        self, instance_name: str, job_ids: List[str]
    ) -> None:
        """
        This will asyncio-sleep forever until it is cancelled and then kill the
        specified jobs
        """
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await self.kill_jobs(instance_name, job_ids)

    async def launch_workers(
        self,
        agent_function_task: asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]],
        num_workers_to_launch: int,
        resources_required_per_task: ResourcesInternal,
        queue_index: int,
        abort_launching_new_workers: asyncio.Event,
    ) -> AsyncIterable[List[WorkerTask]]:
        async for allocated_hosts in allocate_jobs_to_instances(
            self._instance_registrar,
            resources_required_per_task,
            self._base_job_id,
            self._next_worker_suffix,
            num_workers_to_launch,
            self._alloc_vm,
            self._job_fields["ports"],
            abort_launching_new_workers,
        ):
            worker_tasks = []
            for (
                (public_address, instance_name),
                worker_job_ids,
            ) in allocated_hosts.items():
                job_object_id = construct_job_object_id(self._base_job_id, queue_index)
                if job_object_id not in self._job_object_uploads:
                    self._job_object_uploads[job_object_id] = asyncio.create_task(
                        self._upload_job_object_wrapper(
                            agent_function_task, job_object_id
                        )
                    )
                await self._job_object_uploads[job_object_id]

                await self.launch_jobs(
                    public_address, instance_name, job_object_id, worker_job_ids
                )
                worker_tasks.append(
                    WorkerTask(
                        f"Worker placeholder: {instance_name}, {worker_job_ids}",
                        queue_index,
                        asyncio.create_task(
                            self._on_cancel_kill_jobs(instance_name, worker_job_ids)
                        ),
                        worker_job_ids,
                    )
                )

            yield worker_tasks
        self._next_worker_suffix += num_workers_to_launch


_PRINT_RECEIVED_TASKS_SECONDS = 10


@dataclasses.dataclass
class WorkerQueue:
    """
    Keeps track of how many workers we've started/shutdown for each queue. The 0th queue
    is the "normal" one, and subsequent queues are for retrying tasks with more
    resources
    """

    queue_index: int
    num_workers_needed: int
    num_workers_launched: int = 0
    num_worker_shutdown_messages_sent: int = 0
    num_workers_exited_unexpectedly: int = 0
    get_agent_function_task: Optional[
        asyncio.Task[Tuple[QualifiedFunctionName, Sequence[Any]]]
    ] = None


@dataclasses.dataclass(frozen=True)
class WorkerTask:
    """
    This class is currently a bit messy. For SSH-launched workers, this represents a
    single worker process. task will complete when the worker completes, and will raise
    an exception if there's a problem with the worker, and the task can be cancelled to
    kill the worker. In this case, worker_ids will be meaningless.

    For queue-launched workers, this represents one or more workers, whose job_ids will
    be in worker_ids. The task will never complete or raise, but can be cancelled to
    kill the worker. log_file_info will be meaningless.
    """

    log_file_info: str
    queue_index: int
    task: asyncio.Task
    worker_ids: List[str]


def _memory_gb_for_queue_index(
    queue_index: int, base_resources: ResourcesInternal
) -> float:
    """See WorkerQueue"""
    return base_resources.consumable[MEMORY_GB] * (1 + queue_index)


def _resources_for_queue_index(
    queue_index: int, base_resources: ResourcesInternal
) -> ResourcesInternal:
    """See WorkerQueue"""
    if queue_index == 0:
        return base_resources
    else:
        return base_resources.add(
            ResourcesInternal.from_cpu_and_memory(
                0,
                base_resources.consumable[MEMORY_GB] * queue_index,
            )
        )


class GridJobDriver:
    """
    See GridJobCloudInterface. This class handles the cloud-independent logic for
    running a grid job, e.g. replacing workers that exit unexpectedly (not implemented
    yet), retrying vs giving up on tasks.

    The basic design is that there are two "loops" run_worker_functions and
    add_tasks_and_get_results that run "in parallel" via asyncio. They interact using
    shared asyncio.Event objects.
    """

    def __init__(
        self,
        cloud_interface: GridJobCloudInterface,
        worker_launcher: GridJobWorkerLauncher,
        num_concurrent_tasks: int,
        resources_required_per_task: ResourcesInternal,
    ):
        """This constructor must be called on an EventLoop"""
        self._cloud_interface = cloud_interface
        self._worker_launcher = worker_launcher
        self._resources_required_per_task = resources_required_per_task

        # run_worker_functions will set this to indicate to add_tasks_and_get_results
        # that there all of our workers have either exited unexpectedly (and we have
        # given up trying to restore them), or have been told to shutdown normally
        self._no_workers_available = asyncio.Event()

        self._worker_queues = [WorkerQueue(0, num_concurrent_tasks)]
        self._num_workers_needed_changed = asyncio.Event()

        self._abort_launching_new_workers = asyncio.Event()

        self._worker_process_states: List[List[WorkerProcessState]] = []
        self._worker_process_state_received = asyncio.Event()

    async def run_worker_functions(self) -> None:
        """
        Allocates cloud instances, runs a worker function on them, sends worker shutdown
        messages when requested by add_tasks_and_get_results, and generally manages
        workers (e.g. replacing workers when they exit unexpectedly, not implemented
        yet).
        """

        worker_tasks: List[WorkerTask] = []
        worker_id_to_queue_index: Dict[str, int] = {}

        workers_needed_changed_wait_task = asyncio.create_task(
            self._num_workers_needed_changed.wait()
        )
        worker_process_state_received_task = asyncio.create_task(
            self._worker_process_state_received.wait()
        )
        pickled_worker_function_task: Optional[asyncio.Task[bytes]] = None
        async_cancel_exception = False

        try:
            # we create an asyncio.task for this because it requires waiting for
            # _cloud_interface.setup_and_add_tasks to complete. We don't want to just
            # run this sequentially, though, because we want to start launching
            # instances before setup_and_add_tasks is complete.
            # initially, we just want the 0th queue
            self._worker_queues[0].get_agent_function_task = asyncio.create_task(
                self._cloud_interface.get_agent_function(0)
            )

            while True:
                for worker_queue in self._worker_queues:
                    # TODO we should subtract workers_exited_unexpectedly from
                    # workers_launched, this would mean we replace workers that exited
                    # unexpectedly. Implementing this properly means we should add more
                    # code that will tell us why workers exited unexpectedly (e.g.
                    # segfault in user code, spot instance eviction, vs an issue
                    # creating the environment). The main concern is ending up in an
                    # infinite loop where we're constantly launching workers that fail.
                    new_workers_to_launch = worker_queue.num_workers_needed - (
                        worker_queue.num_workers_launched
                        - worker_queue.num_worker_shutdown_messages_sent
                    )

                    # launch new workers if they're needed
                    if new_workers_to_launch > 0:
                        if worker_queue.get_agent_function_task is None:
                            worker_queue.get_agent_function_task = asyncio.create_task(
                                self._cloud_interface.get_agent_function(
                                    worker_queue.queue_index
                                )
                            )

                        async for new_worker_tasks in self._worker_launcher.launch_workers(  # noqa: E501
                            worker_queue.get_agent_function_task,
                            new_workers_to_launch,
                            _resources_for_queue_index(
                                worker_queue.queue_index,
                                self._resources_required_per_task,
                            ),
                            worker_queue.queue_index,
                            self._abort_launching_new_workers,
                        ):
                            for worker_task in new_worker_tasks:
                                for worker_id in worker_task.worker_ids:
                                    worker_id_to_queue_index[
                                        worker_id
                                    ] = worker_queue.queue_index
                            worker_queue.num_workers_launched += sum(
                                len(worker_task.worker_ids)
                                for worker_task in new_worker_tasks
                            )
                            worker_tasks.extend(new_worker_tasks)

                    # shutdown workers if they're no longer needed
                    # once we have the lost worker replacement logic this should
                    # just be -workers_to_launch
                    workers_to_shutdown = (
                        worker_queue.num_workers_launched
                        - worker_queue.num_worker_shutdown_messages_sent
                        - worker_queue.num_workers_exited_unexpectedly
                    ) - (worker_queue.num_workers_needed)
                    if workers_to_shutdown > 0:
                        await self._cloud_interface.shutdown_workers(
                            workers_to_shutdown, worker_queue.queue_index
                        )
                        worker_queue.num_worker_shutdown_messages_sent += (
                            workers_to_shutdown
                        )

                # this means all workers are either done or shutting down
                if all(
                    worker_queue.num_workers_launched
                    - worker_queue.num_worker_shutdown_messages_sent
                    - worker_queue.num_workers_exited_unexpectedly
                    <= 0
                    for worker_queue in self._worker_queues
                ):
                    break

                # now we wait until either add_tasks_and_get_results tells us to
                # shutdown some workers, or workers exit
                await asyncio.wait(
                    itertools.chain(
                        cast(
                            Iterable[asyncio.Task],
                            (task.task for task in worker_tasks),
                        ),
                        (
                            workers_needed_changed_wait_task,
                            worker_process_state_received_task,
                        ),
                    ),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                new_worker_tasks = []
                for worker_task in worker_tasks:
                    if worker_task.task.done():
                        exception = worker_task.task.exception()
                        if exception is not None:
                            print(
                                # TODO also include queue index here?
                                f"Error running worker {worker_task.log_file_info}:"
                                f"\n"
                                + "".join(
                                    traceback.format_exception(
                                        type(exception),
                                        exception,
                                        exception.__traceback__,
                                    )
                                )
                            )
                            self._worker_queues[
                                worker_task.queue_index
                            ].num_workers_exited_unexpectedly += 1
                            # TODO ideally we would tell the receive_results loop to
                            # reschedule whatever task the worker was working on
                        #  TODO do something with worker_task.result()
                    else:
                        new_worker_tasks.append(worker_task)
                worker_tasks = new_worker_tasks

                if worker_process_state_received_task.done():
                    self._worker_process_state_received.clear()
                    worker_process_state_received_task = asyncio.create_task(
                        self._worker_process_state_received.wait()
                    )
                    while self._worker_process_states:
                        for worker_process_state in self._worker_process_states.pop():
                            queue_index = worker_id_to_queue_index.pop(
                                worker_process_state.worker_index, None
                            )
                            if queue_index is not None:
                                # This doesn't check to make sure that we're not
                                # double-counting a worker whose task raised an
                                # exception. It must never be the case that
                                # worker_task.task raises an exception AND a
                                # worker_process_state is received
                                self._worker_queues[
                                    queue_index
                                ].num_workers_exited_unexpectedly += 1

                            if (
                                worker_process_state.result.state
                                != ProcessState.ProcessStateEnum.SUCCEEDED
                            ):
                                print(
                                    "Error running worker: "
                                    f"{MeadowrunException(worker_process_state.result)}"
                                )

                if workers_needed_changed_wait_task.done():
                    self._num_workers_needed_changed.clear()
                    workers_needed_changed_wait_task = asyncio.create_task(
                        self._num_workers_needed_changed.wait()
                    )

        except asyncio.CancelledError:
            # if we're being cancelled, then most likely the worker_tasks are being
            # cancelled as well (because someone pressed Ctrl+C), which means it's
            # unhelpful to cancel them again, we want them to finish running their
            # except/finally clauses
            async_cancel_exception = True
            raise
        finally:
            print("Shutting down workers")
            # setting this is very critical--otherwise, add_tasks_and_get_results will
            # hang forever, not knowing that it has no hope of workers working on any of
            # its tasks
            self._no_workers_available.set()

            if not async_cancel_exception:
                # cancel any outstanding workers. Even if there haven't been any
                # exception and we've sent worker shutdown messages for all the workers,
                # a worker might be in the middle of building/pulling an environment and
                # we don't want to wait for that to complete
                for worker_task in worker_tasks:
                    worker_task.task.cancel()

            # we still want the tasks to complete their except/finally blocks, after
            # which they should reraise asyncio.CancelledError, which we can safely
            # ignore
            await asyncio.gather(
                *(task.task for task in worker_tasks), return_exceptions=True
            )

            workers_needed_changed_wait_task.cancel()
            if pickled_worker_function_task is not None:
                pickled_worker_function_task.cancel()

    async def add_tasks_and_get_results(
        self,
        args: Sequence[_T],
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult]:
        """
        Adds the specified tasks to the "queue", and retries tasks as needed. Yields
        TaskResult objects as soon as tasks complete.
        """

        # this keeps track of which queue each arg is assigned to. Initially they are
        # all assigned to the 0th queue, and if they fail because of suspected lack of
        # memory and retry_with_more_memory is set, then we will increase their queue
        # index. -1 indicates that the arg is done so it is no longer assigned to any
        # queue
        arg_to_queue_index = [0] * len(args)
        await self._cloud_interface.setup_and_add_tasks(args)

        # done = successful or exhausted retries
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
        async for task_batch, worker_batch in await self._cloud_interface.receive_task_results(  # noqa: E501
            stop_receiving=stop_receiving, workers_done=self._no_workers_available
        ):
            if worker_batch:
                self._worker_process_states.append(worker_batch)
                self._worker_process_state_received.set()

            for task in task_batch:
                task_result = TaskResult.from_process_state(task)
                if task_result.is_success:
                    num_tasks_done += 1
                    arg_to_queue_index[task.task_id] = -1
                    yield task_result
                elif (
                    task.attempt < max_num_task_attempts
                    and task_result.state != "RESULT_CANNOT_BE_UNPICKLED"
                ):
                    prev_queue_index = arg_to_queue_index[task.task_id]
                    prev_memory_requirement = _memory_gb_for_queue_index(
                        prev_queue_index, self._resources_required_per_task
                    )
                    if (
                        retry_with_more_memory
                        and task.result.max_memory_used_gb
                        >= 0.95 * prev_memory_requirement
                    ):
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, "
                            "retrying with more memory (task used "
                            f"{task.result.max_memory_used_gb:.2f}/"
                            f"{prev_memory_requirement}GB requested)"
                        )

                        new_queue_index = arg_to_queue_index[task.task_id] + 1
                        arg_to_queue_index[task.task_id] = new_queue_index
                        if len(self._worker_queues) < new_queue_index + 1:
                            # TODO any new queue gets 1 worker by default. We should
                            # increase this later depending on how the situation changes
                            self._cloud_interface.create_queue()
                            self._worker_queues.append(WorkerQueue(new_queue_index, 1))
                        await self._cloud_interface.retry_task(
                            task.task_id, task.attempt, new_queue_index
                        )
                    else:
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, "
                            "retrying"
                        )
                        await self._cloud_interface.retry_task(
                            task.task_id, task.attempt, prev_queue_index
                        )
                else:
                    if (
                        task.attempt < max_num_task_attempts
                        and task_result.state == "RESULT_CANNOT_BE_UNPICKLED"
                    ):
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, max"
                            f" attempts is {max_num_task_attempts}, but not retrying "
                            "because the failure happened when trying to unpickle the "
                            "result on the client."
                        )
                    else:
                        print(
                            f"Task {task.task_id} failed at attempt {task.attempt}, max"
                            f" attempts is {max_num_task_attempts}, not retrying."
                        )
                    num_tasks_done += 1
                    arg_to_queue_index[task.task_id] = -1
                    yield task_result

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
            num_workers_needed_per_queue = collections.Counter(arg_to_queue_index)
            for worker_queue in self._worker_queues:
                # num_workers_needed = min(outstanding tasks for this queue, current num
                # workers needed for this queue)
                # TODO at some point we might want to increase the number of workers
                # needed?
                num_workers_needed = min(
                    num_workers_needed_per_queue.get(worker_queue.queue_index, 0),
                    worker_queue.num_workers_needed,
                )
                if num_workers_needed < worker_queue.num_workers_needed:
                    worker_queue.num_workers_needed = num_workers_needed
                    self._num_workers_needed_changed.set()

        # We could be more finegrained about aborting launching workers. This is the
        # easiest to implement, but ideally every time num_workers_needed changes we
        # would consider cancelling launching new workers
        self._abort_launching_new_workers.set()

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
