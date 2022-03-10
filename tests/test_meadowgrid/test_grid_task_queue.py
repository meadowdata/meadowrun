import asyncio
import threading
import uuid

from meadowgrid.aws_integration import _get_default_region_name
from meadowgrid.grid_task_queue import (
    _add_tasks,
    _complete_task,
    _create_queues_for_job,
    _get_task,
    get_results,
    worker_loop,
)
from meadowgrid.meadowgrid_pb2 import ProcessState
from meadowgrid.runner import run_map, EC2AllocHosts
from test_meadowgrid.test_ec2_alloc import _PRIVATE_KEY_FILENAME


def manual_test_grid_task_queue():
    """
    Tests the grid_task_queue functions without actually running any tasks. Uses SQS
    resources.
    """
    region_name = asyncio.run(_get_default_region_name())
    task_arguments = ["hello", ("hey", "there"), {"a": 1}]

    # dummy variables
    job_id = str(uuid.uuid4())
    public_address = "foo"
    worker_id = 1

    request_queue_url, result_queue_url = asyncio.run(
        _create_queues_for_job(job_id, region_name)
    )

    # get results in a different thread as we're adding/completing tasks
    results = None

    def get_results_thread():
        nonlocal results
        results = asyncio.run(
            get_results(result_queue_url, region_name, len(task_arguments), 1)
        )

    results_thread = threading.Thread(target=get_results_thread)
    results_thread.start()

    # add some tasks
    asyncio.run(_add_tasks(request_queue_url, region_name, task_arguments))

    # get some tasks and complete them
    task1 = _get_task(
        request_queue_url, result_queue_url, region_name, 0, public_address, worker_id
    )
    assert task1 is not None
    task2 = _get_task(
        request_queue_url, result_queue_url, region_name, 0, public_address, worker_id
    )
    assert task2 is not None
    _complete_task(
        result_queue_url,
        region_name,
        task1,
        ProcessState(
            state=ProcessState.ProcessStateEnum.SUCCEEDED,
            pickled_result=task1.pickled_function_arguments,
        ),
        public_address,
        worker_id,
    )
    task3 = _get_task(
        request_queue_url, result_queue_url, region_name, 0, public_address, worker_id
    )
    assert task3 is not None
    # there should be no more tasks to get
    assert (
        _get_task(
            request_queue_url,
            result_queue_url,
            region_name,
            0,
            public_address,
            worker_id,
        )
        is None
    )
    _complete_task(
        result_queue_url,
        region_name,
        task2,
        ProcessState(
            state=ProcessState.ProcessStateEnum.SUCCEEDED,
            pickled_result=task2.pickled_function_arguments,
        ),
        public_address,
        worker_id,
    )
    _complete_task(
        result_queue_url,
        region_name,
        task3,
        ProcessState(
            state=ProcessState.ProcessStateEnum.SUCCEEDED,
            pickled_result=task3.pickled_function_arguments,
        ),
        public_address,
        worker_id,
    )

    results_thread.join()
    assert results == task_arguments


def manual_test_worker():
    region_name = asyncio.run(_get_default_region_name())
    task_arguments = [1, 2, 3, 4]

    # dummy variables
    job_id = str(uuid.uuid4())
    public_address = "foo"
    worker_id = 1

    request_queue_url, result_queue_url = asyncio.run(
        _create_queues_for_job(job_id, region_name)
    )

    # get results on another thread
    results = None

    def get_results_thread():
        nonlocal results
        results = asyncio.run(
            get_results(result_queue_url, region_name, len(task_arguments), 1)
        )

    results_thread = threading.Thread(target=get_results_thread)
    results_thread.start()

    # add tasks
    asyncio.run(_add_tasks(request_queue_url, region_name, task_arguments))

    # start a worker_loop which will get tasks and complete them
    worker_thread = threading.Thread(
        target=lambda: worker_loop(
            lambda x: x**x,
            request_queue_url,
            result_queue_url,
            region_name,
            public_address,
            worker_id,
        )
    )
    worker_thread.start()

    results_thread.join()
    worker_thread.join()
    assert results == [1, 4, 27, 256]


async def manual_test_run_map():
    """Runs a "real" run_map"""
    results = await run_map(
        lambda x: x**x,
        [1, 2, 3, 4],
        EC2AllocHosts(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
    )

    assert results == [1, 4, 27, 256]
