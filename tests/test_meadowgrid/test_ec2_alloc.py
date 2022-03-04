import asyncio
import os
import platform
import time
from typing import Tuple

import boto3
import pytest

from meadowgrid.aws_integration import _get_default_region_name
from meadowgrid.ec2_alloc import (
    _EC2InstanceState,
    _EC2_ALLOC_LAMBDA_NAME,
    _allocate_job_to_ec2_instance,
    _choose_existing_ec2_instances,
    _ensure_ec2_alloc_table,
    _get_ec2_instances,
    _register_ec2_instance,
    allocate_ec2_instances,
    deallocate_job_from_ec2_instance,
    ensure_ec2_alloc_lambda,
    get_jobs_on_ec2_instance,
)
from meadowgrid.ec2_alloc_lambda.adjust_ec2_instances import (
    _deregister_ec2_instance,
    adjust,
)
from meadowgrid.resource_allocation import Resources
from meadowgrid.runner import run_function, EC2AllocHost


async def _clear_ec2_instances_table() -> None:
    # pretend that we're running in a lambda for _deregister_ec2_instance
    os.environ["AWS_REGION"] = await _get_default_region_name()

    table = await _ensure_ec2_alloc_table()
    instances = _get_ec2_instances(table)
    for instance in instances:
        _deregister_ec2_instance(instance.public_address, False)


async def manual_test_allocate_deallocate_mechanics():
    """Tests allocating and deallocating, does not involve real machines"""
    await _clear_ec2_instances_table()

    await _register_ec2_instance("testhost-1", 8, 64, [])
    await _register_ec2_instance(
        "testhost-2", 4, 32, [("worker-1", Resources(1, 2, {}))]
    )

    # Can't create the same table twice
    with pytest.raises(ValueError):
        await _register_ec2_instance("testhost-2", 4, 32, [])

    worker1_job = (await get_jobs_on_ec2_instance("testhost-2"))["worker-1"]
    assert await deallocate_job_from_ec2_instance("testhost-2", "worker-1", worker1_job)
    # Can't deallocate the same worker twice
    assert not await deallocate_job_from_ec2_instance(
        "testhost-2", "worker-1", worker1_job
    )

    table = await _ensure_ec2_alloc_table()

    def get_instances() -> Tuple[_EC2InstanceState, _EC2InstanceState]:
        instances = _get_ec2_instances(table)
        return [i for i in instances if i.public_address == "testhost-1"][0], [
            i for i in instances if i.public_address == "testhost-2"
        ][0]

    testhost1, testhost2 = get_instances()
    assert testhost2.available_resources.logical_cpu == 6
    assert testhost2.available_resources.memory_gb == 33

    assert _allocate_job_to_ec2_instance(
        table, testhost1.public_address, Resources(4, 2, {}), ["worker-2", "worker-3"]
    )
    assert _allocate_job_to_ec2_instance(
        table, testhost1.public_address, Resources(3, 1, {}), ["worker-4"]
    )
    # cannot allocate if the worker id already is in use
    assert not _allocate_job_to_ec2_instance(
        table, testhost1.public_address, Resources(4, 2, {}), ["worker-2"]
    )

    # make sure our resources available is correct
    testhost1, _ = get_instances()
    assert testhost1.available_resources.logical_cpu == 3
    assert testhost1.available_resources.memory_gb == 53

    # we've kind of already tested deallocation, but just for good measure
    worker4_job = (await get_jobs_on_ec2_instance("testhost-1"))["worker-4"]
    assert await deallocate_job_from_ec2_instance("testhost-1", "worker-4", worker4_job)
    testhost1, _ = get_instances()
    assert testhost1.available_resources.logical_cpu == 4
    assert testhost1.available_resources.memory_gb == 56


async def manual_test_allocate_existing_instances():
    """
    Tests logic for allocating existing EC2 instances, does not involve actual instances
    """
    await _clear_ec2_instances_table()

    await _register_ec2_instance("testhost-3", 2, 16, [])
    await _register_ec2_instance("testhost-4", 4, 32, [])

    resources_required = Resources(2, 1, {})
    results = await _choose_existing_ec2_instances(resources_required, 3)

    # we should put 2 tasks on testhost-3 because that's more "compact"
    assert len(results["testhost-3"]) == 2
    assert len(results["testhost-4"]) == 1


# set this to your private key if needed
_PRIVATE_KEY_FILENAME = None


async def manual_test_launch_one_instance():
    """Launches instances that must be cleaned up manually"""
    await _clear_ec2_instances_table()

    def remote_function():
        return os.getpid(), platform.node()

    pid1, host1 = await run_function(
        remote_function,
        EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
    )
    time.sleep(1)
    pid2, host2 = await run_function(
        remote_function,
        EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
    )
    time.sleep(1)
    pid3, host3 = await run_function(
        remote_function,
        EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
    )

    # these should have all run on the same host, but in different processes
    assert pid1 != pid2 and pid2 != pid3
    assert host1 == host2 and host2 == host3

    instances = _get_ec2_instances(await _ensure_ec2_alloc_table())
    assert len(instances) == 1
    assert instances[0].available_resources.logical_cpu >= 1
    assert instances[0].available_resources.memory_gb >= 0.5

    # remember to kill the instance when you're done!


async def manual_test_launch_multiple_instances():
    """Launches instances that must be cleaned up manually"""
    await _clear_ec2_instances_table()

    def remote_function():
        return os.getpid(), platform.node()

    task1 = asyncio.create_task(
        run_function(
            remote_function,
            EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
        )
    )
    task2 = asyncio.create_task(
        run_function(
            remote_function,
            EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
        )
    )
    task3 = asyncio.create_task(
        run_function(
            remote_function,
            EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
        )
    )

    results = await asyncio.gather(task1, task2, task3)
    ((pid1, host1), (pid2, host2), (pid3, host3)) = results

    # These should all have ended up on different hosts
    assert host1 != host2 and host2 != host3
    instances = _get_ec2_instances(await _ensure_ec2_alloc_table())
    assert len(instances) == 3
    assert all(
        instance.available_resources.logical_cpu >= 1
        and instance.available_resources.memory_gb >= 0.5
        for instance in instances
    )


async def very_manual_test_deallocate_after_running():
    """Requires running commands manually during the test to confirm correct behavior"""
    await _clear_ec2_instances_table()

    def remote_function():
        time.sleep(60 * 10)

    await run_function(
        remote_function,
        EC2AllocHost(1, 0.5, 15, private_key_filename=_PRIVATE_KEY_FILENAME),
    )

    # 1. now get the address of the EC2 instance and the job id and SSH into the remote
    # server and run:
    # /meadowgrid/env/bin/python /meadowgrid/env/lib/python3.9/site-packages/meadowgrid/deallocate_jobs.py --job-id JOB_ID  # noqa: E501
    # this should NOT deallocate the job, as it's still running
    # 2. now find the pid of the run_one_job process via `ps aux | grep run_one_job`,
    # and then kill that process using `kill -9 PID`. This way the regular
    # deallocate_jobs process won't be able to run
    # 3. now run
    # /meadowgrid/env/bin/python /meadowgrid/env/lib/python3.9/site-packages/meadowgrid/deallocate_jobs.py  # noqa: E501
    # without the --job-id argument. You should see the job being deallocated.


async def very_manual_test_deallocate_before_running():
    """Requires running commands manually during the test to confirm correct behavior"""
    # run this after another test when we still have an instance lying around

    result = await allocate_ec2_instances(
        Resources(0.5, 1, {}), 1, 100, await _get_default_region_name()
    )
    print(result)
    # 1. now using the address of the EC2 instance we allocated to, SSH to the machine
    # and run:
    # /meadowgrid/env/bin/python /meadowgrid/env/lib/python3.9/site-packages/meadowgrid/deallocate_jobs.py  # noqa: E501
    # this should NOT deallocate the job because the timeout has not elapsed yet
    # 2. Wait 7 minutes and then run the same command again, and the job should get
    # deallocated


async def manual_test_deregister():
    """Tests registering and deregistering, does not involve real machines"""
    await _clear_ec2_instances_table()

    await _register_ec2_instance("testhost-1", 8, 64, [])
    await _register_ec2_instance(
        "testhost-2", 4, 32, [("worker-1", Resources(1, 2, {}))]
    )

    assert _deregister_ec2_instance("testhost-1", True)
    # with require_no_running_jobs=True, testhost-2 should fail to deregister
    assert not _deregister_ec2_instance("testhost-2", True)
    assert _deregister_ec2_instance("testhost-2", False)


def _run_adjust(use_lambda: bool) -> None:
    if use_lambda:
        lambda_client = boto3.client("lambda")
        lambda_client.invoke(FunctionName=_EC2_ALLOC_LAMBDA_NAME)
    else:
        adjust()


async def very_manual_test_adjust_ec2_instances(use_lambda: bool):
    """
    Tests the adjust function, involves running real machines, but they should all get
    terminated automatically by the test.

    If use_lambda is false, the test will just call the adjust function directly. For a
    slightly more realistic test, you can run with use_lambda set to true. In that case,
    the test will invoke the ec2 alloc lambda, assuming it has already been created.
    """

    await _clear_ec2_instances_table()
    table = await _ensure_ec2_alloc_table()

    # first, register an instance that's not actually running
    await _register_ec2_instance(
        "testhost-1", 8, 64, [("worker-1", Resources(1, 2, {}))]
    )
    assert len(_get_ec2_instances(table)) == 1

    # adjust should deregister the instance (even if there are "jobs" supposedly running
    # on it) because there's no actual instance running
    _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 0

    # now, launch two instances (which should get registered automatically)
    instances1 = await allocate_ec2_instances(
        Resources(0.5, 1, {}), 1, 15, await _get_default_region_name()
    )
    assert len(instances1) == 1
    public_address1 = list(instances1.keys())[0]

    instances2 = await allocate_ec2_instances(
        Resources(0.5, 1, {}), 1, 15, await _get_default_region_name()
    )
    assert len(instances2) == 1
    public_address2 = list(instances2.keys())[0]
    job2 = list(instances2.values())[0][0]

    assert len(_get_ec2_instances(table)) == 2

    # deregister one without turning off the instance then adjust should terminate it
    # automatically
    _deregister_ec2_instance(public_address1, False)
    _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 1
    print(
        f"Now manually check that {public_address1} is being terminated but "
        f"{public_address2} is still running"
    )

    # now deallocate the job from the second instance
    await deallocate_job_from_ec2_instance(
        public_address2, job2, (await get_jobs_on_ec2_instance(public_address2))[job2]
    )

    # adjust should NOT deregister/terminate the instance because the timeout has not
    # happened yet.
    _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 1

    # after 30 seconds, run adjust again, now that instance should get
    # deregistered/terminated
    time.sleep(31)
    _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 0
    print(f"Now manually check that {public_address2} is also being terminated")


async def very_manual_test_create_ec2_alloc_lambda():
    """Tests setting up the ec2_alloc lambda"""
    # 1. delete the lambda and the ec2_alloc_lambda_role, then run this.
    # 2. make a small change to the lambda code then run this again
    await ensure_ec2_alloc_lambda(True)
