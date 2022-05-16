"""
These tests require manual inspection while the test is running in order to verify that
the test has passed
"""

import time

import boto3

from automated.test_aws_automated import _clear_ec2_instances_table
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_mgmt_lambda_setup import (
    ensure_clean_up_lambda,
    ensure_ec2_alloc_lambda,
)
from meadowrun.aws_integration.ec2_alloc import (
    _EC2_ALLOC_LAMBDA_NAME,
    _ensure_ec2_alloc_table,
    _get_ec2_instances,
    _register_ec2_instance,
    allocate_ec2_instances,
    deallocate_job_from_ec2_instance,
    get_jobs_on_ec2_instance,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _deregister_ec2_instance,
    adjust,
)
from meadowrun.instance_selection import Resources
from meadowrun.run_job import run_function, EC2AllocHost


async def manual_test_deallocate_after_running():
    """Requires running commands manually during the test to confirm correct behavior"""
    await _clear_ec2_instances_table()

    def remote_function():
        time.sleep(60 * 10)

    await run_function(remote_function, EC2AllocHost(1, 0.5, 15))

    # 1. now get the address of the EC2 instance and the job id and SSH into the remote
    # server and run:
    # /meadowrun/env/bin/python /meadowrun/env/lib/python3.9/site-packages/meadowrun/deallocate_jobs.py --job-id JOB_ID  # noqa: E501
    # this should NOT deallocate the job, as it's still running
    # 2. now find the pid of the run_one_job process via `ps aux | grep run_one_job`,
    # and then kill that process using `kill -9 PID`. This way the regular
    # deallocate_jobs process won't be able to run
    # 3. now run
    # /meadowrun/env/bin/python /meadowrun/env/lib/python3.9/site-packages/meadowrun/deallocate_jobs.py  # noqa: E501
    # without the --job-id argument. You should see the job being deallocated.


async def manual_test_deallocate_before_running():
    """Requires running commands manually during the test to confirm correct behavior"""
    # run this after another test when we still have an instance lying around

    result = await allocate_ec2_instances(
        Resources(0.5, 1, {}), 1, 100, await _get_default_region_name()
    )
    print(result)
    # 1. now using the address of the EC2 instance we allocated to, SSH to the machine
    # and run:
    # /meadowrun/env/bin/python /meadowrun/env/lib/python3.9/site-packages/meadowrun/deallocate_jobs.py  # noqa: E501
    # this should NOT deallocate the job because the timeout has not elapsed yet
    # 2. Wait 7 minutes and then run the same command again, and the job should get
    # deallocated


async def _run_adjust(use_lambda: bool) -> None:
    if use_lambda:
        lambda_client = boto3.client("lambda")
        lambda_client.invoke(FunctionName=_EC2_ALLOC_LAMBDA_NAME)
    else:
        adjust(await _get_default_region_name())


async def manual_test_adjust_ec2_instances(use_lambda: bool):
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
    await _run_adjust(use_lambda)
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
    _deregister_ec2_instance(public_address1, False, await _get_default_region_name())
    await _run_adjust(use_lambda)
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
    await _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 1

    # after 30 seconds, run adjust again, now that instance should get
    # deregistered/terminated
    time.sleep(31)
    await _run_adjust(use_lambda)
    assert len(_get_ec2_instances(table)) == 0
    print(f"Now manually check that {public_address2} is also being terminated")


async def manual_test_create_management_lambdas():
    """Tests setting up the ec2_alloc lambda"""
    # 1. delete the lambda and the ec2_alloc_lambda_role, then run this.
    # 2. make a small change to the lambda code then run this again
    await ensure_ec2_alloc_lambda(True)
    await ensure_clean_up_lambda(True)
