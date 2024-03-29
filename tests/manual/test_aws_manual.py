"""
These tests require manual inspection while the test is running in order to verify that
the test has passed
"""

import time
import uuid

from automated.aws.test_instance_registrar import EC2InstanceRegistrarProvider
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_mgmt_lambda_install import (
    ensure_clean_up_lambda,
    ensure_ec2_alloc_lambda,
)
from meadowrun.aws_integration.ec2_instance_allocation import (
    AllocEC2Instance,
    EC2InstanceRegistrar,
)
from meadowrun.instance_allocation import (
    allocate_single_job_to_instance,
)
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.run_job import run_function
from meadowrun.run_job_core import Resources


async def manual_test_deallocate_after_running() -> None:
    """Requires running commands manually during the test to confirm correct behavior"""
    async with EC2InstanceRegistrar(None, "create") as instance_registrar:
        await EC2InstanceRegistrarProvider().clear_instance_registrar(
            instance_registrar
        )

        def remote_function() -> None:
            time.sleep(60 * 10)

        await run_function(remote_function, AllocEC2Instance(), Resources(1, 0.5, 15))

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


async def manual_test_deallocate_before_running() -> None:
    """Requires running commands manually during the test to confirm correct behavior"""
    # run this after another test when we still have an instance lying around

    async with EC2InstanceRegistrar(None, "create") as instance_registrar:
        result = await allocate_single_job_to_instance(
            instance_registrar,
            ResourcesInternal.from_cpu_and_memory(1, 0.5, 100),
            str(uuid.uuid4()),
            AllocEC2Instance(),
            None,
        )
        print(result)

    # 1. now using the address of the EC2 instance we allocated to, SSH to the machine
    # and run:
    # /meadowrun/env/bin/python /meadowrun/env/lib/python3.9/site-packages/meadowrun/deallocate_jobs.py  # noqa: E501
    # this should NOT deallocate the job because the timeout has not elapsed yet
    # 2. Wait 7 minutes and then run the same command again, and the job should get
    # deallocated


async def manual_test_create_management_lambdas() -> None:
    """Tests setting up the ec2_alloc lambda"""
    # 1. delete the lambda and the ec2_alloc_lambda_role, then run this.
    # 2. make a small change to the lambda code then run this again
    region_name = await _get_default_region_name()
    await ensure_ec2_alloc_lambda(True, None, region_name)
    await ensure_clean_up_lambda(True, None, region_name)
