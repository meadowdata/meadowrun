"""
These tests require an AWS account to be set up, but don't require any manual
intervention beyond some initial setup. Also, these tests create instances (which cost
money!). Either `meadowrun-manage install` needs to be set up, or `meadowrun-manage
clean` needs to be run periodically
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from suites import (
    DeploymentSuite,
    DeploymentSuite2,
    EdgeCasesSuite,
    HostProvider,
    MapRetriesSuite,
)
from meadowrun import Resources, ssh
from meadowrun.aws_integration.ec2_instance_allocation import SSH_USER, AllocEC2Instance
from meadowrun.aws_integration.ec2_ssh_keys import get_meadowrun_ssh_key

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host, JobCompletion


# TODO don't always run tests in us-east-2
REGION = "us-east-2"


class AwsHostProvider(HostProvider):
    def get_resources_required(self) -> Resources:
        return Resources(1, 4, 80)

    def get_host(self) -> Host:
        return AllocEC2Instance(REGION)

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        log_file_name = job_completion.log_file_name
        if log_file_name.startswith(job_completion.public_address + ":"):
            log_file_name = log_file_name[len(job_completion.public_address) + 1 :]
        async with ssh.connect(
            job_completion.public_address,
            username=SSH_USER,
            private_key=get_meadowrun_ssh_key(REGION),
        ) as conn:
            return await ssh.read_text_from_file(conn, log_file_name)


class TestDeploymentsAws(AwsHostProvider, DeploymentSuite):
    pass


class TestDeployments2Aws(AwsHostProvider, DeploymentSuite2):
    pass


class TestEdgeCasesAws(AwsHostProvider, EdgeCasesSuite):
    pass


class TestMapRetriesAws(AwsHostProvider, MapRetriesSuite):
    pass
