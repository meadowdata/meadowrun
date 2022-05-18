import io

import fabric
import pytest

from basics import HostProvider, BasicsSuite
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.run_job import AllocCloudInstance, run_map, AllocCloudInstances
from meadowrun.run_job_core import Host, JobCompletion


class AzureHostProvider(HostProvider):
    # TODO don't always run tests in us-east-2

    def get_host(self) -> Host:
        return AllocCloudInstance(1, 2, 80, "AzureVM", "eastus")

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        private_key, public_key = await ensure_meadowrun_key_pair("eastus")
        with fabric.Connection(
            job_completion.public_address,
            user="meadowrunuser",
            connect_kwargs={"pkey": private_key},
        ) as conn:
            with io.BytesIO() as local_copy:
                conn.get(job_completion.log_file_name, local_copy)
                return local_copy.getvalue().decode("utf-8")


class TestBasicsAzure(AzureHostProvider, BasicsSuite):
    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_run_map(self):
        """Runs a "real" run_map"""
        results = await run_map(
            lambda x: x**x, [1, 2, 3, 4], AllocCloudInstances(1, 0.5, 15)
        )

        assert results == [1, 4, 27, 256]
