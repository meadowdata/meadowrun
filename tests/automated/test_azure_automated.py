from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import meadowrun.ssh as ssh

from suites import (
    DeploymentSuite,
    DeploymentSuite2,
    EdgeCasesSuite,
    HostProvider,
    MapRetriesSuite,
)
from instance_registrar_suite import (
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
)
from meadowrun import Resources
from meadowrun.azure_integration.azure_instance_allocation import (
    AzureInstanceRegistrar,
    AllocAzureVM,
)
from meadowrun.azure_integration.azure_meadowrun_core import (
    get_default_location,
    ensure_meadowrun_resource_group,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.vm_adjust import (
    _deregister_and_terminate_vms,
    _deregister_vm,
    _get_running_vms,
    _get_all_vms,
    terminate_all_vms,
)

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host, JobCompletion


class AzureHostProvider(HostProvider):
    def get_resources_required(self) -> Resources:
        return Resources(1, 4, 80)

    def get_host(self) -> Host:
        # TODO don't always run tests in us-east-2
        return AllocAzureVM("eastus")

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        log_file_name = job_completion.log_file_name
        if log_file_name.startswith(job_completion.public_address + ":"):
            log_file_name = log_file_name[len(job_completion.public_address) + 1 :]
        private_key, public_key = await ensure_meadowrun_key_pair("eastus")
        async with ssh.connect(
            job_completion.public_address,
            username="meadowrunuser",
            private_key=private_key,
        ) as conn:
            return await ssh.read_text_from_file(conn, log_file_name)

    def get_num_concurrent_tasks(self) -> int:
        # default quota on Azure is very low (3vCPUs)
        return 2


class TestDeploymentsAzure(AzureHostProvider, DeploymentSuite):
    pass


class TestDeployments2Azure(AzureHostProvider, DeploymentSuite2):
    pass


class TestEdgeCasesAzure(AzureHostProvider, EdgeCasesSuite):
    pass


class TestMapRetriesAzure(AzureHostProvider, MapRetriesSuite):
    pass


class AzureVMInstanceRegistrarProvider(
    InstanceRegistrarProvider[AzureInstanceRegistrar]
):
    async def get_instance_registrar(self) -> AzureInstanceRegistrar:
        return AzureInstanceRegistrar(get_default_location(), "create")

    async def deregister_instance(
        self,
        instance_registrar: AzureInstanceRegistrar,
        name: str,
        require_no_running_jobs: bool,
    ) -> bool:
        if require_no_running_jobs:
            vm = await instance_registrar.get_registered_instance(name)
            if len(vm.get_running_jobs()) > 0:
                return False
            etag = vm.etag
        else:
            etag = None

        assert instance_registrar._storage_account is not None
        return await _deregister_vm(instance_registrar._storage_account, name, etag)

    async def num_currently_running_instances(
        self, instance_registrar: AzureInstanceRegistrar
    ) -> int:
        resource_group_path = await ensure_meadowrun_resource_group(
            instance_registrar.get_region_name()
        )
        running_vms, non_running_vms = await _get_running_vms(
            await _get_all_vms(resource_group_path), resource_group_path
        )
        return len(running_vms)

    async def run_adjust(self, instance_registrar: AzureInstanceRegistrar) -> None:
        assert instance_registrar._storage_account is not None
        await _deregister_and_terminate_vms(
            instance_registrar._storage_account,
            await ensure_meadowrun_resource_group(instance_registrar.get_region_name()),
            TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
            datetime.timedelta.min,
        )

    async def terminate_all_instances(
        self, instance_registrar: AzureInstanceRegistrar
    ) -> None:
        await terminate_all_vms(
            await ensure_meadowrun_resource_group(instance_registrar.get_region_name())
        )

    def get_host(self) -> Host:
        return AllocAzureVM()


class TestAzureVMInstanceRegistrar(
    AzureVMInstanceRegistrarProvider, InstanceRegistrarSuite
):
    pass
