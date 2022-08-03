from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import meadowrun.ssh as ssh

from basics import HostProvider, BasicsSuite, MapSuite, ErrorsSuite
from instance_registrar_suite import (
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
)
from meadowrun import ResourcesRequired
from meadowrun.azure_integration.azure_instance_allocation import AzureInstanceRegistrar
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
from meadowrun.run_job import AllocCloudInstance

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host, JobCompletion, CloudProviderType


class AzureHostProvider(HostProvider):
    def get_resources_required(self) -> ResourcesRequired:
        return ResourcesRequired(1, 4, 80)

    def get_host(self) -> Host:
        # TODO don't always run tests in us-east-2
        return AllocCloudInstance("AzureVM", region_name="eastus")

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    async def get_log_file_text(self, job_completion: JobCompletion) -> str:
        private_key, public_key = await ensure_meadowrun_key_pair("eastus")
        async with ssh.connect(
            job_completion.public_address,
            username="meadowrunuser",
            private_key=private_key,
        ) as conn:
            return await ssh.read_text_from_file(conn, job_completion.log_file_name)


class TestBasicsAzure(AzureHostProvider, BasicsSuite):
    pass


class TestErrorsAzure(AzureHostProvider, ErrorsSuite):
    pass


class TestMapAzure(MapSuite):
    def cloud_provider(self) -> CloudProviderType:
        return "AzureVM"


class AzureVMInstanceRegistrarProvider(
    InstanceRegistrarProvider[AzureInstanceRegistrar]
):
    async def get_instance_registrar(self) -> AzureInstanceRegistrar:
        return AzureInstanceRegistrar(get_default_location(), "create")

    async def deregister_instance(
        self,
        instance_registrar: AzureInstanceRegistrar,
        public_address: str,
        require_no_running_jobs: bool,
    ) -> bool:
        if require_no_running_jobs:
            vm = await instance_registrar.get_registered_instance(public_address)
            if len(vm.get_running_jobs()) > 0:
                return False
            etag = vm.etag
        else:
            etag = None

        assert instance_registrar._storage_account is not None
        return await _deregister_vm(
            instance_registrar._storage_account, public_address, etag
        )

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

    def cloud_provider(self) -> CloudProviderType:
        return "AzureVM"


class TestAzureVMInstanceRegistrar(
    AzureVMInstanceRegistrarProvider, InstanceRegistrarSuite
):
    pass
