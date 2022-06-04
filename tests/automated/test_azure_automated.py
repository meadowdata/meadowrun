import datetime
import io

import fabric

from basics import HostProvider, BasicsSuite, MapSuite, ErrorsSuite
from instance_registrar_suite import (
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
)
from meadowrun.azure_integration.azure_instance_allocation import AzureInstanceRegistrar
from meadowrun.azure_integration.azure_meadowrun_core import (
    get_default_location,
    ensure_meadowrun_resource_group,
)
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.azure.azure_rest_api import (
    azure_rest_api_paged,
    azure_rest_api,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import (
    _deregister_and_terminate_vms,
    _deregister_vm,
    terminate_all_vms,
)
from meadowrun.run_job import AllocCloudInstance
from meadowrun.run_job_core import Host, JobCompletion, CloudProviderType


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
        count = 0

        resource_group_path = await ensure_meadowrun_resource_group(
            instance_registrar.get_region_name()
        )
        virtual_machines_path = (
            f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines"
        )

        vm_names = [
            vm["name"]
            async for page in azure_rest_api_paged(
                "GET",
                virtual_machines_path,
                "2021-11-01",
            )
            for vm in page["value"]
        ]
        for vm_name in vm_names:
            vm = await azure_rest_api(
                "GET",
                f"{virtual_machines_path}/{vm_name}",
                "2021-11-01",
                query_parameters={"$expand": "instanceView"},
            )

            power_states = [
                status["code"][len("PowerState/") :]
                for status in vm["properties"]["instanceView"]["statuses"]
                if status["code"].startswith("PowerState/")
            ]
            if len(power_states) > 0 and power_states[-1] == "running":
                count += 1

        return count

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
