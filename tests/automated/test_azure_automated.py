import datetime
import io

import fabric
from azure.mgmt.compute.aio import ComputeManagementClient

from basics import HostProvider, BasicsSuite, MapSuite
from instance_registrar_suite import (
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
)
from meadowrun.azure_integration.azure_core import (
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.azure_instance_allocation import AzureInstanceRegistrar
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
    get_credential_aio,
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

        assert instance_registrar._table_client is not None
        return await _deregister_vm(
            instance_registrar._table_client, public_address, etag
        )

    async def num_currently_running_instances(
        self, instance_registrar: AzureInstanceRegistrar
    ) -> int:
        count = 0
        async with ComputeManagementClient(
            get_credential_aio(), subscription_id=await get_subscription_id()
        ) as compute_client:

            async for vm in compute_client.virtual_machines.list(
                MEADOWRUN_RESOURCE_GROUP_NAME
            ):
                vm_info = await compute_client.virtual_machines.get(
                    MEADOWRUN_RESOURCE_GROUP_NAME, vm.name, expand="instanceView"
                )
                power_states = [
                    status.code[len("PowerState/") :]
                    for status in vm_info.instance_view.statuses
                    if status.code.startswith("PowerState/")
                ]
                if len(power_states) > 0 and power_states[-1] == "running":
                    count += 1

        return count

    async def run_adjust(self, instance_registrar: AzureInstanceRegistrar) -> None:
        async with ComputeManagementClient(
            get_credential_aio(), subscription_id=await get_subscription_id()
        ) as compute_client:
            assert instance_registrar._table_client is not None
            await _deregister_and_terminate_vms(
                instance_registrar._table_client,
                compute_client,
                TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
                datetime.timedelta.min,
            )

    async def terminate_all_instances(
        self, instance_registrar: AzureInstanceRegistrar
    ) -> None:
        async with ComputeManagementClient(
            get_credential_aio(), subscription_id=await get_subscription_id()
        ) as compute_client:
            await terminate_all_vms(compute_client)

    def cloud_provider(self) -> CloudProviderType:
        return "AzureVM"


class TestAzureVMInstanceRegistrar(
    AzureVMInstanceRegistrarProvider, InstanceRegistrarSuite
):
    pass
