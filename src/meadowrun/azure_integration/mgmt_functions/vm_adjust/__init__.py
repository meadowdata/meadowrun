"""
This code cannot reference anything outside of mgmt_functions (as that's what gets
uploaded to the Azure function). We use relative imports which will work both in the
"regular" environment as well as in the Azure function
"""

import asyncio
import dataclasses
import datetime
import json
import logging
from typing import Optional, Sequence, List, Tuple

from ..azure_core.azure_exceptions import ResourceModifiedError
from ..azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_paged,
    azure_rest_api_poll,
    parse_azure_timestamp,
    wait_for_poll,
)
from ..azure_core.azure_storage_api import (
    StorageAccount,
    azure_table_api,
    azure_table_api_paged,
    table_key_url,
)
from ..azure_constants import (
    LAST_UPDATE_TIME,
    RUNNING_JOBS,
    SINGLE_PARTITION_KEY,
    VM_ALLOC_TABLE_NAME,
    VM_NAME,
)
from ..mgmt_functions_shared import get_resource_group_path, get_storage_account

# Terminate instances if they haven't run any jobs in the last 5 minutes
_TERMINATE_INSTANCES_IF_IDLE_FOR = datetime.timedelta(minutes=5)
# If we see instances running that aren't registered, we assume there is something wrong
# and they need to be terminated. However, it's possible that we happen to query between
# when an instance is launched and when it's registered. So for the first 5 minutes
# after an instance is launched, we don't terminate it even if it's not registered.
_LAUNCH_REGISTER_DELAY = datetime.timedelta(minutes=5)


@dataclasses.dataclass
class RegisteredVM:
    public_address: str
    last_update_time: datetime.datetime
    num_running_jobs: int
    name: str
    etag: str


@dataclasses.dataclass
class ExistingVM:
    name: str
    time_created: datetime.datetime


async def _get_registered_vms(
    storage_account: StorageAccount,
) -> Sequence[RegisteredVM]:
    """Gets instances registered by AzureInstanceRegistrars"""
    return [
        RegisteredVM(
            item["RowKey"],
            datetime.datetime.fromisoformat(item[LAST_UPDATE_TIME]),
            len(json.loads(item[RUNNING_JOBS])),
            item[VM_NAME],
            item["odata.etag"],
        )
        async for page in azure_table_api_paged(
            "GET",
            storage_account,
            VM_ALLOC_TABLE_NAME,
            query_parameters={
                "$select": ",".join(["RowKey", RUNNING_JOBS, LAST_UPDATE_TIME, VM_NAME])
            },
        )
        for item in page["value"]
    ]


async def _deregister_vm(
    storage_account: StorageAccount, public_address: str, etag: Optional[str]
) -> bool:
    """
    Deregisters a VM. If etag is None, deregisters unconditionally. If etag is provided,
    only deregisters if the etag matches. Returns False if the etag does not match (i.e.
    optimistic concurrency check failed). Returns True if successful.
    """
    if etag is not None:
        if_match = etag
    else:
        if_match = "*"

    try:
        await azure_table_api(
            "DELETE",
            storage_account,
            table_key_url(VM_ALLOC_TABLE_NAME, SINGLE_PARTITION_KEY, public_address),
            additional_headers={"If-Match": if_match},
        )
        return True
    except ResourceModifiedError:
        # this is how the API indicates that the etag does not match
        return False


async def _get_running_vms(
    vms_to_check: Sequence[ExistingVM], resource_group_path: str
) -> Tuple[Sequence[ExistingVM], Sequence[ExistingVM]]:
    """Returns (running vms, not running vms)"""

    virtual_machines_path = (
        f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines"
    )

    result: Tuple[List[ExistingVM], List[ExistingVM]] = ([], [])

    for vm in vms_to_check:
        vm_view = await azure_rest_api(
            "GET",
            f"{virtual_machines_path}/{vm.name}",
            "2021-11-01",
            query_parameters={"$expand": "instanceView"},
        )

        power_states = [
            status["code"][len("PowerState/") :]
            for status in vm_view["properties"]["instanceView"]["statuses"]
            if status["code"].startswith("PowerState/")
        ]
        if len(power_states) > 0 and power_states[-1] == "running":
            result[0].append(vm)
        else:
            result[1].append(vm)

    return result


async def _get_all_vms(resource_group_path: str) -> Sequence[ExistingVM]:
    """Gets all VMs via the Azure compute API"""
    return [
        ExistingVM(
            item["name"], parse_azure_timestamp(item["properties"]["timeCreated"])
        )
        async for page in azure_rest_api_paged(
            "GET",
            f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines",
            "2021-11-01",
        )
        for item in page["value"]
    ]


async def _terminate_vm(resource_group_path: str, name: str) -> None:
    await (
        wait_for_poll(
            await azure_rest_api_poll(
                "DELETE",
                f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines/"
                f"{name}",
                "2021-11-01",
                "LocationStatusCode",
            )
        )
    )


async def _deregister_and_terminate_vms(
    storage_account: StorageAccount,
    resource_group_path: str,
    terminate_vms_if_idle_for: datetime.timedelta,
    launch_register_delay: datetime.timedelta = _LAUNCH_REGISTER_DELAY,
) -> List[str]:
    """
    Returns a list of log statements. Because of a bug in Azure Functions, logging calls
    only work from the main thread:
    https://github.com/Azure/azure-functions-python-worker/issues/898 So we return our
    log statements and log them from the main thread, which is unfortunate, but better
    than having no logs.
    """
    logs = []

    all_vms = await _get_all_vms(resource_group_path)
    running_vms, non_running_vms = await _get_running_vms(all_vms, resource_group_path)
    registered_vms = await _get_registered_vms(storage_account)

    now = datetime.datetime.utcnow()

    termination_tasks = []

    existing_vms_names = {vm.name for vm in running_vms}
    for vm in registered_vms:
        if vm.name not in existing_vms_names:
            # deregister VMs that have been registered but don't exist
            logs.append(
                f"{vm.name} ({vm.public_address}) is registered but does not exist, "
                "deregistering"
            )
            await _deregister_vm(storage_account, vm.public_address, None)
        elif (
            vm.num_running_jobs == 0
            and (now - vm.last_update_time) > terminate_vms_if_idle_for
        ):
            # deregister and terminate machines that have been idle for a while
            success = await _deregister_vm(storage_account, vm.public_address, vm.etag)
            if success:
                logs.append(
                    f"{vm.name} ({vm.public_address}) is not running any jobs and has "
                    f"not run anything since {vm.last_update_time} so we will "
                    "deregister and terminate it"
                )

                termination_tasks.append(
                    asyncio.create_task(_terminate_vm(resource_group_path, vm.name))
                )
        else:
            logs.append(
                f"Letting {vm.name} ({vm.public_address}) continue to run--this VM is "
                "active"
            )

    registered_vms_names = {vm.name for vm in registered_vms}
    for vm in running_vms:
        if (
            vm.name not in registered_vms_names
            and (now - vm.time_created) > launch_register_delay
        ):
            # terminate instances that are running but not registered
            logs.append(
                f"{vm.name} is running but not registered, will terminate. Was launched"
                f" at {vm.time_created}"
            )
            termination_tasks.append(
                asyncio.create_task(_terminate_vm(resource_group_path, vm.name))
            )

    # TODO terminate stopped VMs

    if termination_tasks:
        await asyncio.wait(termination_tasks)

    return logs


async def adjust() -> List[str]:
    """
    This code is designed to run on an Azure function as set up by
    azure_mgmt_functions_setup
    """
    return await _deregister_and_terminate_vms(
        get_storage_account(),
        get_resource_group_path(),
        _TERMINATE_INSTANCES_IF_IDLE_FOR,
        _LAUNCH_REGISTER_DELAY,
    )


async def terminate_all_vms(resource_group_path: str) -> None:
    terminate_tasks = []
    for vm in await _get_all_vms(resource_group_path):
        print(f"Terminating {vm.name}")
        terminate_tasks.append(
            asyncio.create_task(_terminate_vm(resource_group_path, vm.name))
        )
    if terminate_tasks:
        await asyncio.wait(terminate_tasks)


def main(myTimer) -> None:  # type: ignore
    """
    The entry point for the Azure function (see function.json). Configured to run every
    minute by default.

    myTime should be annotated as azure.functions.TimerRequest, but we'd rather not add
    that dependency (it exists by default in the function runtime where this function
    will actually run). Also, the variable cannot be snake_case, and can only be changed
    if the corresponding name in function.json is changed.
    """
    logs = asyncio.run(adjust())
    for log in logs:
        logging.info(log)
