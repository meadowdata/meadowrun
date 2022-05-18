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
import os
from typing import Optional, Sequence, List

import azure.core.exceptions
from azure.core import MatchConditions
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables.aio import TableClient
from azure.mgmt.compute.aio import ComputeManagementClient

from ..azure_instance_alloc_stub import (
    LAST_UPDATE_TIME,
    MEADOWRUN_RESOURCE_GROUP_NAME,
    RUNNING_JOBS,
    SINGLE_PARTITION_KEY,
    VM_ALLOC_TABLE_NAME,
    VM_NAME,
    get_credential_aio,
)

MEADOWRUN_STORAGE_ACCOUNT_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT"
MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT_KEY"
MEADOWRUN_SUBSCRIPTION_ID = "MEADOWRUN_SUBSCRIPTION_ID"

# Terminate instances if they haven't run any jobs in the last 30 seconds
_TERMINATE_INSTANCES_IF_IDLE_FOR = datetime.timedelta(seconds=30)
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


async def _get_registered_vms(table_client: TableClient) -> Sequence[RegisteredVM]:
    """Gets instances registered by AzureInstanceRegistrars"""
    return [
        RegisteredVM(
            item["RowKey"],
            datetime.datetime.fromisoformat(item[LAST_UPDATE_TIME]),
            len(json.loads(item[RUNNING_JOBS])),
            item[VM_NAME],
            item.metadata["etag"],
        )
        async for item in table_client.list_entities(
            select=["RowKey", RUNNING_JOBS, LAST_UPDATE_TIME, VM_NAME]
        )
    ]


async def _deregister_vm(
    table_client: TableClient, public_address: str, etag: Optional[str]
) -> bool:
    """
    Deregisters a VM. If etag is None, deregisters unconditionally. If etag is provided,
    only deregisters if the etag matches. Returns False if the etag does not match (i.e.
    optimistic concurrency check failed). Returns True if successful.
    """
    if etag is not None:
        kwargs = {"etag": etag, "match_condition": MatchConditions.IfNotModified}
    else:
        kwargs = {}

    try:
        await table_client.delete_entity(SINGLE_PARTITION_KEY, public_address, **kwargs)
        return True
    except azure.core.exceptions.ResourceModifiedError:
        # this is how the API indicates that the etag does not match
        return False


async def _get_all_vms(compute_client: ComputeManagementClient) -> Sequence[ExistingVM]:
    """Gets all VMs via the Azure compute API"""
    return [
        ExistingVM(vm.name, vm.time_created)
        async for vm in compute_client.virtual_machines.list(
            MEADOWRUN_RESOURCE_GROUP_NAME
        )
    ]


async def _terminate_vm(compute_client: ComputeManagementClient, name: str) -> None:
    poller = await compute_client.virtual_machines.begin_delete(
        MEADOWRUN_RESOURCE_GROUP_NAME, name
    )
    await poller.result()


async def _deregister_and_terminate_vms(
    table_client: TableClient,
    compute_client: ComputeManagementClient,
    terminate_vms_if_idle_for: datetime.timedelta,
    launch_register_delay: datetime.timedelta,
) -> List[str]:
    """
    Returns a list of log statements. Because of a bug in Azure Functions, logging calls
    only work from the main thread:
    https://github.com/Azure/azure-functions-python-worker/issues/898 So we return our
    log statements and log them from the main thread, which is unfortunate, but better
    than having no logs.
    """
    logs = []

    existing_vms = await _get_all_vms(compute_client)
    registered_vms = await _get_registered_vms(table_client)

    now = datetime.datetime.utcnow()
    now_with_timezone = datetime.datetime.now(datetime.timezone.utc)

    termination_tasks = []

    existing_vms_names = {vm.name for vm in existing_vms}
    for vm in registered_vms:
        if vm.name not in existing_vms_names:
            # deregister VMs that have been registered but don't exist
            logs.append(
                f"{vm.name} ({vm.public_address}) is registered but does not exist, "
                "deregistering"
            )
            await _deregister_vm(table_client, vm.public_address, None)
        elif (
            vm.num_running_jobs == 0
            and (now - vm.last_update_time) > terminate_vms_if_idle_for
        ):
            # deregister and terminate machines that have been idle for a while
            success = await _deregister_vm(table_client, vm.public_address, vm.etag)
            if success:
                logs.append(
                    f"{vm.name} ({vm.public_address}) is not running any jobs and has "
                    f"not run anything since {vm.last_update_time} so we will "
                    "deregister and terminate it"
                )

                termination_tasks.append(
                    asyncio.create_task(_terminate_vm(compute_client, vm.name))
                )

    registered_vms_names = {vm.name for vm in registered_vms}
    for vm in existing_vms:
        if (
            vm.name not in registered_vms_names
            and (now_with_timezone - vm.time_created) > launch_register_delay
        ):
            # terminate instances that are running but not registered
            logs.append(
                f"{vm.name} is running but not registered, will terminate. Was launched"
                f" at {vm.time_created}"
            )
            termination_tasks.append(
                asyncio.create_task(_terminate_vm(compute_client, vm.name))
            )

    if termination_tasks:
        await asyncio.wait(termination_tasks)

    return logs


async def adjust() -> List[str]:
    """
    This code is designed to run on an Azure function as set up by
    azure_mgmt_functions_setup
    """
    storage_account_name = os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE]
    storage_account_key = os.environ[MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE]
    async with TableClient(
        f"https://{storage_account_name}.table.core.windows.net/",
        VM_ALLOC_TABLE_NAME,
        credential=AzureNamedKeyCredential(storage_account_name, storage_account_key),
    ) as table_client, ComputeManagementClient(
        get_credential_aio(), os.environ[MEADOWRUN_SUBSCRIPTION_ID]
    ) as compute_client:
        return await _deregister_and_terminate_vms(
            table_client,
            compute_client,
            _TERMINATE_INSTANCES_IF_IDLE_FOR,
            _LAUNCH_REGISTER_DELAY,
        )


async def terminate_all_vms(compute_client: ComputeManagementClient) -> None:
    terminate_tasks = [
        asyncio.create_task(_terminate_vm(compute_client, vm.name))
        for vm in await _get_all_vms(compute_client)
    ]
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
