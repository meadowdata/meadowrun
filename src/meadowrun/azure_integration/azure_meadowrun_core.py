"""
This contains "core" functionality used by more than one aspect of Meadowrun when
interacting with Azure
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Optional, Tuple, Set, Any

if TYPE_CHECKING:
    from typing_extensions import Literal

import aiohttp

from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_paged,
    azure_rest_api_poll,
    get_subscription_id,
    wait_for_poll,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_identity import (
    IMDS_AUTHORITY,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceExistsError,
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_storage_api import (
    StorageAccount,
    azure_table_api,
    table_key_url,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    LAST_USED_TABLE_NAME,
    MEADOWRUN_RESOURCE_GROUP_NAME,
    RESOURCE_TYPES_TYPE,
)


def get_default_location() -> str:
    # TODO try `az config get defaults.location`. Then our own custom config, maybe?
    return "eastus"


_MEADOWRUN_RESOURCE_GROUP_ENSURED = False


async def ensure_meadowrun_resource_group(location: str) -> str:
    """
    Creates the meadowrun resource group if it doesn't already exist. This resource
    group will contain all meadowrun-generated resources. Returns the path to the
    resource group.
    """
    global _MEADOWRUN_RESOURCE_GROUP_ENSURED

    subscription_id = await get_subscription_id()
    resource_group_path = (
        f"subscriptions/{subscription_id}/resourcegroups/"
        f"{MEADOWRUN_RESOURCE_GROUP_NAME}"
    )

    if not _MEADOWRUN_RESOURCE_GROUP_ENSURED:
        try:
            await azure_rest_api("GET", resource_group_path, "2021-04-01")
        except ResourceNotFoundError:
            print(
                f"The meadowrun resource group ({MEADOWRUN_RESOURCE_GROUP_NAME}) "
                f"doesn't exist, creating it now in {location}"
            )
            await azure_rest_api(
                "PUT",
                resource_group_path,
                "2021-04-01",
                json_content={"location": location},
            )

        _MEADOWRUN_RESOURCE_GROUP_ENSURED = True

    return resource_group_path


async def get_current_ip_address_on_vm() -> Optional[str]:
    """
    Assuming we're running on an Azure VM, get our current public ip address. If we're
    not running on an Azure VM, or we're running on an Azure VM without a public IP
    address, or if we're running on an Azure VM with a Standard SKU public IP address
    (see comments in _provision_nic_with_public_ip), we will return None.
    """
    async with aiohttp.request(
        "GET",
        f"{IMDS_AUTHORITY}/metadata/instance/network/interface/0/ipv4/ipAddress"
        "/0/publicIpAddress?api-version=2021-02-01&format=text",
        headers={"Metadata": "true"},
    ) as response:
        if not response.ok:
            return None

        return await response.text()


async def get_scheduled_events_on_vm() -> Optional[Any]:
    # See https://docs.microsoft.com/en-us/azure/virtual-machines/linux/scheduled-events
    async with aiohttp.request(
        "GET",
        f"{IMDS_AUTHORITY}/metadata/scheduledevents?api-version=2020-07-01",
        headers={"Metadata": "true"},
    ) as response:
        if not response.ok:
            return None

        return await response.json()


_MEADOWRUN_MANAGED_IDENTITY = "meadowrun-managed-identity"


async def _ensure_managed_identity(location: str) -> Tuple[str, str]:
    """Returns identity id, client id"""
    path = (
        f"{await ensure_meadowrun_resource_group(location)}/providers/"
        "Microsoft.ManagedIdentity/userAssignedIdentities/"
        f"{_MEADOWRUN_MANAGED_IDENTITY}"
    )
    try:
        identity = await azure_rest_api("GET", path, "2018-11-30")
    except ResourceNotFoundError:
        print(
            f"Azure managed identity {_MEADOWRUN_MANAGED_IDENTITY} does not exist, "
            f"creating it now"
        )

        identity = await azure_rest_api(
            "PUT", path, "2018-11-30", json_content={"location": location}
        )
        principal_id = identity["properties"]["principalId"]

        await assign_role_to_principal(
            "Contributor", principal_id, location, "ServicePrincipal"
        )
        await assign_role_to_principal(
            "Key Vault Secrets User",
            principal_id,
            location,
            "ServicePrincipal",
        )

    return identity["id"], identity["properties"]["clientId"]


async def assign_role_to_principal(
    role_name: str,
    principal_id: str,
    location: str,
    principal_type: Literal[None, "ServicePrincipal"] = None,
) -> None:
    """
    Assigns the specified role to the specified principal (e.g. user or identity) in the
    scope of the meadowrun resource group.

    principal_type should be set to ServicePrincipal as per the recommendation in
    https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-template#new-service-principal
    if the service principal is brand new.
    """
    subscription_id = await get_subscription_id()

    # Bizarrely, setting principal_type to ServicePrincipal for newly created service
    # identities only seems to have an effect in 2018-09-01-preview (or later according
    # to the docs), but the role_definitions property is missing on 2018-09-01-preview
    # (and later) API versions. So it seems like we need to use two different API
    # versions in this function
    roles = [
        role
        async for page in azure_rest_api_paged(
            "GET",
            f"subscriptions/{subscription_id}/providers/Microsoft.Authorization/"
            "roleDefinitions",
            "2018-01-01-preview",
            query_parameters={"$filter": f"roleName eq '{role_name}'"},
        )
        for role in page["value"]
    ]

    if len(roles) == 0:
        raise ValueError(f"Role {role_name} was not found")
    elif len(roles) > 1:
        raise ValueError(f"More than one role {role_name} was found")

    parameters = {
        "properties": {
            "roleDefinitionId": roles[0]["id"],
            "principalId": principal_id,
        }
    }
    if principal_type:
        parameters["properties"]["principalType"] = principal_type
    try:
        await azure_rest_api(
            "PUT",
            f"{await ensure_meadowrun_resource_group(location)}/providers/"
            f"Microsoft.Authorization/roleAssignments/{uuid.uuid4()}",
            "2018-09-01-preview",
            json_content=parameters,
        )
    except ResourceExistsError:
        # this means the role assignment already exists
        pass


async def ensure_meadowrun_storage_account(
    location: str, on_missing: Literal["raise", "create"]
) -> StorageAccount:
    """Returns (storage account name, key)"""
    subscription_id = await get_subscription_id()
    # the storage account name must be globally unique in Azure (across all users),
    # alphanumeric, and 24 characters or less. Our strategy is to use "mr" (for
    # meadowrun) plus the last 22 letters/numbers of the subscription id and hope for
    # the best.
    # TODO we need a way to manually set the storage account name in case this ends
    # up colliding
    storage_account_name = "mr" + subscription_id.replace("-", "")[-22:]

    storage_account_path = (
        f"{await ensure_meadowrun_resource_group(location)}/providers/"
        f"Microsoft.Storage/storageAccounts/{storage_account_name}"
    )
    # get the key to the storage account. If the storage account doesn't exist, create
    # it and then get the key
    try:
        # https://docs.microsoft.com/en-us/rest/api/storagerp/storage-accounts/list-keys
        keys = await azure_rest_api(
            "POST", f"{storage_account_path}/listKeys", "2021-09-01"
        )
    except ResourceNotFoundError:
        if on_missing == "raise":
            raise ValueError(f"Storage account {storage_account_name} does not exist")
        elif on_missing == "create":
            print(
                f"Storage account {storage_account_name} does not exist, creating it "
                f"now"
            )
            await wait_for_poll(
                await azure_rest_api_poll(
                    "PUT",
                    f"{storage_account_path}",
                    "2021-09-01",
                    "LocationStatusCode",
                    json_content={
                        "sku": {
                            # Standard (as opposed to Premium latency). Locally
                            # redundant storage (i.e. not very redundant, as opposed to
                            # zone-, geo-, or geo-and-zone- redundant storage)
                            "name": "Standard_LRS"
                        },
                        "kind": "StorageV2",
                        "location": location,
                    },
                )
            )
            keys = await azure_rest_api(
                "POST", f"{storage_account_path}/listKeys", "2021-09-01"
            )
        else:
            raise ValueError(f"Unexpected value for on_missing {on_missing}")

    return StorageAccount(
        storage_account_name, keys["keys"][0]["value"], storage_account_path
    )


_STORAGE_ACCOUNT: Optional[StorageAccount] = None
_EXISTING_TABLES: Set[str] = set()


async def ensure_table(
    table_name: str, location: str, on_missing: Literal["raise", "create"]
) -> StorageAccount:
    """
    Gets the TableClient for the specified table, creates it if it doesn't exist
    (depending on the value of the on_missing parameter). Multiple calls for the same
    table in the same process should be fast.
    """
    global _STORAGE_ACCOUNT
    if _STORAGE_ACCOUNT is None:
        # first, get the key to the storage account. If the storage account doesn't
        # exist, create it and then get the key
        _STORAGE_ACCOUNT = await ensure_meadowrun_storage_account(location, "create")

    if table_name not in _EXISTING_TABLES:
        # check if the table exists and create it if it doesn't
        table_path = (
            f"{_STORAGE_ACCOUNT.get_path()}/tableServices/default/tables/{table_name}"
        )
        try:
            await azure_rest_api("GET", table_path, "2021-09-01")
        except ResourceNotFoundError:
            if on_missing == "raise":
                raise ValueError(
                    f"Table {_STORAGE_ACCOUNT.name}/{table_name} does not exist"
                )
            elif on_missing == "create":
                print(
                    f"Table {_STORAGE_ACCOUNT.name}/{table_name} does not exist, "
                    "creating it now"
                )
                # https://docs.microsoft.com/en-us/rest/api/storagerp/table/create
                await azure_rest_api("PUT", table_path, "2021-09-01")
            else:
                raise ValueError(f"Unexpected value for on_missing {on_missing}")

        _EXISTING_TABLES.add(table_name)

    return _STORAGE_ACCOUNT


async def record_last_used(
    resource_type: RESOURCE_TYPES_TYPE, resource_name: str, location: str
) -> None:
    try:
        storage_account = await ensure_table(LAST_USED_TABLE_NAME, location, "create")
        # no need to insert timestamp, that's always included in the Azure table
        # metadata
        await azure_table_api(
            "PUT",
            storage_account,
            table_key_url(LAST_USED_TABLE_NAME, resource_type, resource_name),
            json_content={},
        )
    except Exception as e:
        logging.error(
            f"Warning, failed when trying to record usage of {resource_name}",
            exc_info=e,
        )
