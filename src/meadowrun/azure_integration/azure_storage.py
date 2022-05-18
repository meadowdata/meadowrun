from typing import Tuple, Literal

import azure.core.exceptions
from azure.mgmt.storage.aio import StorageManagementClient

from meadowrun.azure_integration.azure_core import (
    ensure_meadowrun_resource_group,
    get_subscription_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    get_credential_aio,
)


async def ensure_meadowrun_storage_account(
    location: str, on_missing: Literal["raise", "create"]
) -> Tuple[str, str]:
    """Returns (storage account name, key)"""
    subscription_id = await get_subscription_id()
    # the storage account name must be globally unique in Azure (across all users),
    # alphanumeric, and 24 characters or less. Our strategy is to use "mr" (for
    # meadowrun) plus the last 22 letters/numbers of the subscription id and hope for
    # the best.
    # TODO we need a way to manually set the storage account name in case this ends
    # up colliding
    storage_account_name = "mr" + subscription_id.replace("-", "")[-22:]
    resource_group_name = await ensure_meadowrun_resource_group(location)

    async with get_credential_aio() as credential, StorageManagementClient(
        credential, subscription_id
    ) as client:
        # first, get the key to the storage account. If the storage account doesn't
        # exist, create it and then get the key
        try:
            key = (
                (
                    await client.storage_accounts.list_keys(
                        resource_group_name, storage_account_name
                    )
                )
                .keys[0]
                .value
            )
        except azure.core.exceptions.ResourceNotFoundError:
            if on_missing == "raise":
                raise ValueError(
                    f"Storage account {storage_account_name} does not exist"
                )
            elif on_missing == "create":
                print(
                    f"Storage account {storage_account_name} does not exist, "
                    "creating it now"
                )
                poller = await client.storage_accounts.begin_create(
                    resource_group_name,
                    storage_account_name,
                    # https://docs.microsoft.com/en-us/python/api/azure-mgmt-storage/azure.mgmt.storage.v2021_09_01.models.storageaccountcreateparameters?view=azure-python
                    {
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
                await poller.result()
                key = (
                    (
                        await client.storage_accounts.list_keys(
                            resource_group_name, storage_account_name
                        )
                    )
                    .keys[0]
                    .value
                )
            else:
                raise ValueError(f"Unexpected value for on_missing {on_missing}")

    return storage_account_name, key
