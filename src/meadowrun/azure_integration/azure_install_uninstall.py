from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Awaitable

if TYPE_CHECKING:
    from typing_extensions import Final

from meadowrun.azure_integration import blob_storage
from meadowrun.azure_integration.azure_mgmt_functions_setup import (
    _create_or_update_mgmt_function_app,
    _update_mgmt_function_code,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_poll,
    get_subscription_id,
    wait_for_poll,
)

RESOURCE_PROVIDERS: Final = (
    "Microsoft.OperationalInsights",
    "microsoft.insights",
    "Microsoft.Storage",
    "Microsoft.ManagedIdentity",
    "Microsoft.KeyVault",
    "Microsoft.Network",
    "Microsoft.Compute",
    "Microsoft.ContainerRegistry",
    "Microsoft.Web",
)


def _ensure_resource_provider_registered(
    subscription_id: str, resource_provider_ns: str
) -> Awaitable[None]:
    """Ensure the given resource provider is registered."""
    return azure_rest_api(
        "POST",
        f"subscriptions/{subscription_id}/providers/{resource_provider_ns}/register",
        "2021-04-01",
    )


async def _register_resource_providers(subscription_id: str) -> None:
    """Register all the resource providers we're using."""

    tasks = [
        _ensure_resource_provider_registered(subscription_id, rp)
        for rp in RESOURCE_PROVIDERS
    ]
    await asyncio.gather(*tasks)


async def delete_meadowrun_resource_group() -> None:
    """
    This should delete all meadowrun-generated resources as deletes everything in the
    meadowrun resource group.
    """
    try:
        # https://docs.microsoft.com/en-us/rest/api/resources/resource-groups/delete
        await wait_for_poll(
            await azure_rest_api_poll(
                "DELETE",
                f"subscriptions/{await get_subscription_id()}/resourcegroups/"
                f"{MEADOWRUN_RESOURCE_GROUP_NAME}",
                "2021-04-01",
                "LocationStatusCode",
            )
        )
    except ResourceNotFoundError:
        pass


async def install(location: str) -> None:
    subscription_id = await get_subscription_id()
    await _register_resource_providers(subscription_id)
    storage_account = await _create_or_update_mgmt_function_app(
        location, subscription_id
    )
    await blob_storage.ensure_container(subscription_id, storage_account, location)
    await _update_mgmt_function_code()
