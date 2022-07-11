from __future__ import annotations

import asyncio
import io
import os
import time
import zipfile
from typing import TYPE_CHECKING, Tuple, Callable, Awaitable, TypeVar, Union, Type

import aiohttp

import meadowrun.azure_integration.mgmt_functions
from meadowrun.azure_integration.azure_meadowrun_core import (
    _ensure_managed_identity,
    ensure_meadowrun_resource_group,
    ensure_meadowrun_storage_account,
    get_subscription_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_identity import (
    get_token,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_poll,
    wait_for_poll,
)

if TYPE_CHECKING:
    from meadowrun.azure_integration.mgmt_functions.azure_core.azure_storage_api import (  # noqa: E501
        StorageAccount,
    )
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE,
    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE,
    MEADOWRUN_SUBSCRIPTION_ID,
)


_T = TypeVar("_T")

_MEADOWRUN_MGMT_FUNCTIONS_LOGS_COMPONENT_NAME = "meadowrun-mgmt-functions-logs"
_MEADOWRUN_MGMT_FUNCTIONS_LOGS_WORKSPACE = "meadowrun-mgmt-functions-logs-workspace"


async def _create_application_insights_component(location: str) -> Tuple[str, str]:
    """
    The Azure Function logs get sent to an application insights component. Returns
    instrumentation key, connection string which is part of the Azure Function
    configuration
    """

    resource_group_path = await ensure_meadowrun_resource_group(location)

    # first, we need to create a log analytics workspace
    workspace_path = (
        f"{resource_group_path}/providers/Microsoft.OperationalInsights/workspaces/"
        f"{_MEADOWRUN_MGMT_FUNCTIONS_LOGS_WORKSPACE}"
    )
    await wait_for_poll(
        await azure_rest_api_poll(
            "PUT",
            workspace_path,
            "2021-06-01",
            "GetProvisioningState",
            json_content={"location": location},
        )
    )

    # then, we can create a application insights component based on that log analytics
    # workspace
    component = await azure_rest_api(
        "PUT",
        (
            f"{resource_group_path}/providers/Microsoft.Insights/components/"
            f"{_MEADOWRUN_MGMT_FUNCTIONS_LOGS_COMPONENT_NAME}"
        ),
        "2020-02-02-preview",
        json_content={
            "location": location,
            "kind": "web",
            "properties": {
                "Application_Type": "web",
                "IngestionMode": "LogAnalytics",
                "WorkspaceResourceId": "/" + workspace_path,
            },
        },
    )
    return (
        component["properties"]["InstrumentationKey"],
        component["properties"]["ConnectionString"],
    )


def _meadowrun_mgmt_function_app_name(subscription_id: str) -> str:
    # This needs to be globally unique
    return "mr" + subscription_id.replace("-", "")


async def _create_or_update_mgmt_function_app(
    location: str, subscription_id: str
) -> StorageAccount:
    """ "Function app" and "site" seem to be interchangeable terms"""
    resource_group_path = await ensure_meadowrun_resource_group(location)

    site_path = (
        f"{resource_group_path}/providers/Microsoft.Web/sites/"
        f"{_meadowrun_mgmt_function_app_name(subscription_id)}"
    )

    # first create some prerequisites:

    # create application insights component for logging
    app_insights_component_task = asyncio.create_task(
        _create_application_insights_component(location)
    )

    # ensure storage account which is required by the functions runtime
    storage_account_task = ensure_meadowrun_storage_account(location, "create")

    # managed identity for the function
    identity_id, identity_client_id = await _ensure_managed_identity(location)

    # now create the actual "function app"/"site"
    # https://docs.microsoft.com/en-us/rest/api/appservice/web-apps/create-or-update
    # oddly, the python SDK thinks that we should be doing the equivalent of an
    # azure_rest_api_poll(poll_scheme="GetProvisioningState"), but there's no
    # "provisioningState" property to check. This seems like a bug/outdated
    # implementation in the SDK.
    await azure_rest_api(
        "PUT",
        site_path,
        "2021-03-01",
        json_content={
            "kind": "functionapp,linux",
            "location": location,
            "identity": {
                "type": "UserAssigned",
                "userAssignedIdentities": {identity_id: {}},
            },
            "properties": {
                # a truly insane name for this parameter, controls Linux vs Windows
                # https://docs.microsoft.com/en-us/azure/templates/microsoft.web/2018-02-01/serverfarms?tabs=bicep#appserviceplanproperties
                "reserved": True,
                "siteConfig": {"linuxFxVersion": "Python|3.9"},
            },
        },
    )

    storage_account = await storage_account_task
    instrumentation_key, connection_string = await app_insights_component_task

    # the "Application Configuration" is a key part of setting up the function app.
    # These settings are technically just environment variables, but they are important
    # for configuring the function runtime
    # https://docs.microsoft.com/en-us/azure/azure-functions/functions-app-settings
    await azure_rest_api(
        "PUT",
        f"{site_path}/config/appsettings",
        "2021-03-01",
        json_content={
            "properties": {
                # these seem to be needed by the function runtime
                "FUNCTIONS_EXTENSION_VERSION": "~4",
                "FUNCTIONS_WORKER_RUNTIME": "python",
                # this is required by the function runtime for its storage
                "AzureWebJobsStorage": (
                    "DefaultEndpointsProtocol=https;"
                    f"AccountName={storage_account.name};"
                    f"AccountKey={storage_account.key};"
                    "EndpointSuffix=core.windows.net"
                ),
                # these variables are needed to for logging to Application Insights
                "APPINSIGHTS_INSTRUMENTATIONKEY": instrumentation_key,
                "APPLICATIONINSIGHTS_CONNECTION_STRING": connection_string,
                # the ManagedIdentityClient seems to need this when running in a
                # function
                "AZURE_CLIENT_ID": identity_client_id,
                # These are used by meadowrun code as environment variables to
                # access the instance registrar table
                MEADOWRUN_STORAGE_ACCOUNT_VARIABLE: storage_account.name,
                MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE: storage_account.key,
                MEADOWRUN_SUBSCRIPTION_ID: subscription_id,
            }
        },
    )
    return storage_account


_IGNORE_FOR_ZIP = {".git", ".venv", ".vscode"}


def _zip_azure_mgmt_function_code() -> bytes:
    """Zips all the code for meadowrun.azure_integration.mgmt_functions"""
    # https://docs.microsoft.com/en-us/azure/azure-functions/functions-reference-python?tabs=asgi%2Cazurecli-linux%2Capplication-level#folder-structure

    zip_path = meadowrun.azure_integration.mgmt_functions.__path__[0]
    first_root_len = None

    with io.BytesIO() as buffer:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(zip_path):
                to_remove = [d for d in dirs if d in _IGNORE_FOR_ZIP]
                for d in to_remove:
                    # this prevents iteration into these directories
                    dirs.remove(d)

                if first_root_len is None:
                    first_root_len = len(root)

                for file in files:
                    if file not in _IGNORE_FOR_ZIP:
                        zf.write(
                            os.path.join(root, file),
                            os.path.join(root[first_root_len:], file),
                        )

        buffer.seek(0)

        return buffer.read()


async def _update_mgmt_function_code(total_timeout_secs: float = 300) -> None:
    """Assumes the function app already exists, uploads the code for the function app"""
    # https://github.com/Azure/azure-cli/blob/ccdc56e7806b6544ddf228bb04be83e485a7611a/src/azure-cli/azure/cli/command_modules/appservice/custom.py#L498
    # https://docs.microsoft.com/en-us/azure/azure-functions/deployment-zip-push#with-curl

    headers = {"Authorization": f"Bearer {await get_token()}"}

    # this is effectively the same code as azure_rest_api_poll, but we don't have an
    # api-version parameter

    async with aiohttp.request(
        "POST",
        f"https://{_meadowrun_mgmt_function_app_name(await get_subscription_id())}"
        ".scm.azurewebsites.net/api/zipdeploy",
        params={"isAsync": "true"},
        data=_zip_azure_mgmt_function_code(),
        headers=headers,
    ) as response:
        response.raise_for_status()

        if response.status not in (201, 202):
            return

        poll_url = response.headers["Location"]

    t0 = time.time()
    while time.time() - t0 < total_timeout_secs:
        print(f"Waiting for a long-running Azure operation ({5}s)")
        await asyncio.sleep(5)
        async with aiohttp.request("GET", poll_url, headers=headers) as response:
            response.raise_for_status()

            if response.status not in (201, 202):
                return

    raise TimeoutError(
        f"_update_mgmt_function_code timed out after {total_timeout_secs}"
    )


async def _retry(
    function: Callable[[], Awaitable[_T]],
    exception_types: Union[Type, Tuple[Type, ...]],
    max_num_attempts: int = 5,
    delay_seconds: float = 1,
    retry_message: str = "Retrying on error",
) -> _T:
    i = 0
    while True:
        try:
            return await function()
        except exception_types as e:
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(f"{retry_message}: {e}")
                await asyncio.sleep(delay_seconds)
