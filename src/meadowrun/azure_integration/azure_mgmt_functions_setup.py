import asyncio
import io
import os
import zipfile
from typing import Tuple

import aiohttp
from azure.mgmt.applicationinsights.aio import ApplicationInsightsManagementClient
from azure.mgmt.loganalytics.aio import LogAnalyticsManagementClient
from azure.mgmt.web.aio import WebSiteManagementClient

import meadowrun.azure_integration.mgmt_functions
from meadowrun.azure_integration.azure_core import (
    _ensure_managed_identity,
    ensure_meadowrun_resource_group,
    ensure_meadowrun_storage_account,
    get_subscription_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    get_credential_aio,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import (
    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE,
    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE,
    MEADOWRUN_SUBSCRIPTION_ID,
)

_MEADOWRUN_MGMT_FUNCTIONS_LOGS_COMPONENT_NAME = "meadowrun-mgmt-functions-logs"
_MEADOWRUN_MGMT_FUNCTIONS_LOGS_WORKSPACE = "meadowrun-mgmt-functions-logs-workspace"


async def _create_application_insights_component(location: str) -> Tuple[str, str]:
    """
    The Azure Function logs get sent to an application insights component. Returns
    instrumentation key, connection string which is part of the Azure Function
    configuration
    """
    resource_group_name = await ensure_meadowrun_resource_group(location)
    subscription_id = await get_subscription_id()

    async with get_credential_aio() as credential:
        # first, we need to create a log analytics workspace
        async with LogAnalyticsManagementClient(
            credential, subscription_id
        ) as logs_client:
            # https://docs.microsoft.com/en-us/python/api/azure-mgmt-loganalytics/azure.mgmt.loganalytics.aio.operations.workspacesoperations?view=azure-python-preview#azure-mgmt-loganalytics-aio-operations-workspacesoperations-begin-create-or-update
            poller = await logs_client.workspaces.begin_create_or_update(
                resource_group_name,
                _MEADOWRUN_MGMT_FUNCTIONS_LOGS_WORKSPACE,
                {"location": location},
            )
            workspace = await poller.result()

        # then, we can create a application insights component based on that log
        # analytics workspace
        async with ApplicationInsightsManagementClient(
            credential, api_version="2020-02-02-preview"
        ) as client:
            # this seems like a bug in the Azure SDK
            client._config.subscription_id = subscription_id
            # https://docs.microsoft.com/en-us/python/api/azure-mgmt-applicationinsights/azure.mgmt.applicationinsights.v2020_02_02_preview.operations.componentsoperations?view=azure-python#azure-mgmt-applicationinsights-v2020-02-02-preview-operations-componentsoperations-create-or-update
            component = await client.components.create_or_update(
                resource_group_name,
                _MEADOWRUN_MGMT_FUNCTIONS_LOGS_COMPONENT_NAME,
                {
                    "location": location,
                    "kind": "web",
                    "properties": {
                        "Application_Type": "web",
                        "IngestionMode": "LogAnalytics",
                        "WorkspaceResourceId": workspace.id,
                    },
                },
            )
            return component.instrumentation_key, component.connection_string


def _meadowrun_mgmt_function_app_name(subscription_id: str) -> str:
    # This needs to be globally unique
    return "mr" + subscription_id.replace("-", "")


async def _create_or_update_mgmt_function_app(location: str) -> None:
    """ "Function app" and "site" seem to be interchangeable terms"""
    resource_group_name = await ensure_meadowrun_resource_group(location)
    subscription_id = await get_subscription_id()

    async with get_credential_aio() as credential, WebSiteManagementClient(
        credential, subscription_id, api_version="2021-03-01"
    ) as client:
        # application insights component for logging
        app_insights_component_task = asyncio.create_task(
            _create_application_insights_component(location)
        )
        # storage account is required by the functions runtime
        storage_account_task = ensure_meadowrun_storage_account(location, "create")

        # now create the actual "function app"/"site"
        site_name = _meadowrun_mgmt_function_app_name(subscription_id)
        identity_id, identity_client_id = await _ensure_managed_identity(location)
        # https://docs.microsoft.com/en-us/python/api/azure-mgmt-web/azure.mgmt.web.v2021_02_01.operations.webappsoperations?view=azure-python#azure-mgmt-web-v2021-02-01-operations-webappsoperations-begin-create-or-update
        poller = await client.web_apps.begin_create_or_update(
            resource_group_name,
            site_name,
            {
                "kind": "functionapp,linux",
                "location": location,
                # a truly insane name for this parameter, controls Linux vs Windows
                # https://docs.microsoft.com/en-us/azure/templates/microsoft.web/2018-02-01/serverfarms?tabs=bicep#appserviceplanproperties
                "reserved": True,
                "siteConfig": {"linux_fx_version": "Python|3.9"},
                "identity": {
                    "type": "UserAssigned",
                    "userAssignedIdentities": {identity_id: {}},
                },
            },
        )
        await poller.result()

        storage_account_name, key = await storage_account_task
        instrumentation_key, connection_string = await app_insights_component_task

        # the "Application Configuration" is a key part of setting up the function app.
        # These settings are technically just environment variables, but they are
        # important for configuring the function runtime
        # https://docs.microsoft.com/en-us/azure/azure-functions/functions-app-settings
        return await client.web_apps.update_application_settings(
            resource_group_name,
            site_name,
            {
                "properties": {
                    # these seem to be needed by the function runtime
                    "FUNCTIONS_EXTENSION_VERSION": "~4",
                    "FUNCTIONS_WORKER_RUNTIME": "python",
                    # this is required by the function runtime for its storage
                    "AzureWebJobsStorage": (
                        "DefaultEndpointsProtocol=https;"
                        f"AccountName={storage_account_name};AccountKey={key};"
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
                    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE: storage_account_name,
                    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE: key,
                    MEADOWRUN_SUBSCRIPTION_ID: subscription_id,
                }
            },
        )


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


async def _update_mgmt_function_code() -> None:
    """Assumes the function app already exists, uploads the code for the function app"""
    # https://github.com/Azure/azure-cli/blob/ccdc56e7806b6544ddf228bb04be83e485a7611a/src/azure-cli/azure/cli/command_modules/appservice/custom.py#L498

    subscription_id = await get_subscription_id()

    async with get_credential_aio() as credential:
        token = (
            await credential.get_token("https://management.azure.com/.default")
        ).token

    # https://docs.microsoft.com/en-us/azure/azure-functions/deployment-zip-push#with-curl
    async with aiohttp.request(
        "POST",
        f"https://{_meadowrun_mgmt_function_app_name(subscription_id)}"
        ".scm.azurewebsites.net/api/zipdeploy",
        data=_zip_azure_mgmt_function_code(),
        headers={"Authorization": f"Bearer {token}"},
    ) as response:
        response.raise_for_status()


async def create_or_update_mgmt_function(location: str) -> None:
    await _create_or_update_mgmt_function_app(location)
    await _update_mgmt_function_code()
