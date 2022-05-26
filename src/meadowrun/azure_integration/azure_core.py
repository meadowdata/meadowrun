from __future__ import annotations

import logging
import os
import uuid
from types import TracebackType
from typing import cast, Optional, Literal, Type, Tuple, Set

import aiohttp
import azure.core
import azure.core.exceptions
import azure.identity
import azure.identity.aio
from azure.core.credentials import TokenCredential, AzureNamedKeyCredential
from azure.data.tables.aio import TableClient
from azure.mgmt.authorization.aio import AuthorizationManagementClient
from azure.mgmt.msi.aio import ManagedServiceIdentityClient
from azure.mgmt.resource.resources.aio import ResourceManagementClient

# There are two Subscription clients, azure.mgmt.subscription.SubscriptionClient and
# azure.mgmt.resource.subscriptions. They seem pretty similar, but the first one seems
# to return a tenant_id for each subscription and the second one does not, so we just
# use the first one.
from azure.mgmt.resource.subscriptions import (
    SubscriptionClient as SubscriptionClientSync,
)
from azure.mgmt.resource.subscriptions.aio import SubscriptionClient
from azure.mgmt.storage.aio import StorageManagementClient
from msgraph.core import GraphClient

from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    LAST_USED_TABLE_NAME,
    MEADOWRUN_RESOURCE_GROUP_NAME,
    RESOURCE_TYPES_TYPE,
    _DEFAULT_CREDENTIAL_OPTIONS,
    get_credential_aio,
)


# credentials and subscriptions


class TokenCredentialWithContextManager(TokenCredential):
    """
    This is a really silly duck-typing shim. DefaultAzureCredential implements the
    TokenCredential interface but doesn't declare it. But DefaultAzureCredential needs
    to be used as a context manager, but the TokenCredential interface doesn't have the
    __enter__/__exit__ functions. So we create this type just to satisfy the type
    checker.
    """

    def __enter__(self) -> TokenCredentialWithContextManager:
        pass

    def __exit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass


def get_credential() -> TokenCredentialWithContextManager:
    return cast(
        TokenCredentialWithContextManager,
        azure.identity.DefaultAzureCredential(**_DEFAULT_CREDENTIAL_OPTIONS),
    )


_SUBSCRIPTION_ID = None
_TENANT_ID = None


async def get_tenant_id() -> str:
    """Gets the tenant id corresponding to the subscription from get_subscription_id"""
    # transitively can modify _SUBSCRIPTION_ID as well
    global _TENANT_ID

    if _TENANT_ID is not None:
        return _TENANT_ID

    if _SUBSCRIPTION_ID is None:
        await get_subscription_id()

        # it's possible that get_subscription_id populated _TENANT_ID in which case we
        # don't need to do anything else
        if _TENANT_ID is not None:
            return _TENANT_ID

    # in this case we need to look up the tenant id from the subscription id
    async with get_credential_aio() as credential, SubscriptionClient(
        credential
    ) as sub_client:
        _TENANT_ID = (await sub_client.subscriptions.get(_SUBSCRIPTION_ID)).tenant_id
        return _TENANT_ID


async def get_subscription_id() -> str:
    """
    First, tries to get the AZURE_SUBSCRIPTION_ID environment variable. If that's not
    available, queries for available subscription and if there's only one, returns that.
    If there are 0 or 2+ available subscriptions, raises an exception. The subscription
    id we choose is cached and will not change for the duration of the process.
    """

    global _SUBSCRIPTION_ID
    # This function MIGHT populate _TENANT_ID if it's available because we had to query
    # for the available subscriptions. If we're just reading the subscription id off of
    # the environment variable, then we don't populate _TENANT_ID
    global _TENANT_ID

    if _SUBSCRIPTION_ID is None:
        specified_subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")

        if specified_subscription_id:
            _SUBSCRIPTION_ID = specified_subscription_id
        else:
            async with get_credential_aio() as credential, SubscriptionClient(
                credential
            ) as sub_client:
                subscriptions = [
                    sub
                    async for sub in sub_client.subscriptions.list()
                    if sub.state == "Enabled"
                ]
            if len(subscriptions) > 1:
                raise ValueError(
                    "Please specify a subscription via the "
                    "AZURE_SUBSCRIPTION_ID environment variable from among the "
                    "available subscription ids: "
                    + ", ".join([sub.subscription_id for sub in subscriptions])
                )
            elif len(subscriptions) == 0:
                raise ValueError("There are no subscriptions available")
            else:
                _SUBSCRIPTION_ID = cast(str, subscriptions[0].subscription_id)
                _TENANT_ID = cast(str, subscriptions[0].tenant_id)

    return _SUBSCRIPTION_ID


def get_subscription_id_sync() -> str:
    """Identical to get_subscription_id but not async"""

    global _SUBSCRIPTION_ID
    global _TENANT_ID

    if _SUBSCRIPTION_ID is None:
        specified_subscription_id = os.environ.get("AZURE_SUBSCRIPTION_ID")

        if specified_subscription_id:
            _SUBSCRIPTION_ID = specified_subscription_id
        else:
            with get_credential() as credential, SubscriptionClientSync(
                credential
            ) as sub_client:
                subscriptions = [
                    sub
                    for sub in sub_client.subscriptions.list()
                    if sub.state == "Enabled"
                ]
            if len(subscriptions) > 1:
                raise ValueError(
                    "Please specify a subscription via the "
                    "AZURE_SUBSCRIPTION_ID environment variable from among the "
                    "available subscription ids: "
                    + ", ".join([sub.subscription_id for sub in subscriptions])
                )
            elif len(subscriptions) == 0:
                raise ValueError("There are no subscriptions available")
            else:
                _SUBSCRIPTION_ID = cast(str, subscriptions[0].subscription_id)
                _TENANT_ID = cast(str, subscriptions[0].tenant_id)

    return _SUBSCRIPTION_ID


def get_current_user_id() -> str:
    """
    This functions gets the id of the current user that is signed in to the Azure CLI.

    In order to get this information, it looks like there are two different services,
    "Microsoft Graph" (developer.microsoft.com/graph) and "Azure AD Graph"
    (graph.windows.net), the latter being deprecated
    (https://devblogs.microsoft.com/microsoft365dev/microsoft-graph-or-azure-ad-graph/).
    I think these services correspond to two different python libraries, msal
    (https://docs.microsoft.com/en-us/python/api/overview/azure/active-directory?view=azure-python)
    and adal (https://docs.microsoft.com/en-us/python/api/adal/adal?view=azure-python),
    but these libraries don't appear to do anything super useful on their own.

    The deprecated Azure Graph API seems to correspond to a higher-level library
    azure-graphrbac, which does seem to have the functionality we need:
    azure.graphrbac.GraphRbacManagementClient.signed_in_user, but is deprecated along
    with Azure Graph
    (https://github.com/Azure/azure-sdk-for-python/issues/14022#issuecomment-752279618).

    The msgraph library that we use here seems to be a not-very-high-level library
    for Microsoft Graph (https://github.com/microsoftgraph/msgraph-sdk-python-core).

    As a side note, another way to get this information is to use the command line to
    call `az ad signed-in-user show`, but that appears to be relying on the deprecated
    Azure Graph API as it gives a deprecation warning.
    """

    # crucial scopes parameter is needed, see
    # https://github.com/microsoftgraph/msgraph-sdk-python-core/issues/106#issuecomment-969281260
    with get_credential() as credential:
        client = GraphClient(
            credential=credential, scopes=["https://graph.microsoft.com"]
        )
    # https://docs.microsoft.com/en-us/graph/api/user-get?view=graph-rest-1.0&tabs=http
    result = client.get("/me")
    return result.json()["id"]


def get_default_location() -> str:
    # TODO try `az config get defaults.location`. Then our own custom config, maybe?
    return "eastus"


_MEADOWRUN_RESOURCE_GROUP_ENSURED = False


async def ensure_meadowrun_resource_group(location: str) -> str:
    """
    Creates the meadowrun resource group if it doesn't already exist. This resource
    group will contain all meadowrun-generated resources
    """
    global _MEADOWRUN_RESOURCE_GROUP_ENSURED

    if not _MEADOWRUN_RESOURCE_GROUP_ENSURED:
        async with get_credential_aio() as credential, ResourceManagementClient(
            credential, await get_subscription_id()
        ) as resource_client:

            try:
                await resource_client.resource_groups.get(
                    resource_group_name=MEADOWRUN_RESOURCE_GROUP_NAME
                )
            except azure.core.exceptions.ResourceNotFoundError:
                print(
                    f"The meadowrun resource group ({MEADOWRUN_RESOURCE_GROUP_NAME}) "
                    f"doesn't exist, creating it now in {location}"
                )
                await resource_client.resource_groups.create_or_update(
                    MEADOWRUN_RESOURCE_GROUP_NAME, {"location": location}
                )

        _MEADOWRUN_RESOURCE_GROUP_ENSURED = True

    return MEADOWRUN_RESOURCE_GROUP_NAME


async def get_current_ip_address_on_vm() -> Optional[str]:
    """
    Assuming we're running on an Azure VM, get our current public ip address. If we're
    not running on an Azure VM, or we're running on an Azure VM without a public IP
    address, or if we're running on an Azure VM with a Standard SKU public IP address
    (see comments in _provision_nic_with_public_ip), we will return None.
    """
    async with aiohttp.request(
        "GET",
        "http://169.254.169.254/metadata/instance/network/interface/0/ipv4/ipAddress"
        "/0/publicIpAddress?api-version=2021-02-01&format=text",
        headers={"Metadata": "true"},
    ) as response:
        if not response.ok:
            return None

        return await response.text()


_MEADOWRUN_MANAGED_IDENTITY = "meadowrun-managed-identity"


async def _ensure_managed_identity(location: str) -> Tuple[str, str]:
    """Returns identity id, client id"""
    resource_group_name = await ensure_meadowrun_resource_group(location)
    async with get_credential_aio() as credential, ManagedServiceIdentityClient(
        credential, await get_subscription_id()
    ) as client:
        try:
            identity = await client.user_assigned_identities.get(
                resource_group_name, _MEADOWRUN_MANAGED_IDENTITY
            )
            return identity.id, identity.client_id
        except azure.core.exceptions.ResourceNotFoundError:
            print(
                f"Azure managed identity {_MEADOWRUN_MANAGED_IDENTITY} does not exist, "
                f"creating it now"
            )

            identity = await client.user_assigned_identities.create_or_update(
                resource_group_name, _MEADOWRUN_MANAGED_IDENTITY, {"location": location}
            )

            await assign_role_to_principal(
                "Contributor", identity.principal_id, location, "ServicePrincipal"
            )
            await assign_role_to_principal(
                "Key Vault Secrets User",
                identity.principal_id,
                location,
                "ServicePrincipal",
            )

            return identity.id, identity.client_id


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
    resource_group = await ensure_meadowrun_resource_group(location)

    # Bizarrely, setting principal_type to ServicePrincipal for newly created service
    # identities only seems to have an effect in 2018-09-01-preview (or later according
    # to the docs), but the role_definitions property is missing on 2018-09-01-preview
    # (and later) API versions. So it seems like we need to create two different clients
    # with different API verisons.
    async with get_credential_aio() as credential, AuthorizationManagementClient(
        credential, subscription_id, api_version="2018-01-01-preview"
    ) as client, AuthorizationManagementClient(
        credential, subscription_id, api_version="2018-09-01-preview"
    ) as client2:
        roles = [
            r
            async for r in client.role_definitions.list(
                scope=f"/subscriptions/{subscription_id}",
                filter=f"roleName eq '{role_name}'",
            )
        ]

        if len(roles) == 0:
            raise ValueError(f"Role {role_name} was not found")
        elif len(roles) > 1:
            raise ValueError(f"More than one role {role_name} was found")

        try:
            # https://docs.microsoft.com/en-us/python/api/azure-mgmt-authorization/azure.mgmt.authorization.v2020_10_01_preview.operations.roleassignmentsoperations?view=azure-python#azure-mgmt-authorization-v2020-10-01-preview-operations-roleassignmentsoperations-create
            parameters = {
                "role_definition_id": roles[0].id,
                "principal_id": principal_id,
            }
            if principal_type:
                parameters["principal_type"] = principal_type
            await client2.role_assignments.create(
                f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}",
                str(uuid.uuid4()),
                parameters,
            )
        except azure.core.exceptions.ResourceExistsError:
            # this means the role assignment already exists
            pass


async def delete_meadowrun_resource_group() -> None:
    """
    This should delete all meadowrun-generated resources as deletes everything in the
    meadowrun resource group.
    """
    async with get_credential_aio() as credential, ResourceManagementClient(
        credential, await get_subscription_id()
    ) as resource_client:
        try:
            poller = await resource_client.resource_groups.begin_delete(
                MEADOWRUN_RESOURCE_GROUP_NAME
            )
            await poller.result()
        except azure.core.exceptions.ResourceNotFoundError:
            pass


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


_STORAGE_ACCOUNT_NAME = None
_STORAGE_ACCOUNT_KEY = None
_EXISTING_TABLES: Set[str] = set()


async def ensure_table(
    table_name: str, location: str, on_missing: Literal["raise", "create"]
) -> TableClient:
    """
    Gets the TableClient for the specified table, creates it if it doesn't exist
    (depending on the value of the on_missing parameter). Multiple calls for the same
    table in the same process should be fast.
    """
    global _STORAGE_ACCOUNT_NAME, _STORAGE_ACCOUNT_KEY
    if _STORAGE_ACCOUNT_NAME is None or _STORAGE_ACCOUNT_KEY is None:
        # first, get the key to the storage account. If the storage account doesn't
        # exist, create it and then get the key
        (
            _STORAGE_ACCOUNT_NAME,
            _STORAGE_ACCOUNT_KEY,
        ) = await ensure_meadowrun_storage_account(location, "create")

    if table_name not in _EXISTING_TABLES:
        resource_group_name = await ensure_meadowrun_resource_group(location)

        async with get_credential_aio() as credential, StorageManagementClient(
            credential, await get_subscription_id()
        ) as client:
            # check if the table exists and create it if it doesn't
            try:
                await client.table.get(
                    resource_group_name, _STORAGE_ACCOUNT_NAME, table_name
                )
            except azure.core.exceptions.ResourceNotFoundError:
                if on_missing == "raise":
                    raise ValueError(
                        f"Table {_STORAGE_ACCOUNT_NAME}/{table_name} does not exist"
                    )
                elif on_missing == "create":
                    print(
                        f"Table {_STORAGE_ACCOUNT_NAME}/{table_name} does not exist, "
                        "creating it now"
                    )
                    await client.table.create(
                        resource_group_name, _STORAGE_ACCOUNT_NAME, table_name
                    )
                else:
                    raise ValueError(f"Unexpected value for on_missing {on_missing}")

            _EXISTING_TABLES.add(table_name)

    return TableClient(
        f"https://{_STORAGE_ACCOUNT_NAME}.table.core.windows.net/",
        table_name,
        credential=AzureNamedKeyCredential(_STORAGE_ACCOUNT_NAME, _STORAGE_ACCOUNT_KEY),
    )


async def record_last_used(
    resource_type: RESOURCE_TYPES_TYPE, resource_name: str, location: str
) -> None:
    try:
        async with await ensure_table(
            LAST_USED_TABLE_NAME, location, "create"
        ) as table_client:
            # no need to insert timestamp, that's always included in the Azure table
            # metadata
            await table_client.upsert_entity(
                {"PartitionKey": resource_type, "RowKey": resource_name}
            )
    except Exception as e:
        logging.error(
            f"Warning, failed when trying to record usage of {resource_name}",
            exc_info=e,
        )
