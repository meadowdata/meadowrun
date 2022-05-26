"""See ecr.py"""

import urllib.parse
from typing import Tuple

import aiohttp
import azure.core.exceptions
from azure.containerregistry.aio import ContainerRegistryClient
from azure.mgmt.containerregistry.aio import ContainerRegistryManagementClient

from meadowrun.azure_integration.azure_core import (
    ensure_meadowrun_resource_group,
    get_default_location,
    get_subscription_id,
    get_tenant_id,
    record_last_used,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    CONTAINER_IMAGE,
    get_credential_aio,
    meadowrun_registry_name,
)
from meadowrun.credentials import UsernamePassword
from meadowrun.run_job_core import ContainerRegistryHelper


async def _ensure_meadowrun_registry(registry_name: str, location: str) -> str:
    """
    Returns the login server for the meadowrun-managed Azure container registry (after
    creating it if it doesn't exist)
    """
    resource_group_name = await ensure_meadowrun_resource_group(location)
    async with ContainerRegistryManagementClient(
        get_credential_aio(), await get_subscription_id()
    ) as client:
        # https://docs.microsoft.com/en-us/python/api/azure-mgmt-containerregistry/azure.mgmt.containerregistry.v2021_12_01_preview.aio.operations.registriesoperations?view=azure-python#azure-mgmt-containerregistry-v2021-12-01-preview-aio-operations-registriesoperations-begin-create
        poller = await client.registries.begin_create(
            resource_group_name,
            registry_name,
            {
                "location": location,
                # sets performance characteristics
                "sku": {"name": "Basic"},
            },
        )
        await poller.result()

    return f"{registry_name}.azurecr.io"


_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


async def _get_username_password(registry_name: str) -> UsernamePassword:
    """
    Gets the UsernamePassword for logging into the meadowrun-managed Azure container
    registry. Can be passed to functions in docker_controller.py
    """

    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L115
    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli-core/azure/cli/core/_profile.py#L349
    # https://github.com/Azure/azure-cli/blob/02cb00efbf661a1c18bab4a1fe03f2b1c59e30ef/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L365
    # https://github.com/Azure/azure-cli/blob/02cb00efbf661a1c18bab4a1fe03f2b1c59e30ef/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L403

    realm, service = await _get_auth_parameters(registry_name)
    realm_url = urllib.parse.urlparse(realm)
    auth_host = urllib.parse.urlunparse(
        (realm_url[0], realm_url[1], "/oauth2/exchange", "", "", "")
    )

    async with get_credential_aio() as credential:
        token = (
            await credential.get_token("https://management.azure.com/.default")
        ).token

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    content = {
        "grant_type": "access_token",
        "service": service,
        "tenant": await get_tenant_id(),
        "access_token": token,
    }

    async with aiohttp.request(
        "POST", auth_host, data=urllib.parse.urlencode(content), headers=headers
    ) as response:
        return UsernamePassword(_EMPTY_GUID, (await response.json())["refresh_token"])


async def _get_auth_parameters(registry_name: str) -> Tuple[str, str]:
    """Returns realm, service"""

    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L63

    # technically should read this off of the login_server property of the registry
    async with aiohttp.request(
        "GET", f"https://{registry_name}.azurecr.io/v2/"
    ) as response:
        if response.status != 401:

            raise ValueError(
                f"Unexpected response from ACR login server, status {response.status}"
            )
        if "WWW-Authenticate" not in response.headers:
            raise ValueError(
                "Unexpected response from ACR login server, no WWW-Authenticate header"
            )

        authenticate = response.headers["WWW-Authenticate"]

    auth_type, sep, auth_parameters = authenticate.partition(" ")
    if sep != " ":
        raise ValueError(
            "Unexpected response from ACR login server, WWW-Authenticate header has"
            " no space"
        )
    if auth_type.lower() != "bearer":
        raise ValueError(
            "Unexpected response from ACR login server, WWW-Authenticate header "
            f"does not start with Bearer, instead starts with {auth_type}"
        )
    auth_parameters_dict = {}
    for pair in auth_parameters.split(","):
        key, sep, value = pair.partition("=")
        if sep != "=":
            raise ValueError(
                "Unexpected response from ACR login server. Auth parameters were "
                f"not in the expected format: {pair}"
            )
        auth_parameters_dict[key] = value.strip('"')

    if "realm" not in auth_parameters or "service" not in auth_parameters:
        raise ValueError(
            "Unexpected response from ACR login server, either realm or service "
            "auth parameter expected"
        )

    return auth_parameters_dict["realm"], auth_parameters_dict["service"]


async def _does_image_exist(registry_name: str, repository: str, tag: str) -> bool:
    async with ContainerRegistryClient(
        f"{registry_name}.azurecr.io",
        get_credential_aio(),
        audience="https://management.azure.com",
    ) as client:
        try:
            async for existing_tag in client.list_tag_properties(repository):
                if existing_tag.name == tag:
                    return True
        except azure.core.exceptions.ResourceNotFoundError:
            # this means the repository doesn't exist, which is fine
            return False

    return False


async def get_acr_helper(
    repository: str, tag: str, location: str
) -> ContainerRegistryHelper:
    """This function is tightly coupled with compile_environment_spec_to_container"""
    if location == "default":
        location = get_default_location()

    subscription_id = await get_subscription_id()
    registry_name = meadowrun_registry_name(subscription_id)

    repository_prefix = await _ensure_meadowrun_registry(registry_name, location)

    await record_last_used(CONTAINER_IMAGE, tag, location)

    return ContainerRegistryHelper(
        True,
        await _get_username_password(registry_name),
        f"{repository_prefix}/{repository}:{tag}",
        await _does_image_exist(registry_name, repository, tag),
    )
