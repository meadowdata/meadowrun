"""See ecr.py"""
from typing import Optional

from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_resource_group,
    get_default_location,
    get_subscription_id,
    record_last_used,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_acr import (
    get_acr_token,
    get_tags_in_repository,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_poll,
    wait_for_poll,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    CONTAINER_IMAGE,
    meadowrun_container_registry_name,
)
from meadowrun.credentials import UsernamePassword
from meadowrun.run_job_core import ContainerRegistryHelper


async def _ensure_meadowrun_registry(registry_name: str, location: str) -> str:
    """
    Returns the login server for the meadowrun-managed Azure container registry (after
    creating it if it doesn't exist)
    """
    container_registry_path = (
        f"{await ensure_meadowrun_resource_group(location)}/providers/"
        f"Microsoft.ContainerRegistry/registries/{registry_name}"
    )

    try:
        await azure_rest_api("GET", container_registry_path, "2021-09-01")
    except ResourceNotFoundError:
        print("meadowrun container registry does not exist, creating it now")
        await wait_for_poll(
            await azure_rest_api_poll(
                "PUT",
                container_registry_path,
                "2021-09-01",
                "AsyncOperationJsonStatus",
                json_content={
                    "location": location,
                    # sets performance characteristics
                    "sku": {"name": "Basic"},
                },
            )
        )

    return f"{registry_name}.azurecr.io"


_EMPTY_GUID = "00000000-0000-0000-0000-000000000000"


async def _get_username_password(registry_name: str) -> UsernamePassword:
    """
    Gets the UsernamePassword for logging into the meadowrun-managed Azure container
    registry. Can be passed to functions in docker_controller.py
    """
    return UsernamePassword(_EMPTY_GUID, await get_acr_token(registry_name, "refresh"))


async def _does_image_exist(registry_name: str, repository: str, tag: str) -> bool:
    try:
        return any(
            existing_tag["name"] == tag
            for existing_tag in await get_tags_in_repository(registry_name, repository)
        )
    except ResourceNotFoundError:
        # this means the repository doesn't exist, which is fine
        return False


async def get_acr_helper(
    repository: str, tag: str, location: str
) -> ContainerRegistryHelper:
    """This function is tightly coupled with compile_environment_spec_to_container"""
    if location == "default":
        location = get_default_location()

    subscription_id = await get_subscription_id()
    registry_name = meadowrun_container_registry_name(subscription_id)

    repository_prefix = await _ensure_meadowrun_registry(registry_name, location)

    await record_last_used(CONTAINER_IMAGE, tag, location)

    return ContainerRegistryHelper(
        True,
        await _get_username_password(registry_name),
        f"{repository_prefix}/{repository}:{tag}",
        await _does_image_exist(registry_name, repository, tag),
    )


async def get_acr_username_password(registry_name: str) -> Optional[UsernamePassword]:
    """Gets the username/password if registry_domain is an ACR repo"""
    if registry_name.lower().endswith(".azurecr.io"):
        return await _get_username_password(registry_name)

    return None
