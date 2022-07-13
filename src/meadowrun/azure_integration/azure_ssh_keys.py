import asyncio
from typing import Tuple, Optional

import asyncssh
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from meadowrun.azure_integration.azure_meadowrun_core import (
    assign_role_to_principal,
    ensure_meadowrun_resource_group,
    get_subscription_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_identity import (
    get_current_user_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_paged,
    azure_rest_api_poll,
    get_tenant_id,
    wait_for_poll,
)
from meadowrun.ssh_keys import generate_rsa_private_key

_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME = "meadowrunSshPrivateKey"


def get_meadowrun_vault_name(subscription_id: str) -> str:
    # key vault names need to be globally unique. Also, they cannot be more than 24
    # characters long
    # TODO we need a way to manually set Key Vault name in case this ends up colliding
    return "mr" + subscription_id.replace("-", "")[-22:]


async def _ensure_meadowrun_vault(location: str) -> str:
    """
    Gets the meadowrun key vault URI if it exists. If it doesn't exist, also creates the
    meadowrun key vault, and tries to assign the Key Vault Administrator role to the
    current user.
    """

    subscription_id = await get_subscription_id()
    vault_name = get_meadowrun_vault_name(subscription_id)
    resource_group_path = await ensure_meadowrun_resource_group(location)
    vault_path = (
        f"{resource_group_path}/providers/Microsoft.KeyVault/vaults/{vault_name}"
    )

    try:
        vault = await azure_rest_api("GET", vault_path, "2019-09-01")
        return vault["properties"]["vaultUri"]
    except ResourceNotFoundError:
        # theoretically key_vault_client.vaults.get_deleted() should be faster,
        # but that requires specifying a location and there's no way to know what
        # location the key vault may have been originally created in.
        deleted_vault_found = False
        async for page in azure_rest_api_paged(
            "GET",
            f"/subscriptions/{subscription_id}/providers/Microsoft.KeyVault/"
            f"deletedVaults",
            "2019-09-01",
        ):
            for vault in page["value"]:
                if vault["name"] == vault_name:
                    deleted_vault_found = True
                    break

            if deleted_vault_found:
                break

        if deleted_vault_found:
            # if we have a deleted vault, then we should try to recover it
            create_mode = "recover"
            print(f"The meadowrun Key Vault {vault_name} was deleted, recovering")
        else:
            create_mode = "default"
            print(
                f"The meadowrun Key Vault {vault_name} does not exist, creating it "
                "now"
            )

        # if we're creating or recreating the Key Vault, assume that we need to add the
        # current user to the Key Vault Administrator role so that the current user can
        # access secrets.
        assign_role_task = asyncio.create_task(
            assign_role_to_principal(
                "Key Vault Administrator", await get_current_user_id(), location
            )
        )

        # Now we can create/recover the Key Vault.
        # https://docs.microsoft.com/en-us/rest/api/keyvault/keyvault/vaults/create-or-update#vaultproperties
        vault, _ = await wait_for_poll(
            await azure_rest_api_poll(
                "PUT",
                vault_path,
                "2019-09-01",
                "AsyncOperationJsonStatus",
                json_content={
                    "location": location,
                    "properties": {
                        "tenantId": await get_tenant_id(),
                        "sku": {"family": "A", "name": "Standard"},
                        "enableRbacAuthorization": True,
                        "createMode": create_mode,
                    },
                },
            )
        )

        try:
            await assign_role_task
        except Exception as e:
            print(
                "Warning: we were not able to assign the Key Vault Administrator role "
                f"to the current user. You may not be able to create/read secrets: {e}"
            )

        return vault["properties"]["vaultUri"]


_private_key: Optional[asyncssh.SSHKey] = None


async def ensure_meadowrun_key_pair(location: str) -> Tuple[asyncssh.SSHKey, str]:
    """
    Makes sure that a private key is stored in a Key Vault secret. If it does not exist,
    creates a fresh private key and uploads it.

    Returns (private key, public key). Private key is an asyncssh.SSHKey that can be
    passed to ssh.connect(private_key=pkey), public key is a string that can be passed
    into launch_vms.
    """
    global _private_key

    vault_uri = await _ensure_meadowrun_vault(location)

    if _private_key is None:
        try:
            result = await azure_rest_api(
                "GET",
                f"secrets/{_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME}",
                "7.3",
                base_url=vault_uri,
                token_scope="https://vault.azure.net/.default",
            )
            private_key_text = result["value"]
        except ResourceNotFoundError:
            try:
                # if we have a deleted secret, recover it, as we cannot add update a
                # deleted secret, we must recover it first. No need to check if the
                # deleted secret exists already, this call will just fail with
                # ResourceNotFoundError
                await azure_rest_api(
                    "POST",
                    f"deletedsecrets/{_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME}/recover",
                    "7.3",
                    base_url=vault_uri,
                    token_scope="https://vault.azure.net/.default",
                )
            except ResourceNotFoundError:
                pass

            print(
                "The Azure Key Vault secret that stores the meadowrun private key does "
                "not exist, creating it now"
            )

            private_key_text = generate_rsa_private_key()
            # create the private key secret
            # https://docs.microsoft.com/en-us/rest/api/keyvault/secrets/set-secret/set-secret
            await azure_rest_api(
                "PUT",
                f"secrets/{_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME}",
                "7.3",
                base_url=vault_uri,
                token_scope="https://vault.azure.net/.default",
                json_content={"value": private_key_text},
            )

        _private_key = asyncssh.import_private_key(private_key_text)

    rsa_key = _private_key.pyca_key
    if not isinstance(rsa_key, RSAPrivateKey):
        raise ValueError(
            "Private key text is bad, the resulting RSAKey.key is of type "
            f"{type(rsa_key)}"
        )
    return _private_key, rsa_key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    ).decode("utf-8")


async def download_ssh_key(output_path: str, location: str) -> None:
    """Downloads the meadowrun private SSH key to the specified file name"""
    vault_uri = await _ensure_meadowrun_vault(location)
    result = await azure_rest_api(
        "GET",
        f"secrets/{_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME}",
        "7.3",
        base_url=vault_uri,
        token_scope="https://vault.azure.net/.default",
    )
    key = result["value"]

    if key is None:
        raise ValueError("Azure SSH key secret's value was None")

    with open(output_path, "w", encoding="utf-8") as output_file:
        output_file.write(key)
