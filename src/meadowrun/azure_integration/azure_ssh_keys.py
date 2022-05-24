import asyncio
import io
from typing import Tuple, Optional, cast

import azure.core.exceptions
import paramiko
from azure.keyvault.secrets.aio import SecretClient
from azure.mgmt.keyvault.aio import KeyVaultManagementClient
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from meadowrun.azure_integration.azure_core import (
    assign_role_to_principal,
    ensure_meadowrun_resource_group,
    get_current_user_id,
    get_subscription_id,
    get_tenant_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    get_credential_aio,
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
    resource_group_name = await ensure_meadowrun_resource_group(location)

    async with get_credential_aio() as credential, KeyVaultManagementClient(
        credential, subscription_id
    ) as key_vault_client:
        try:
            vault = await key_vault_client.vaults.get(resource_group_name, vault_name)
            return vault.properties.vault_uri
        except azure.core.exceptions.ResourceNotFoundError:
            # theoretically key_vault_client.vaults.get_deleted() should be faster,
            # but that requires specifying a location and there's no way to know what
            # location the key vault may have been originally created in.
            deleted_vault = [
                vault
                async for vault in key_vault_client.vaults.list_deleted()
                if vault.name == vault_name
            ]
            if any(deleted_vault):
                # if we have a deleted vault, then we should try to recover it
                create_mode = "recover"
                assign_role_task = None
                print(f"The meadowrun Key Vault {vault_name} was deleted, recovering")
            else:
                create_mode = "default"
                # if we're creating the Key Vault for the first time, assume that we
                # need to add the current user to the Key Vault Administrator role so
                # that the current user can access secrets.
                assign_role_task = asyncio.create_task(
                    assign_role_to_principal(
                        "Key Vault Administrator", get_current_user_id(), location
                    )
                )
                print(
                    f"The meadowrun Key Vault {vault_name} does not exist, creating it "
                    "now"
                )

            # Now we can create/recover the Key Vault.
            # https://docs.microsoft.com/en-us/python/api/azure-mgmt-keyvault/azure.mgmt.keyvault.v2021_06_01_preview.operations.vaultsoperations?view=azure-python#azure-mgmt-keyvault-v2021-06-01-preview-operations-vaultsoperations-begin-create-or-update
            poller = await key_vault_client.vaults.begin_create_or_update(
                resource_group_name,
                vault_name,
                {
                    "location": location,
                    "properties": {
                        "tenant_id": await get_tenant_id(),
                        "sku": {"family": "A", "name": "Standard"},
                        "enable_rbac_authorization": True,
                        "create_mode": create_mode,
                    },
                },
            )
            vault = await poller.result()

            if assign_role_task:
                try:
                    await assign_role_task
                except Exception as e:
                    print(
                        "Warning: we were not able to assign the Key Vault "
                        "Administrator role to the current user. You may not be able to"
                        f" create/read secrets: {e}"
                    )

            return vault.properties.vault_uri


_private_key: Optional[paramiko.RSAKey] = None


async def ensure_meadowrun_key_pair(location: str) -> Tuple[paramiko.PKey, str]:
    """
    Makes sure that a private key is stored in a Key Vault secret. If it does not exist,
    creates a fresh private key and uploads it.

    Returns (private key, public key). Private key is a paramiko.PKey that can be passed
    to paramiko.connect(pkey=pkey), public key is a string that can be passed into
    launch_vms.
    """
    global _private_key

    vault_uri = await _ensure_meadowrun_vault(location)

    if _private_key is None:
        async with get_credential_aio() as credential, SecretClient(
            vault_uri, credential
        ) as secret_client:
            # for some reason, reveal_type(secret_client) returns
            # AsyncKeyVaultClientBase which is the super class of SecretClient. This is
            # either a problem with the azure SDK type hints or a bug in mypy
            secret_client = cast(SecretClient, secret_client)

            try:
                private_key_text = (
                    await secret_client.get_secret(
                        _MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME
                    )
                ).value
            except azure.core.exceptions.ResourceNotFoundError:
                try:
                    await secret_client.get_deleted_secret(
                        _MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME
                    )
                    # if we have a deleted secret, recover it, as we cannot add update a
                    # deleted secret, we must recover it first
                    await secret_client.recover_deleted_secret(
                        _MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME
                    )
                except azure.core.exceptions.ResourceNotFoundError:
                    pass

                print(
                    "The Azure Key Vault secret that stores the meadowrun private key "
                    "does not exist, creating it now"
                )

                private_key_text = generate_rsa_private_key()
                # create the private key secret
                await secret_client.set_secret(
                    _MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME, private_key_text
                )

        with io.StringIO(private_key_text) as s:
            _private_key = paramiko.RSAKey(file_obj=s)

    rsa_key = _private_key.key
    if not isinstance(rsa_key, RSAPrivateKey):
        raise ValueError(
            "Private key text is bad, the resulting RSAKey.key is of type "
            f"{type(rsa_key)}"
        )
    return _private_key, rsa_key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    ).decode("utf-8")


async def download_ssh_key(output_path: str, file_name: str) -> None:
    """Downloads the meadowrun private SSH key to the specified file name"""
    vault_uri = await _ensure_meadowrun_vault(file_name)
    async with get_credential_aio() as credential, SecretClient(
        vault_uri, credential
    ) as secret_client:
        # for some reason, reveal_type(secret_client) returns
        # AsyncKeyVaultClientBase which is the super class of SecretClient. This is
        # either a problem with the azure SDK type hints or a bug in mypy
        secret_client = cast(SecretClient, secret_client)

        key = (
            await secret_client.get_secret(_MEADOWRUN_PRIVATE_SSH_KEY_SECRET_NAME)
        ).value

    if key is None:
        raise ValueError("Azure SSH key secret's value was None")

    with open(output_path, "w") as output_file:
        output_file.write(key)
