import asyncssh

import boto3

from meadowrun.aws_integration.aws_core import wrap_access_or_install_errors
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)

MEADOWRUN_KEY_PAIR_NAME = "meadowrun_key_pair"
_MEADOWRUN_KEY_PAIR_SECRET_NAME = "meadowrun_private_key"


def ensure_meadowrun_key_pair(region_name: str) -> None:
    """
    Ensures that an EC2 key pair and corresponding private key in an AWS secret exist.
    If both exist, they will not be modified. If either do not exist, both will be
    recreated.
    """

    ec2_client = boto3.client("ec2", region_name=region_name)
    secrets_client = boto3.client("secretsmanager", region_name=region_name)

    # first figure out the state of the secret, and if it has been deleted, restore it

    success, secret_value_result, error_code = ignore_boto3_error_code(
        lambda: secrets_client.get_secret_value(
            SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME
        ),
        {
            "ResourceNotFoundException",
            "InvalidRequestException",
        },
        True,
    )

    if success:
        secret_state = "exists"
    elif error_code == "InvalidRequestException":
        # this means the secret is marked for deletion, so we need to restore it then
        # overwrite it
        secrets_client.restore_secret(SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME)
        secret_state = "restored"
    elif error_code == "ResourceNotFoundException":
        # this means the secret doesn't exist
        secret_state = "does not exist"
    else:
        raise ValueError(
            f"success was not True, but error_code was an unexpected value {error_code}"
        )

    # next figure out the state of the key pair

    key_pair_exists, _ = ignore_boto3_error_code(
        lambda: ec2_client.describe_key_pairs(KeyNames=[MEADOWRUN_KEY_PAIR_NAME]),
        "InvalidKeyPair.NotFound",
    )

    if secret_state in ("restored", "does not exist") or not key_pair_exists:
        ec2_client.delete_key_pair(KeyName=MEADOWRUN_KEY_PAIR_NAME)
        create_key_pair_result = ec2_client.create_key_pair(
            KeyName=MEADOWRUN_KEY_PAIR_NAME, KeyType="rsa"
        )
        private_key_text = create_key_pair_result["KeyMaterial"]
        if secret_state == "does not exist":
            secrets_client.create_secret(
                Name=_MEADOWRUN_KEY_PAIR_SECRET_NAME, SecretString=private_key_text
            )
        else:  # restored or exists
            secrets_client.put_secret_value(
                SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME, SecretString=private_key_text
            )


def _get_meadowrun_ssh_key_text(region_name: str) -> str:
    # TODO should we cache the key locally so that we're not requesting the secret
    # constantly?
    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    secret_result = wrap_access_or_install_errors(
        lambda: secrets_client.get_secret_value(
            SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME
        ),
        f"secret {_MEADOWRUN_KEY_PAIR_SECRET_NAME}",
        "AccessDeniedException",
        # InvalidRequestException actually means this was deleted
        {"ResourceNotFoundException", "InvalidRequestException"},
    )
    return secret_result["SecretString"]


def get_meadowrun_ssh_key(region_name: str) -> asyncssh.SSHKey:
    """
    Returns an asyncssh.SSHKey that is compatible with that EC2 key pair and can be
    passed to ssh.connect(private_key=pkey).
    """
    private_key_text = _get_meadowrun_ssh_key_text(region_name)
    return asyncssh.import_private_key(private_key_text)


def download_ssh_key(output_path: str, region_name: str) -> None:
    private_key_text = _get_meadowrun_ssh_key_text(region_name)
    with open(output_path, "w", encoding="utf-8") as output_file:
        output_file.write(private_key_text)
