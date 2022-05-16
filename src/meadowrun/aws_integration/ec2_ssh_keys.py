import io

import boto3
import paramiko

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)

MEADOWRUN_KEY_PAIR_NAME = "meadowrunKeyPair"
_MEADOWRUN_KEY_PAIR_SECRET_NAME = "meadowrunKeyPairPrivateKey"


def ensure_meadowrun_key_pair(region_name: str) -> paramiko.PKey:
    """
    On first run, creates an EC2 key pair and stores the private key in an AWS secret,
    and returns a paramiko.PKey that is compatible with that EC2 key pair and can be
    passed to paramiko.connect(pkey=pkey).

    On subsequent runs, this will return the same paramiko.PKey using the AWS secret.

    TODO should we cache the key locally so that we're not requesting the secret
    constantly?
    """

    ec2_client = boto3.client("ec2", region_name=region_name)
    secrets_client = boto3.client("secretsmanager", region_name=region_name)

    private_key_text = None

    # first figure out the state of the secret, and if it has been deleted, restore it

    success1, inner_result = ignore_boto3_error_code(
        lambda: ignore_boto3_error_code(
            lambda: secrets_client.get_secret_value(
                SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME
            ),
            "ResourceNotFoundException",
        ),
        "InvalidRequestException",
    )

    if not success1:
        # this means we got an InvalidRequestException, which we assume means the secret
        # is marked for deletion, so we need to restore it then overwrite it
        secrets_client.restore_secret(SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME)
        secret_state = "restored"
    else:
        assert inner_result is not None  # just for mypy
        success2, secret_value_result = inner_result
        if not success2:
            # this means the secret doesn't exist
            secret_state = "does not exist"
        else:
            secret_state = "exists"
            assert secret_value_result is not None  # just for mypy
            private_key_text = secret_value_result["SecretString"]

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

    assert private_key_text is not None  # just for mypy, logic above guarantees this

    with io.StringIO(private_key_text) as s:
        return paramiko.RSAKey(file_obj=s)


def download_ssh_key(output_path: str, region_name: str) -> None:
    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    key = secrets_client.get_secret_value(SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME)[
        "SecretString"
    ]
    with open(output_path, "w") as output_file:
        output_file.write(key)
