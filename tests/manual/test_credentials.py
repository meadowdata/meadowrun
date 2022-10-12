"""
These tests require extensive manual setup, e.g. creating a DockerHub account and
uploading an image to it.
"""

import subprocess

from meadowrun import run_function, Deployment, KubernetesSecret
from automated.test_kubernetes_automated import _kubernetes_host
from meadowrun.azure_integration.azure_meadowrun_core import get_subscription_id
from meadowrun.azure_integration.azure_ssh_keys import get_meadowrun_vault_name
from meadowrun.credentials import CredentialsSource, CredentialsSourceForService
from meadowrun.docker_controller import delete_images_from_repository
from meadowrun.meadowrun_pb2 import (
    AwsSecretProto,
    AzureSecretProto,
    ContainerAtTag,
    Credentials,
    GitRepoBranch,
    ServerAvailableFile,
)
from automated.test_local_automated import LocalHost

_PRIVATE_DOCKER_REPOSITORY = "hrichardlee/test1"


async def manual_test_docker_credentials_file() -> None:
    r"""
    This is a manual test because it requires out-of-band steps to set up.

    1. Create an account on DockerHub if you don't have one already. Replace
       "hrichardlee" in _PRIVATE_DOCKER_REPOSITORY with your username

    2. Put your credentials in C:\temp\dockerhub_credentials.txt, username on first
       line, password on second line

    3. Create a docker image that has meadowrun in it:
       > build_docker_image.bat

    4. In DockerHub, create a private repository called test1. Then, upload our new
       image:
       > docker tag meadowrun <username>/test1
       > docker login
       > docker push <username>/test1

    5. Now run this test
    """
    await _manual_test_docker_credentials(
        ServerAvailableFile(
            credentials_type=Credentials.Type.USERNAME_PASSWORD,
            path=r"C:\temp\dockerhub_credentials.txt",
        )
    )


async def manual_test_docker_credentials_aws_secret() -> None:
    """
    Follow the same steps as in manual_test_docker_credentials_file, but just a
    different step 2:

    2. Create an AWS Secret with the name/id "dockerhub". It should have a username key
       and a password key populated with your Dockerhub credentials. Make sure the
       machine you're running the test under has access to the secret you've created. If
       you've installed the AWS CLI, `aws secretsmanager get-secret-value --secret-id
       dockerhub` should work.
    """
    await _manual_test_docker_credentials(
        AwsSecretProto(
            credentials_type=Credentials.Type.USERNAME_PASSWORD, secret_name="dockerhub"
        )
    )


async def manual_test_docker_credentials_azure_secret() -> None:
    """
    Follow the same steps as in manual_test_docker_credentials_file, but just a
    different step 2:

    2. Create an Azure Secret in the meadowrun-generated Vault with name "dockerhub". It
        should have a username key and a password key populated with your Dockerhub
        credentials. Make sure the machine you're running the test under has access to
        the secret you've created. If you've installed the Azure CLI, `az keyvault
        secret show --name dockerhub --vault-name <mr...>` should work.
    """
    await _manual_test_docker_credentials(
        AzureSecretProto(
            credentials_type=Credentials.Type.USERNAME_PASSWORD,
            vault_name=get_meadowrun_vault_name(await get_subscription_id()),
            secret_name="dockerhub",
        )
    )


async def _manual_test_docker_credentials(
    credentials_source: CredentialsSource,
) -> None:
    await delete_images_from_repository(_PRIVATE_DOCKER_REPOSITORY)
    # doesn't seem like there's an API for this, so we just have to use the command
    # line
    subprocess.run("docker logout", check=True)

    await run_function(
        lambda: 2 + 2,
        LocalHost(),
        deployment=Deployment(
            ContainerAtTag(repository=_PRIVATE_DOCKER_REPOSITORY, tag="latest"),
            credentials_sources=[
                CredentialsSourceForService(
                    "DOCKER", "registry-1.docker.io", credentials_source
                )
            ],
        ),
    )


_PRIVATE_GIT_REPOSITORY = "git@github.com:hrichardlee/test_repo.git"


async def manual_test_git_ssh_key_file() -> None:
    r"""
    This is a manual test because it requires out-of-band steps to set up.

    1. Create an account on Github if you don't have one already. Replace "hrichardlee"
       in _PRIVATE_GIT_REPOSITORY above with your username

    2. Create a repo called test_repo, and make sure to make it private. Github has the
       option to import code from another repository--click Import code and enter
       "https://github.com/meadowdata/test_repo.git" as the clone URL. (We can't do a
       fork because Github does not let you make a private fork of a public repo)

    3. Now, let's create some SSH credentials by running `ssh-keygen -t ed25519`. When
       it prompts you for where to save it, do NOT save it to the default location,
       instead save it to a folder that is NOT ~/.ssh so that we can test that we are
       getting the private key through meadowrun's credentials system rather than just
       picking up the default keys in ~/.ssh. For the code below, enter C:\temp\key as
       the location for the key

    4. Now, add this key's public key as a deploy key in Github. Go to your test_repo >
       Settings > Deploy keys > Add deploy key. The title can be anything (e.g.
       "meadowrun test") and then copy the public key from C:\temp\key.pub and click
       "Add key"

    5. (Optional) Now test that you can NOT clone this repo without this key, e.g. `git
       clone git@github.com/<username>/test_repo.git` fails. And test that you CAN clone
       this repo with the key: `set GIT_SSH_COMMAND=ssh -i c:\\temp\\key` and then `git
       clone git@github.com/<username>/test_repo.git` should work

    6. Now run this test
    """
    await _manual_test_git_ssh_key(
        ServerAvailableFile(
            credentials_type=Credentials.Type.SSH_KEY, path=r"C:\temp\key"
        )
    )


async def manual_test_git_ssh_key_aws_secret() -> None:
    r"""
    Follow the same steps as in manual_test_git_ssh_key_file but just an additional
    step before 6:

    5.1: Create an AWS Secret with the name/id meadowrun_test_ssh_key. It should have a
         private_key field with the contents of your c:\temp\key file that you generated
         in step 3. Unfortunately, AWS Secrets Manager doesn't provide a smooth
         experience for storing multi-line secrets, but it is possible--you will need to
         manually replace your newlines with \n--json requires that newlines are escaped
         within strings. E.g. the "value" in the UI should look like "first line\nsecond
         line\netc."
    """
    await _manual_test_git_ssh_key(
        AwsSecretProto(
            credentials_type=Credentials.Type.SSH_KEY,
            secret_name=r"meadowrun_test_ssh_key",
        )
    )


async def manual_test_git_ssh_key_azure_secret() -> None:
    r"""
    Follow the same steps as in manual_test_git_ssh_key_file but just an additional
    step before 6:

    5.1: Create an Azure Secret with the name/id meadowrun_test_ssh_key. It should
        have the contents of your c:\temp\key file that you generated in step 3. See
        https://docs.microsoft.com/en-in/azure/key-vault/secrets/multiline-secrets for
        instructions on how to add a multi-line value to an Azure secret
    """
    await _manual_test_git_ssh_key(
        AzureSecretProto(
            credentials_type=Credentials.Type.SSH_KEY,
            vault_name=get_meadowrun_vault_name(await get_subscription_id()),
            secret_name=r"meadowrun-test-ssh-key",
        )
    )


async def _manual_test_git_ssh_key(credentials_source: CredentialsSource) -> None:
    # make this a nested function so that it gets pickled as code rather than as a
    # reference
    def test_function() -> str:
        import example_package.example  # type: ignore[import]

        return example_package.example.join_strings("hello ", "there")

    await run_function(
        test_function,
        LocalHost(),
        deployment=Deployment(
            code=GitRepoBranch(repo_url=_PRIVATE_GIT_REPOSITORY, branch="main"),
            credentials_sources=[
                CredentialsSourceForService("GIT", "git@github.com", credentials_source)
            ],
        ),
    )


async def manual_test_kubernetes_container() -> None:
    """
    Running this tests requires the same setup as TestKubernetes, plus following the
    instructions at docs/how_to/private_container_kubernetes.md, and replacing the names
    of your container image and secret below.
    """
    result = await run_function(
        lambda: 2 * 2,
        _kubernetes_host(),
        deployment=Deployment.container_image(
            "hrichardlee/test1",
            username_password_secret=KubernetesSecret("mydockersecret"),
        ),
    )

    assert result == 4
