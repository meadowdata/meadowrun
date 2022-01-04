import asyncio

import meadowgrid.coordinator_main
import meadowgrid.agent_main
from meadowgrid import grid_map
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientSync
from meadowgrid.credentials import CredentialsSource
from meadowgrid.docker_controller import delete_images_from_repository
from meadowgrid.meadowgrid_pb2 import (
    AwsSecret,
    ContainerAtTag,
    Credentials,
    GitRepoBranch,
    ServerAvailableFile,
    ServerAvailableInterpreter,
)
from test_meadowgrid.test_meadowgrid_basics import (
    TEST_WORKING_FOLDER,
    wait_for_agents_sync,
)

_PRIVATE_DOCKER_REPOSITORY = "hrichardlee/test1"


def manual_test_docker_credentials_file():
    r"""
    This is a manual test because it requires out-of-band steps to set up.

    1. Create an account on DockerHub if you don't have one already. Replace
       "hrichardlee" in _PRIVATE_DOCKER_REPOSITORY with your username

    2. Put your credentials in C:\temp\dockerhub_credentials.txt, username on first
       line, password on second line

    3. Create a docker image that has meadowdata in it:
       > build_docker_image.bat

    4. In DockerHub, create a private repository called test1. Then, upload our new
       image:
       > docker tag meadowdata <username>/test1
       > docker login
       > docker push <username>/test1

    5. Delete the local copy of the image and log out of docker
       > docker image rm <username>/test1
       > docker image rm <username>/test1@<digest>
       > docker image rm meadowdata
       > docker logout

    6. Now run this test
    """
    _manual_test_docker_credentials(
        ServerAvailableFile(
            credentials_type=Credentials.Type.USERNAME_PASSWORD,
            path=r"C:\temp\dockerhub_credentials.txt",
        )
    )


def manual_test_docker_credentials_aws_secret():
    """
    Follow the same steps as in manual_test_docker_credentials_file, but just a
    different step 2:

    2. Create an AWS Secret with the name/id "dockerhub". It should have a username key
       and a password key populated with your Dockerhub credentials. Make sure the
       machine you're running the test under has access to the secret you've created. If
       you've installed the AWS CLI, `aws secretsmanager get-secret-value --secret-id
       dockerhub` should work.
    """
    _manual_test_docker_credentials(
        AwsSecret(
            credentials_type=Credentials.Type.USERNAME_PASSWORD, secret_name="dockerhub"
        )
    )


def _manual_test_docker_credentials(credentials_source: CredentialsSource) -> None:
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        asyncio.run(delete_images_from_repository(_PRIVATE_DOCKER_REPOSITORY))

        with MeadowGridCoordinatorClientSync() as coordinator_client:
            coordinator_client.add_credentials(
                "DOCKER", "registry-1.docker.io", credentials_source
            )
            wait_for_agents_sync(coordinator_client, 1)

        grid_map(
            lambda x: x * 2,
            [1, 2, 3],
            ContainerAtTag(repository=_PRIVATE_DOCKER_REPOSITORY, tag="latest"),
            None,
        )


_PRIVATE_GIT_REPOSITORY = "git@github.com:hrichardlee/test_repo.git"


def manual_test_git_ssh_key_file():
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
       getting the private key through meadowdata's credentials system rather than just
       picking up the default keys in ~/.ssh. For the code below, enter C:\temp\key as
       the location for the key

    4. Now, add this key's public key as a deploy key in Github. Go to your test_repo >
       Settings > Deploy keys > Add deploy key. The title can be anything (e.g.
       "meadowdata test") and then copy the public key from C:\temp\key.pub and click
       "Add key"

    5. (Optional) Now test that you can NOT clone this repo without this key, e.g. `git
       clone git@github.com/<username>/test_repo.git` fails. And test that you CAN clone
       this repo with the key: `set GIT_SSH_COMMAND=ssh -i c:\\temp\\key` and then `git
       clone git@github.com/<username>/test_repo.git` should work

    6. Now run this test
    """
    _manual_test_git_ssh_key(
        ServerAvailableFile(
            credentials_type=Credentials.Type.SSH_KEY, path=r"C:\temp\key"
        )
    )


def manual_test_git_ssh_key_aws_secret():
    r"""
    Follow the same steps as in manual_test_git_ssh_key_file but just an additional
    step before 6:

    5.1: Create an AWS Secret with the name/id meadowdata_test_ssh_key. It should have a
         private_key field with the contents of your c:\temp\key file that you generated
         in step 3. Unfortunately, AWS Secrets Manager doesn't provide a smooth
         experience for storing multi-line secrets, but it is possible--you will need to
         manually replace your newlines with \n--json requires that newlines are escaped
         within strings. E.g. the "value" in the UI should look like "first line\nsecond
         line\netc."
    """
    _manual_test_git_ssh_key(
        AwsSecret(
            credentials_type=Credentials.Type.SSH_KEY,
            secret_name=r"meadowdata_test_ssh_key",
        )
    )


def _manual_test_git_ssh_key(credentials_source: CredentialsSource) -> None:
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        with MeadowGridCoordinatorClientSync() as coordinator_client:
            coordinator_client.add_credentials(
                "GIT", "git@github.com", credentials_source
            )
            wait_for_agents_sync(coordinator_client, 1)

        # make this a nested function so that it gets pickled as code rather than as a
        # reference
        def test_function(x):
            import example_package.example  # type: ignore[import]

            return example_package.example.join_strings("hello ", str(x))

        grid_map(
            test_function,
            [1, 2, 3],
            ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
            GitRepoBranch(repo_url=_PRIVATE_GIT_REPOSITORY, branch="main"),
        )
