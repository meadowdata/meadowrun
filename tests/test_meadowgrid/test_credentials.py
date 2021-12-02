import asyncio

import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid import grid_map
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientSync
from meadowgrid.credentials import CredentialsSource
from meadowgrid.docker_controller import delete_images_from_repository
from meadowgrid.meadowgrid_pb2 import ServerAvailableFile, ContainerAtTag, AwsSecret
from test_meadowgrid.test_meadowgrid_basics import TEST_WORKING_FOLDER

_PRIVATE_REPOSITORY = "hrichardlee/test1"


def manual_test_docker_credentials_file():
    """
    This is a manual test because it requires out-of-band steps to set up.

    1. Create an account on DockerHub if you don't have one already. Replace
       "hrichardlee" in _PRIVATE_REPOSITORY with your username

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
        ServerAvailableFile(path=r"C:\temp\dockerhub_credentials.txt")
    )


def manual_test_docker_credentials_aws_secret():
    """
    Follow the same steps as in manual_test_docker_credentials_file, but just a
    different step 2:

    2. Create an AWS Secret with the name "dockerhub". It should have a username key and
       a password key populated with your Dockerhub credentials. Make sure the machine
       you're running the test under has access to the secret you've created. If you've
       installed the AWS CLI, `aws secretsmanager get-secret-value --secret-id
       dockerhub` should work.
    """
    _manual_test_docker_credentials(AwsSecret(secret_name="dockerhub"))


def _manual_test_docker_credentials(credentials_source: CredentialsSource) -> None:
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        asyncio.run(delete_images_from_repository(_PRIVATE_REPOSITORY))

        with MeadowGridCoordinatorClientSync() as client:
            client.add_credentials("DOCKER", "registry-1.docker.io", credentials_source)

        grid_map(
            lambda x: x * 2,
            [1, 2, 3],
            ContainerAtTag(repository=_PRIVATE_REPOSITORY, tag="latest"),
            None,
        )
