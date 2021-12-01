import asyncio

import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid import grid_map
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientSync
from meadowgrid.docker_controller import delete_image
from meadowgrid.meadowgrid_pb2 import ServerAvailableFile, ContainerAtDigest, AwsSecret
from test_meadowgrid.test_meadowgrid_basics import TEST_WORKING_FOLDER

_PRIVATE_REPOSITORY = "hrichardlee/test1"
_PRIVATE_DIGEST = (
    "sha256:a6848cf6d57039650b4a6062aea53f8259b0de60e55b8d61e3f5de2e7d51c591"
)


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

    5. Replace the digest in _PRIVATE_DIGEST with the actual digest of this image using
       > docker images --digests

    6. Delete the local copy of the image and log out of docker
       > docker image rm <username>/test1
       > docker image rm <username>/test1@<digest>
       > docker image rm meadowdata
       > docker logout

    7. Now run the test below
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        asyncio.run(delete_image(f"{_PRIVATE_REPOSITORY}@{_PRIVATE_DIGEST}"))

        with MeadowGridCoordinatorClientSync() as client:
            client.add_credentials(
                "DOCKER",
                "registry-1.docker.io",
                ServerAvailableFile(path=r"C:\temp\dockerhub_credentials.txt"),
            )

        grid_map(
            lambda x: x * 2,
            [1, 2, 3],
            ContainerAtDigest(repository=_PRIVATE_REPOSITORY, digest=_PRIVATE_DIGEST),
            None,
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
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        asyncio.run(delete_image(f"{_PRIVATE_REPOSITORY}@{_PRIVATE_DIGEST}"))

        with MeadowGridCoordinatorClientSync() as client:
            client.add_credentials(
                "DOCKER", "registry-1.docker.io", AwsSecret(secret_name="dockerhub")
            )

        grid_map(
            lambda x: x * 2,
            [1, 2, 3],
            ContainerAtDigest(repository=_PRIVATE_REPOSITORY, digest=_PRIVATE_DIGEST),
            None,
        )
