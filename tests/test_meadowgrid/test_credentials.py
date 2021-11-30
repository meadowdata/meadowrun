import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid import grid_map
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientSync
from meadowgrid.meadowgrid_pb2 import ServerAvailableFile, ContainerAtDigest
from test_meadowgrid.test_meadowgrid_basics import TEST_WORKING_FOLDER


def manual_test_docker_credentials():
    """
    This is a manual test because it requires out-of-band steps to set up.

    1. First, create an account on DockerHub if you don't have one already. Then, put
       your credentials in C:\temp\dockerhub_credentials.txt, username on first line,
       password on second line

    2. Create a docker image that has meadowdata in it:
       > build_docker_image.bat

    3. In DockerHub, create a private repository called test1. Then, upload our new
       image:
       > docker tag meadowdata <username>/test1
       > docker login
       > docker push <username>/test1

    4. Replace the digest below with the actual digest of this image using
       > docker images --digests

    5. Delete the local copy of the image and log out of docker
       > docker image rm <username>/test1
       > docker image rm meadowdata
       > docker logout

    6. Now run the test below
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.job_worker_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        with MeadowGridCoordinatorClientSync() as client:
            client.add_credentials(
                "DOCKER",
                "registry-1.docker.io",
                ServerAvailableFile(path=r"C:\temp\dockerhub_credentials.txt"),
            )

        grid_map(
            lambda x: x * 2,
            [1, 2, 3],
            ContainerAtDigest(
                repository="hrichardlee/test1",
                digest="sha256:a6848cf6d57039650b4a6062aea53f8259b0de60e55b8d61e3f5de2e7d51c591",
            ),
            None,
        )
