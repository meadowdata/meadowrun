import pytest

import meadowrun
from basics import BasicsSuite, HostProvider, MapSuite, ErrorsSuite
from meadowrun import Host, Resources, Kubernetes


def _kubernetes_host() -> meadowrun.Kubernetes:
    return meadowrun.Kubernetes(
        "meadowrunbucket",
        storage_endpoint_url="http://127.0.0.1:9000",
        storage_endpoint_url_in_cluster="http://minio-service:9000",
        storage_username_password_secret="meadowrun-s3-credentials",
        kube_config_context="minikube",
    )


def _meadowrun_container_deployment() -> meadowrun.Deployment:
    return meadowrun.Deployment.container_image("meadowrun/meadowrun-dev")


class MinikubeHostProvider(HostProvider):
    """
    In order to run these tests, you'll need to configure Minikube and Minio as per
    docs/how_to/kubernetes.md.

    You'll also probably want to run `docker_images/meadowrun/build-dev.bat`. You can
    also run `minikube image load meadowrun/meadowrun-dev:py3.10` which is faster than
    letting minikube download the image from Dockerhub.
    """

    def get_resources_required(self) -> Resources:
        return Resources(1, 1)

    def get_host(self) -> Host:
        return Kubernetes(
            storage_bucket="meadowrunbucket",
            storage_endpoint_url="http://127.0.0.1:9000",
            storage_endpoint_url_in_cluster="http://minio-service:9000",
            storage_username_password_secret="minio-credentials",
            kube_config_context="minikube",
        )

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    def can_get_log_file(self) -> bool:
        return False


class TestBasicsKubernetes(MinikubeHostProvider, BasicsSuite):
    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_apt_dependency(self) -> None:
        # Kubernetes doesn't support (and may never support) an environment spec with an
        # apt dependency at the same time
        pass

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_pip_file_in_git_repo_with_sidecar_container(self) -> None:
        # We have not yet implemented sidecar containers on Kubernetes
        pass

    @pytest.mark.skipif("sys.version_info < (3, 8)")
    @pytest.mark.asyncio
    async def test_git_repo_with_container(self) -> None:
        # We have not yet implemented specifying a custom container and a git repo at
        # the same time on Kubernetes
        pass

    @pytest.mark.asyncio
    async def test_meadowrun_git_repo_commit_container(self) -> None:
        # see test_git_repo_with_container
        pass


class TestErrorsKubernetes(MinikubeHostProvider, ErrorsSuite):
    pass


class TestMapKubernetes(MinikubeHostProvider, MapSuite):
    pass
