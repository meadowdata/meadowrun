from suites import DeploymentSuite, HostProvider, EdgeCasesSuite
from meadowrun import Host, Resources, Kubernetes, GenericStorageBucketSpec


def _kubernetes_host() -> Kubernetes:
    return Kubernetes(
        GenericStorageBucketSpec(
            "meadowrunbucket",
            endpoint_url="http://127.0.0.1:9000",
            endpoint_url_in_cluster="http://minio-service:9000",
            username_password_secret="minio-credentials",
        ),
        kube_config_context="minikube",
        reusable_pods=False,
    )


class MinikubeSingleUseHostProvider(HostProvider):
    """
    In order to run these tests, you'll need to configure Minikube and Minio as per
    docs/how_to/kubernetes.md.

    You'll also probably want to run `docker_images/meadowrun/build-dev.bat`. You can
    also run `minikube image load meadowrun/meadowrun-dev:py3.10` which is faster than
    letting minikube download the image from Dockerhub.
    """

    def get_resources_required(self) -> Resources:
        return Resources(0, 0)

    def get_host(self) -> Host:
        return _kubernetes_host()

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    def can_get_log_file(self) -> bool:
        return False


class TestDeploymentsMinikubeSingleUse(MinikubeSingleUseHostProvider, DeploymentSuite):
    pass


class TestEdgeCasesMinikubeSingleUse(MinikubeSingleUseHostProvider, EdgeCasesSuite):
    pass
