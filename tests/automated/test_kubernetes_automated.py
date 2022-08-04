import pytest
import meadowrun
from meadowrun.meadowrun_pb2 import ProcessState


def _kubernetes_host() -> meadowrun.Kubernetes:
    return meadowrun.Kubernetes(
        "meadowrunbucket",
        storage_endpoint_url="http://127.0.0.1:9000",
        storage_endpoint_url_in_cluster="http://minio-service:9000",
        storage_username_password_secret="meadowrun-s3-credentials",
        kube_config_context="minikube",
    )


def _meadowrun_container_deployment():
    return meadowrun.Deployment.container_image("meadowrun/meadowrun-dev")


class TestKubernetes:
    """
    In order to run these tests, you'll need to configure Minikube and Minio as per
    docs/how_to/kubernetes.md.

    You'll also probably want to run `build_scripts/build_docker_image.bat && docker
    push meadowrun/meadowrun-dev`

    TODO eventually this should be replaced by a
    TestBasicsKubernetes(KubernetesHostProvider, BasicsSuite) class once we support more
    functionality in Kubernetes
    """

    @pytest.mark.asyncio
    async def test_kubernetes_function(self):
        result = await meadowrun.run_function(
            lambda: 2 * 2,
            _kubernetes_host(),
            deployment=_meadowrun_container_deployment(),
        )

        assert result == 4

    @pytest.mark.asyncio
    async def test_kubernetes_command(self):
        result = await meadowrun.run_command(
            "python --version",
            _kubernetes_host(),
            deployment=_meadowrun_container_deployment(),
        )

        assert result.process_state == ProcessState.ProcessStateEnum.SUCCEEDED
        assert result.result is None

    @pytest.mark.asyncio
    async def test_kubernetes_map(self):
        result = await meadowrun.run_map(
            lambda x: x**x,
            [1, 2, 3, 4, 5],
            _kubernetes_host(),
            deployment=_meadowrun_container_deployment(),
        )

        assert result == [1, 4, 27, 256, 3125]
