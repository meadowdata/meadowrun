from typing import Callable

import kubernetes_asyncio.client as kubernetes_client

import meadowrun
from meadowrun import Resources, Host
from suites import HostProvider, DeploymentSuite, EdgeCasesSuite

_KUBERNETES_SERVICE_ACCOUNT_NAME = "myserviceaccount"
_STORAGE_BUCKET_NAME = "meadowrun-gke-test"
_KUBE_CONFIG_CONTEXT_NAME = "gke-test-cluster"


def pod_customization(
    pod_template: kubernetes_client.V1PodTemplateSpec,
) -> kubernetes_client.V1PodTemplateSpec:
    pod_template.spec.service_account_name = _KUBERNETES_SERVICE_ACCOUNT_NAME
    # I believe this is only needed for standard GKE clusters, not needed for autopilot
    # clusters
    # pod_template.spec.node_selector = {
    #   "iam.gke.io/gke-metadata-server-enabled": "true"
    # }
    return pod_template


def _get_gke_host() -> meadowrun.Kubernetes:
    """
    To run these tests, first follow the instructions in
    https://docs.meadowrun.io/en/stable/tutorial/gke/

    Replace _KUBERNETES_SERVICE_ACCOUNT_NAME above with the name of your Kubernetes
    service account. Same with _STORAGE_BUCKET_NAME for your Google Cloud Storage
    bucket, and _KUBE_CONFIG_CONTEXT_NAME for your kubeconfig context name
    """

    return meadowrun.Kubernetes(
        meadowrun.GoogleBucketSpec(_STORAGE_BUCKET_NAME),
        kube_config_context=_KUBE_CONFIG_CONTEXT_NAME,
        reusable_pods=True,
        pod_customization=pod_customization,
    )


def _get_remote_function_for_deployment() -> Callable[[], str]:
    def remote_function() -> str:
        import importlib

        py_simple_package = importlib.import_module("py_simple_package")
        return py_simple_package.foo()

    return remote_function


async def test_pip_google_repository() -> None:
    """
    To run these tests:

    - Create a Google Cloud Artifact Repository for python
    - Create a python package called py_simple_package such that py_simple_package.foo()
    returns "Hello from py_simple_package!". Upload this package to the repository you
    created
    - Modify test_repo/requirements_google_artifact.txt to reflect the url of the
    repository you created
    - Give the Google Cloud Service Account that is linked to your Kubernetes service
    account read permissions to the repository you created (e.g. give it the Artifact
    Registry Reader)
    """
    results = await meadowrun.run_function(
        _get_remote_function_for_deployment(),
        _get_gke_host(),
        deployment=meadowrun.Deployment.git_repo(
            repo_url="https://github.com/meadowdata/test_repo",
            path_to_source="example_package",
            interpreter=meadowrun.PipRequirementsFile(
                "requirements_with_google_repository.txt", "3.9"
            ),
        ),
    )
    assert results == "Hello from py_simple_package!"


async def test_poetry_google_repository() -> None:
    """See test_pip_google_repository"""
    results = await meadowrun.run_function(
        _get_remote_function_for_deployment(),
        _get_gke_host(),
        deployment=meadowrun.Deployment.git_repo(
            repo_url="https://github.com/meadowdata/test_repo",
            path_to_source="example_package",
            interpreter=meadowrun.PoetryProjectPath(
                "poetry_with_google_repository", "3.9"
            ),
        ),
    )
    assert results == "Hello from py_simple_package!"


class GKEHostProvider(HostProvider):
    def get_resources_required(self) -> Resources:
        return Resources(1, 4, ephemeral_storage_gb=6)

    def get_host(self) -> Host:
        return _get_gke_host()

    def get_test_repo_url(self) -> str:
        return "https://github.com/meadowdata/test_repo"

    def can_get_log_file(self) -> bool:
        return False


class TestBasicsGKE(GKEHostProvider, DeploymentSuite):
    pass


class TestErrorsGKE(GKEHostProvider, EdgeCasesSuite):
    pass
