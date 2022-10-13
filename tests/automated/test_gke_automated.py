from typing import Callable

import meadowrun
import kubernetes_asyncio.client as kubernetes_client


_KUBERNETES_SERVICE_ACCOUNT_NAME = "myserviceaccount"
_MINIO_PUBLIC_URL = "http://35.231.225.155:9000"


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
    To run these tests:

    - Create a Google Cloud Artifact Repository for python
    - Create a python package called py_simple_package such that py_simple_package.foo()
    returns "Hello from py_simple_package!". Upload this package to the repository you
    created
    - Modify test_repo/requirements_google_artifact.txt to reflect the url of the
    repository you created

    - Create a GKE autopilot cluster
    - Configure your kubectl to make your GKE your default

    - Create a Google Storage bucket named "meadowrun-gke-test" or something similar

    - Create a Google Cloud Service Account called "test-service-account" and give it
    read permissions to the repository you created (e.g. give it the Artifact Registry
    Reader) and to read/write the Google Storage bucket you created (e.g. give it the
    Storage Admin role).

    - Create a Kubernetes Service Account and bind it to the Google Cloud Service
    Account your created. Replace _KUBERNETES_SERVICE_ACCOUNT_NAME if you need to.
    Example command line:
        kubectl create serviceaccount myserviceaccount --namespace default
        gcloud iam service-accounts add-iam-policy-binding \
            test-service-account@meadowrun-playground.iam.gserviceaccount.com \
            --role roles/iam.workloadIdentityUser \
            --member \
            "serviceAccount:meadowrun-playground.svc.id.goog[default/myserviceaccount]"
        kubectl annotate serviceaccount myserviceaccount --namespace default \
            iam.gke.io/gcp-service-account=test-service-account@meadowrun-playground.iam.gserviceaccount.com
    """

    return meadowrun.Kubernetes(
        meadowrun.GoogleBucketSpec("meadowrun-gke-test"),
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
