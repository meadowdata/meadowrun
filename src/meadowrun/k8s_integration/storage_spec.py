from __future__ import annotations

import argparse
import dataclasses
import os
from typing import Optional, List, Iterable, Dict, Type, TYPE_CHECKING

from kubernetes_asyncio import client as kubernetes_client

from meadowrun.func_worker_storage_helper import (
    MEADOWRUN_STORAGE_USERNAME,
    MEADOWRUN_STORAGE_PASSWORD,
)
from meadowrun.gcp_integration.google_storage import get_google_storage_bucket
from meadowrun.k8s_integration.k8s import StorageBucketSpec, STORAGE_TYPE
from meadowrun.k8s_integration.k8s_core import get_kubernetes_secret
from meadowrun.storage_grid_job import (
    get_generic_username_password_bucket,
    GenericStorageBucket,
)

if TYPE_CHECKING:
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket


@dataclasses.dataclass(frozen=True)
class GenericStorageBucketSpec(StorageBucketSpec):
    """
    Specifies a bucket in an object storage system. The object storage must be
    S3-compatible and use username/password authentication. The arguments provided will
    be used along the lines of:

    ```python
    import boto3
    boto3.Session(
        aws_access_key_id=username, aws_secret_access_key=password
    ).client(
        "s3", endpoint_url=endpoint_url
    ).download_file(
        Bucket=bucket, Key="test.file", Filename="test.file"
    )
    ```

    `username` and `password` should be the values provided by
    `username_password_secret`. (boto3 is built to be used with AWS S3, but it should
    work with any S3-compatible object store like Minio, Ceph, etc.)

    Attributes:
        bucket: The name of the bucket to use
        endpoint_url: The endpoint_url for the object storage system
        endpoint_url_in_cluster: Defaults to None which means use endpoint_url. You can
            set this to a different URL if you need to use a different URL from inside
            the Kubernetes cluster to access the storage endpoint
        username_password_secret: This should be the name of a Kubernetes secret
            that has a "username" and "password" key, where the username and password
            can be used to authenticate with the storage API.
    """

    bucket: str
    endpoint_url: str
    endpoint_url_in_cluster: Optional[str] = None
    username_password_secret: Optional[str] = None

    def _get_storage_endpoint_url_in_cluster(self) -> str:
        if self.endpoint_url_in_cluster is not None:
            return self.endpoint_url_in_cluster
        return self.endpoint_url

    async def get_storage_bucket(
        self, kubernetes_namespace: str
    ) -> AbstractStorageBucket:
        if self.username_password_secret is not None:
            secret_data = await get_kubernetes_secret(
                kubernetes_namespace,
                self.username_password_secret,
            )
        else:
            secret_data = {}

        return get_generic_username_password_bucket(
            self.endpoint_url,
            secret_data.get("username", None),
            secret_data.get("password", None),
            self.bucket,
        )

    def get_command_line_arguments(self) -> List[str]:
        return [
            "--storage-bucket",
            self.bucket,
            "--storage-endpoint-url",
            self._get_storage_endpoint_url_in_cluster(),
        ]

    def get_environment_variables(self) -> Iterable[kubernetes_client.V1EnvVar]:
        if self.username_password_secret is not None:
            yield kubernetes_client.V1EnvVar(
                name=MEADOWRUN_STORAGE_USERNAME,
                value_from=kubernetes_client.V1EnvVarSource(
                    secret_key_ref=kubernetes_client.V1SecretKeySelector(
                        key="username",
                        name=self.username_password_secret,
                        optional=False,
                    )
                ),
            )

            yield kubernetes_client.V1EnvVar(
                name=MEADOWRUN_STORAGE_PASSWORD,
                value_from=kubernetes_client.V1EnvVarSource(
                    secret_key_ref=kubernetes_client.V1SecretKeySelector(
                        key="password",
                        name=self.username_password_secret,
                        optional=False,
                    )
                ),
            )

    @classmethod
    def get_storage_type(cls) -> str:
        return "generic"

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--storage-bucket")
        parser.add_argument("--storage-endpoint-url")

    @classmethod
    async def from_parsed_args(cls, args: argparse.Namespace) -> GenericStorageBucket:
        # prepare storage client, filenames and pickle protocol for the result
        storage_username = os.environ.get(MEADOWRUN_STORAGE_USERNAME, None)
        storage_password = os.environ.get(MEADOWRUN_STORAGE_PASSWORD, None)
        if not args.storage_endpoint_url or not args.storage_bucket:
            raise ValueError(
                "--storage-endpoint-url and --storage-bucket must be specified with "
                f"{STORAGE_TYPE} {cls.get_storage_type()}"
            )
        return get_generic_username_password_bucket(
            args.storage_endpoint_url,
            storage_username,
            storage_password,
            args.storage_bucket,
        )


@dataclasses.dataclass(frozen=True)
class GoogleBucketSpec(StorageBucketSpec):
    """
    Specifies a bucket in Google Cloud Storage. Requires that credentials are available.
    This usually means that on the client you've logged in via the Google Cloud CLI, and
    in the Kubernetes cluster you're running with a service account (via
    the pod_customization parameter on Kubernetes) that has access to the specified
    bucket.

    Attributes:
        bucket: The name of the bucket to use
    """

    bucket: str

    async def get_storage_bucket(
        self, kubernetes_namespace: str
    ) -> AbstractStorageBucket:
        return get_google_storage_bucket(self.bucket)

    def get_command_line_arguments(self) -> List[str]:
        return ["--google-bucket", self.bucket]

    def get_environment_variables(self) -> Iterable[kubernetes_client.V1EnvVar]:
        return ()

    @classmethod
    def get_storage_type(cls) -> str:
        return "gcp"

    @classmethod
    def add_arguments_to_parser(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--google-bucket")

    @classmethod
    async def from_parsed_args(cls, args: argparse.Namespace) -> AbstractStorageBucket:
        return get_google_storage_bucket(args.google_bucket)


_ALL_STORAGE_BUCKET_SPEC_TYPES: List[Type[StorageBucketSpec]] = [
    GenericStorageBucketSpec,
    GoogleBucketSpec,
]
_ALL_STORAGE_BUCKET_SPECS: Dict[str, Type[StorageBucketSpec]] = {
    spec_type.get_storage_type(): spec_type
    for spec_type in _ALL_STORAGE_BUCKET_SPEC_TYPES
}


def add_storage_spec_arguments_to_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(STORAGE_TYPE, required=True)
    for spec_type in _ALL_STORAGE_BUCKET_SPECS.values():
        spec_type.add_arguments_to_parser(parser)


async def storage_bucket_from_parsed_args(
    args: argparse.Namespace,
) -> AbstractStorageBucket:
    spec_type = _ALL_STORAGE_BUCKET_SPECS[args.storage_type]
    return await spec_type.from_parsed_args(args)
