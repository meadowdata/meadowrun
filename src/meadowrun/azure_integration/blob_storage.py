from __future__ import annotations

import hashlib
import os
from typing import Tuple

from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_storage_account,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    AzureRestApiError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_storage_api import (
    StorageAccount,
    azure_blob_api,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
)
from meadowrun.run_job_core import S3CompatibleObjectStorage

CONTAINER_NAME = "meadowrun"


_LIFECYCLE_POLICY = {
    "rules": [
        {
            "enabled": True,
            "name": "meadowrun-lifecycle-policy",
            "type": "Lifecycle",
            "definition": {
                "actions": {
                    "baseBlob": {"delete": {"daysAfterModificationGreaterThan": 14}}
                },
                "filters": {"blobTypes": ["blockBlob"], "prefixMatch": ["meadowrun/"]},
            },
        }
    ]
}


async def ensure_container(
    subscription_id: str, storage_account: StorageAccount, location: str
) -> None:
    storage_account = await ensure_meadowrun_storage_account(location, "create")
    # https://docs.microsoft.com/en-us/rest/api/storageservices/create-container
    await azure_blob_api(
        "PUT",
        storage_account,
        CONTAINER_NAME,
        query_parameters={"restype": "container"},
        ignored_status_codes=((409, "ContainerAlreadyExists"),),
    )

    await azure_rest_api(
        "PUT",
        f"subscriptions/{subscription_id}/resourceGroups/"
        f"{MEADOWRUN_RESOURCE_GROUP_NAME}/providers/Microsoft.Storage/storageAccounts/"
        f"{storage_account.name}/managementPolicies/default",
        api_version="2021-09-01",
        json_content={"properties": {"policy": _LIFECYCLE_POLICY}},
    )


async def ensure_uploaded(file_path: str) -> Tuple[str, str]:
    hasher = hashlib.blake2b()
    with open(file_path, "rb") as file:
        buf = file.read()
        hasher.update(buf)
    digest = hasher.hexdigest()

    # the empty location is inelegant here, but:
    # 1. it's only used if the storage account does not exist yet; in that case we're
    #    not even getting to this point
    # 2. it's pretty annoying to thread it where download_file is used.
    storage_account = await ensure_meadowrun_storage_account("", on_missing="raise")
    try:
        _ = await azure_blob_api("HEAD", storage_account, f"{CONTAINER_NAME}/{digest}")
        # CONTAINER_NAME is a constant on Azure, but not AWS. Returning it here for
        # consistency.
        return CONTAINER_NAME, digest
    except AzureRestApiError as error:
        if error.status != 404:
            raise error

    # doesn't exist, need to upload it
    content_length = os.path.getsize(file_path)
    with open(file_path, "rb") as file:
        _ = await azure_blob_api(
            "PUT",
            storage_account,
            f"{CONTAINER_NAME}/{digest}",
            additional_headers={
                "Content-Length": str(content_length),
                "x-ms-blob-type": "BlockBlob",
            },
            binary_content=file,
        )
    return CONTAINER_NAME, digest


async def download_file(container_name: str, object_name: str, file_name: str) -> None:
    # see comment in ensure_uploaded regarding empty location
    storage_account = await ensure_meadowrun_storage_account("", on_missing="raise")

    result: bytes = await azure_blob_api(
        "GET", storage_account, f"{container_name}/{object_name}"
    )
    with open(file_name, "wb") as file:
        file.write(result)


class AzureBlobStorage(S3CompatibleObjectStorage):
    # TODO this should have a region_name property also

    @classmethod
    def get_url_scheme(cls) -> str:
        return "azblob"

    async def _upload(self, file_path: str) -> Tuple[str, str]:
        return await ensure_uploaded(file_path)

    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        return await download_file(bucket_name, object_name, file_name)
