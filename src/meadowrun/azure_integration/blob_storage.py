from __future__ import annotations

import hashlib
import os
import xml.dom.minidom
from typing import Optional, Tuple, List, Any, Coroutine

from meadowrun.abstract_storage_bucket import AbstractStorageBucket
from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_storage_account,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
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
    azure_blob_api_binary,
)

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


async def ensure_uploaded(file_path: str) -> str:
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
        return digest
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
    return digest


async def download_file(object_name: str, file_name: str) -> None:
    # see comment in ensure_uploaded_by_hash regarding empty location
    storage_account = await ensure_meadowrun_storage_account("", on_missing="raise")

    result: bytes = await azure_blob_api(
        "GET", storage_account, f"{CONTAINER_NAME}/{object_name}"
    )
    with open(file_name, "wb") as file:
        file.write(result)


async def get_azure_blob_container(location: str) -> AzureBlobContainer:
    return AzureBlobContainer(
        await ensure_meadowrun_storage_account(location, on_missing="create"),
        CONTAINER_NAME,
    )


class AzureBlobContainer(AbstractStorageBucket):
    def __init__(self, storage_account: StorageAccount, container_name: str):
        self._storage_account = storage_account
        self._container_name = container_name
        self._cache_key = f"azure/{storage_account.name}"

    def get_cache_key(self) -> str:
        return self._cache_key

    def get_bytes(self, key: str) -> Coroutine[Any, Any, bytes]:
        return azure_blob_api_binary(
            "GET", self._storage_account, f"{self._container_name}/{key}"
        )

    async def try_get_bytes(self, key: str) -> Optional[bytes]:
        try:
            return await self.get_bytes(key)
        except AzureRestApiError as error:
            if error.status == 404:
                return None
            raise

    def get_byte_range(
        self, key: str, byte_range: Tuple[int, int]
    ) -> Coroutine[Any, Any, bytes]:
        return azure_blob_api_binary(
            "GET",
            self._storage_account,
            f"{self._container_name}/{key}",
            additional_headers={"x-ms-range": f"bytes={byte_range[0]}-{byte_range[1]}"},
        )

    async def write_bytes(self, data: bytes, key: str) -> None:
        await azure_blob_api(
            "PUT",
            self._storage_account,
            f"{self._container_name}/{key}",
            additional_headers={
                "Content-Length": str(len(data)),
                "x-ms-blob-type": "BlockBlob",
            },
            binary_content=data,
        )

    async def exists(self, key: str) -> bool:
        try:
            await azure_blob_api(
                "HEAD", self._storage_account, f"{self._container_name}/{key}"
            )
            return True
        except AzureRestApiError as error:
            if error.status == 404:
                return False
            raise

    async def get_file(self, key: str, local_filename: str) -> None:
        # TODO we should stream this from the response object
        data = await self.get_bytes(key)
        with open(local_filename, "wb") as f:
            f.write(data)

    async def try_get_file(self, key: str, local_filename: str) -> bool:
        try:
            await self.get_file(key, local_filename)
            return True
        except AzureRestApiError as error:
            if error.status == 404:
                return False
            raise

    async def write_file(self, local_filename: str, key: str) -> None:
        content_length = os.path.getsize(local_filename)
        with open(local_filename, "rb") as file:
            _ = await azure_blob_api(
                "PUT",
                self._storage_account,
                f"{self._container_name}/{key}",
                additional_headers={
                    "Content-Length": str(content_length),
                    "x-ms-blob-type": "BlockBlob",
                },
                binary_content=file,
            )

    async def list_objects(self, key_prefix: str) -> List[str]:
        response = await azure_blob_api(
            "GET",
            self._storage_account,
            self._container_name,
            query_parameters={
                "restype": "container",
                "comp": "list",
                "prefix": key_prefix,
            },
        )

        message = xml.dom.minidom.parseString(response)
        root_node = message.documentElement
        if root_node.nodeName != "EnumerationResults":
            raise ValueError("Expected root element to be EnumerationResults")
        results = []
        for node in root_node.childNodes:
            if node.nodeName == "Blobs":
                for blob_node in node.childNodes:
                    for blob_child in blob_node.childNodes:
                        if blob_child.nodeName == "Name":
                            if len(blob_child.childNodes) != 1:
                                raise ValueError("Unexpected XML format")
                            results.append(blob_child.childNodes[0].nodeValue)
        return results

    async def delete_object(self, key: str) -> None:
        await azure_blob_api(
            "DELETE",
            self._storage_account,
            f"{self._container_name}/{key}",
            additional_headers={"x-ms-delete-snapshots": "include"},
        )
