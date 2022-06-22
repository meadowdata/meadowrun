import base64
import dataclasses
import datetime
import hashlib
import hmac
import xml.dom.minidom
from typing import Any, AsyncIterator, Dict, Iterable, Optional, Sequence, Tuple

import aiohttp
import multidict
from .azure_exceptions import (
    raise_for_status,
)
from .azure_rest_api import (
    _return_response,
)


def _sign_string(key: str, string_to_sign: str) -> str:
    return base64.b64encode(
        hmac.HMAC(
            base64.b64decode(key), string_to_sign.encode("utf-8"), hashlib.sha256
        ).digest()
    ).decode("utf-8")


def _get_now_rfc1123() -> str:
    """
    This is a specific datetime format required for authentication with the storage APIs
    """
    # Copied from
    # https://github.com/Azure/azure-sdk-for-python/blob/1d5096eb1bc8cbd77223ecc7a628738a5f88751c/sdk/storage/azure-storage-file-datalake/azure/storage/filedatalake/_serialize.py#L45
    dt = datetime.datetime.utcnow()

    weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    month = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ][dt.month - 1]
    return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (
        weekday,
        dt.day,
        month,
        dt.year,
        dt.hour,
        dt.minute,
        dt.second,
    )


def table_key_url(table_name: str, partition_key: str, row_key: str) -> str:
    """Creates the url path for the combination of a table and a key"""
    partition_key = partition_key.replace("'", "''")
    row_key = row_key.replace("'", "''")
    return f"{table_name}(PartitionKey='{partition_key}',RowKey='{row_key}')"


@dataclasses.dataclass
class StorageAccount:
    name: str
    key: str
    path: Optional[str]

    def get_path(self) -> str:
        """
        When we construct a StorageAccount normally, it's easy and useful to include the
        path. When we construct a StorageAccount in a management function, it's hard and
        not useful to include the path so we don't do it.
        """
        if self.path is None:
            raise ValueError("Requested StorageAccount.path, but it was not available")
        return self.path


def _get_default_table_api_headers(
    storage_account: StorageAccount, url_path: str
) -> Dict[str, str]:
    """Creates default headers, including importantly the signed Authorization header"""

    headers = {
        "x-ms-version": "2019-02-02",
        "x-ms-date": _get_now_rfc1123(),
        "DataServiceVersion": "3.0",
        "Accept": "application/json;odata=minimalmetadata",
    }
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key
    # based on
    # https://github.com/Azure/azure-sdk-for-python/blob/83964018f39b7702659d208cd2640f5eea7400fc/sdk/tables/azure-data-tables/azure/data/tables/_authentication.py
    signature = _sign_string(
        storage_account.key,
        "\n".join(
            [headers.get("x-ms-date", ""), f"/{storage_account.name}/{url_path}"]
        ),
    )
    headers["Authorization"] = f"SharedKeyLite {storage_account.name}:{signature}"
    return headers


async def azure_table_api(
    method: str,
    storage_account: StorageAccount,
    url_path: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    json_content: Any = None,
    additional_headers: Optional[Dict[str, str]] = None,
) -> Any:
    """Supports any Azure Table API"""
    headers = _get_default_table_api_headers(storage_account, url_path)
    if additional_headers:
        headers.update(additional_headers)
    async with aiohttp.request(
        method,
        f"https://{storage_account.name}.table.core.windows.net/{url_path}",
        params=query_parameters,
        headers=headers,
        json=json_content,
    ) as response:
        await raise_for_status(response)
        return await _return_response(response)


_CONTINUATION_HEADER_PREFIX = "x-ms-continuation-"
_CONTINUATION_HEADER_PREFIX_LENGTH = len(_CONTINUATION_HEADER_PREFIX)


def _get_page_parameters(headers: multidict.CIMultiDictProxy[str]) -> Dict[str, str]:
    return {
        key[_CONTINUATION_HEADER_PREFIX_LENGTH:]: value
        for key, value in headers.items()
        if key.startswith(_CONTINUATION_HEADER_PREFIX)
    }


async def azure_table_api_paged(
    method: str,
    storage_account: StorageAccount,
    url_path: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
) -> AsyncIterator[Any]:
    """
    Supports Azure Table APIs that require paging, as per
    https://docs.microsoft.com/en-us/rest/api/storageservices/query-timeout-and-pagination
    """

    parameters = {"$top": "1000"}
    if query_parameters:
        parameters.update(query_parameters)

    async with aiohttp.request(
        method,
        f"https://{storage_account.name}.table.core.windows.net/{url_path}",
        params=parameters,
        headers=_get_default_table_api_headers(storage_account, url_path),
    ) as response:
        await raise_for_status(response)
        yield await _return_response(response)

        page_parameters = _get_page_parameters(response.headers)

    while page_parameters:
        # a bit sloppy to modify in place, but turning page_parameters into
        # query_parameters here
        page_parameters.update(parameters)
        async with aiohttp.request(
            method,
            f"https://{storage_account.name}.table.core.windows.net/{url_path}",
            params=page_parameters,
            headers=_get_default_table_api_headers(storage_account, url_path),
        ) as response:
            await raise_for_status(response)
            yield await _return_response(response)

            page_parameters = _get_page_parameters(response.headers)


def _replace_linear_whitespace(s: str) -> str:
    return s.replace("\n", " ").replace("\r", " ").replace("\t", " ")


def _canonicalized_headers(headers: Dict[str, str]) -> str:
    "Produce the CanonicalizedHeaders for authorization."
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-headers-string
    headers_to_sign = []
    for key, value in headers.items():
        key = key.lower()
        if key.startswith("x-ms-"):
            headers_to_sign.append(f"{key}:{_replace_linear_whitespace(value)}")
    return "\n".join(sorted(headers_to_sign))


async def azure_queue_api(
    method: str,
    storage_account: StorageAccount,
    url_path: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    xml_content: Optional[str] = None,
) -> Any:
    """Supports any Azure Queue API"""

    # headers and signing
    headers = {
        "x-ms-version": "2021-02-12",
        "x-ms-date": _get_now_rfc1123(),
        "Content-Type": "application/xml",
    }

    signature = _sign_string(
        storage_account.key,
        "\n".join(
            [
                method,
                "",  # Content-MD5, but we don't include it because it's optional
                headers["Content-Type"],
                # according to the docs, x-ms-date should be here, but the python Azure
                # SDK leaves this blank, and this seems to work
                "",
                _canonicalized_headers(headers),
                f"/{storage_account.name}/{url_path}",
            ]
        ),
    )

    headers["Authorization"] = f"SharedKeyLite {storage_account.name}:{signature}"

    # now actually make the request

    async with aiohttp.request(
        method,
        f"https://{storage_account.name}.queue.core.windows.net/{url_path}",
        params=query_parameters,
        headers=headers,
        data=xml_content,
    ) as response:
        await raise_for_status(response)
        return await response.text()


# high-level queue APIs

_QUEUE_MESSAGE_TEMPLATE = "<QueueMessage><MessageText>{}</MessageText></QueueMessage>"


async def queue_send_message(
    storage_account: StorageAccount, queue_name: str, message: bytes
) -> None:
    """Sends a message on the specified queues"""
    # https://docs.microsoft.com/en-us/rest/api/storageservices/put-message
    await azure_queue_api(
        "POST",
        storage_account,
        f"{queue_name}/messages",
        xml_content=_QUEUE_MESSAGE_TEMPLATE.format(
            base64.b64encode(message).decode("utf-8")
        ),
    )


@dataclasses.dataclass
class QueueMessage:
    message_id: str
    pop_receipt: str
    message_content: bytes


async def queue_receive_messages(
    storage_account: StorageAccount,
    queue_name: str,
    *,
    visibility_timeout_secs: Optional[int] = None,
    num_messages: Optional[int] = None,
) -> Sequence[QueueMessage]:
    """Receives 1 (the default) or more messages"""
    # https://docs.microsoft.com/en-us/rest/api/storageservices/get-messages
    query_parameters = {}
    if visibility_timeout_secs is not None:
        query_parameters["visibilitytimeout"] = str(visibility_timeout_secs)
    if num_messages is not None:
        query_parameters["numofmessages"] = str(num_messages)

    message_xml = await azure_queue_api(
        "GET",
        storage_account,
        f"{queue_name}/messages",
        query_parameters=query_parameters,
    )

    try:
        results = []
        message = xml.dom.minidom.parseString(message_xml)
        root_node = message.documentElement
        if root_node.nodeName != "QueueMessagesList":
            raise ValueError("Expected root element to be QueueMessages List")
        for message_node in root_node.childNodes:
            if message_node.nodeName == "QueueMessage":
                message_id = None
                pop_receipt = None
                message_text = None
                for attribute_node in message_node.childNodes:
                    if attribute_node.nodeName == "MessageId":
                        message_id = attribute_node.childNodes[0].nodeValue
                    elif attribute_node.nodeName == "PopReceipt":
                        pop_receipt = attribute_node.childNodes[0].nodeValue
                    elif attribute_node.nodeName == "MessageText":
                        message_text = attribute_node.childNodes[0].nodeValue

                if message_id is None:
                    raise ValueError("A QueueMessage was missing a MessageId")
                if pop_receipt is None:
                    raise ValueError("A QueueMessage was missing a PopReceipt")
                if message_text is None:
                    raise ValueError("A QueueMessage was missing a MessageText")
                results.append(
                    QueueMessage(
                        message_id,
                        pop_receipt,
                        base64.b64decode(message_text),
                    )
                )
    except Exception as e:
        raise ValueError(f"Cannot parse XML: {message_xml}") from e

    return results


async def queue_delete_message(
    storage_account: StorageAccount, queue_name: str, message_id: str, pop_receipt: str
) -> None:
    """Deletes the specified message"""
    # https://docs.microsoft.com/en-us/rest/api/storageservices/delete-message2
    await azure_queue_api(
        "DELETE",
        storage_account,
        f"{queue_name}/messages/{message_id}",
        query_parameters={"popreceipt": pop_receipt},
    )


def _get_default_blob_api_headers(
    method: str,
    storage_account: StorageAccount,
    url_path: str,
    content_type: str,
    additional_headers: Optional[Dict[str, str]],
) -> Dict[str, str]:
    """Creates default headers, including importantly the signed Authorization header"""
    VERSION = "2021-08-06"
    request_date_time = _get_now_rfc1123()
    headers = {
        "x-ms-version": VERSION,
        "x-ms-date": request_date_time,
        "Content-Type": content_type,
    }
    if additional_headers:
        headers.update(additional_headers)
    # https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#blob-queue-and-file-services-shared-key-lite-authorization
    s_str = "\n".join(
        [
            method,
            "",  # Content-MD5
            content_type,
            "",  # Date
            # CanonicalizedHeaders
            _canonicalized_headers(headers),
            # CanonicalizedResource
            f"/{storage_account.name}/{url_path}",
        ]
    )
    signature = _sign_string(storage_account.key, s_str)
    headers["Authorization"] = f"SharedKeyLite {storage_account.name}:{signature}"
    return headers


async def azure_blob_api(
    method: str,
    storage_account: StorageAccount,
    url_path: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    json_content: Any = None,
    binary_content: Any = None,
    additional_headers: Optional[Dict[str, str]] = None,
    ignored_status_codes: Iterable[Tuple[int, str]] = tuple(),
) -> Any:
    """Supports any Azure Blob Storage API"""
    content_type = (
        "application/octet-stream" if binary_content is not None else "application/json"
    )
    headers = _get_default_blob_api_headers(
        method, storage_account, url_path, content_type, additional_headers
    )

    async with aiohttp.request(
        method,
        f"https://{storage_account.name}.blob.core.windows.net/{url_path}",
        params=query_parameters,
        headers=headers,
        json=json_content,
        data=binary_content,
    ) as response:
        await raise_for_status(response, ignored_status_codes=ignored_status_codes)
        return await _return_response(response)
