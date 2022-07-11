from __future__ import annotations

import xml.dom.minidom
from typing import TYPE_CHECKING, Any, Iterable, Optional, Tuple, Type

if TYPE_CHECKING:
    import aiohttp
    import requests


class AzureRestApiError(Exception):
    """
    status is the integer http status code. code and message are typically returned by
    Azure APIs. code is usually a single word/phrase like "ResourceNotFound", and
    message is usually a more verbose explanation.
    """

    def __init__(self, status: int, code: Optional[str], message: str):
        self.status = status
        self.code = code
        self.message = message


class ResourceNotFoundError(AzureRestApiError):
    def __init__(self, status: int, code: Optional[str], message: str):
        super().__init__(status, code, message)


class ResourceExistsError(AzureRestApiError):
    def __init__(self, status: int, code: Optional[str], message: str):
        super().__init__(status, code, message)


class ResourceModifiedError(AzureRestApiError):
    def __init__(self, status: int, code: Optional[str], message: str):
        super().__init__(status, code, message)


def _get_code_and_message_from_json(
    response_json: Any,
) -> Tuple[Optional[str], Optional[str]]:
    if "error" in response_json:
        error = response_json["error"]
    elif "errors" in response_json and len(response_json["errors"]) == 1:
        error = response_json["errors"][0].get("code")
    elif "odata.error" in response_json:
        error = response_json["odata.error"]
    else:
        error = None

    if error is not None:
        return error.get("code"), error.get("message")
    else:
        return None, None


def _get_code_and_message_from_xml(
    response_text: str,
) -> Tuple[Optional[str], Optional[str]]:
    code, message = None, None
    try:
        parsed = xml.dom.minidom.parseString(response_text)
        if parsed and len(parsed.documentElement.childNodes) == 2:
            if parsed.documentElement.childNodes[0].nodeName == "Code":
                code = parsed.documentElement.childNodes[0].childNodes[0].nodeValue
            if parsed.documentElement.childNodes[1].nodeName == "Message":
                message = parsed.documentElement.childNodes[1].childNodes[0].nodeValue
    except Exception:
        pass

    return code, message


def _exception_type_from_code(code: Optional[str]) -> Type:
    if code in (
        "ResourceGroupNotFound",
        "ResourceNotFound",
        "SecretNotFound",
        "NAME_UNKNOWN",
    ):
        return ResourceNotFoundError
    elif code in ("RoleAssignmentExists", "EntityAlreadyExists"):
        return ResourceExistsError
    elif code == "UpdateConditionNotSatisfied":
        return ResourceModifiedError
    else:
        return AzureRestApiError


async def raise_for_status(
    response: aiohttp.ClientResponse,
    ignored_status_codes: Iterable[Tuple[int, str]] = tuple(),
) -> None:
    """
    Like response.raise_for_status, but raises AzureRestApiError based on parsing the
    response content
    """
    if not response.ok:
        code = None
        message = None
        response_text = None

        if (
            response.headers.get("Content-Type", "")
            .lower()
            .startswith("application/json")
        ):
            try:
                code, message = _get_code_and_message_from_json(await response.json())
            except Exception:
                pass
        elif response.headers.get("Content-Type", "").startswith("application/xml"):
            try:
                response_text = await response.text()
            except Exception:
                response_text = ""

            code, message = _get_code_and_message_from_xml(response_text)

        if message is None:
            if response_text is None:
                # None means we haven't tried to get response.text. "" means we have
                # tried and the response was empty or we failed to get the text
                response_text = await response.text()
            message = response_text

        if code is None or (response.status, code) not in ignored_status_codes:
            raise _exception_type_from_code(code)(response.status, code, message)


def raise_for_status_sync(response: requests.Response) -> None:
    if not response.ok:
        code = None
        message = None
        response_text = None

        if (
            response.headers.get("Content-Type", "")
            .lower()
            .startswith("application/json")
        ):
            try:
                code, message = _get_code_and_message_from_json(response.json())
            except Exception:
                pass
        elif response.headers.get("Content-Type", "").startswith("application/xml"):
            try:
                response_text = response.text
            except Exception:
                response_text = ""

            code, message = _get_code_and_message_from_xml(response_text)

        if message is None:
            if response_text is None:
                response_text = response.text
            message = response_text

        raise _exception_type_from_code(code)(response.status_code, code, message)
