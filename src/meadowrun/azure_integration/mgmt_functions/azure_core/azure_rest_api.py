import asyncio.subprocess
import dataclasses
import datetime
import time
from typing import (
    Any,
    AsyncIterator,
    Coroutine,
    Dict,
    Iterable,
    Optional,
    Tuple,
    cast,
)
from typing_extensions import Literal

import aiohttp
import requests

from .azure_exceptions import AzureRestApiError, raise_for_status, raise_for_status_sync
from .azure_identity import CACHED_AZURE_VARIABLES, get_token, get_token_sync

_BASE_URL = "https://management.azure.com"


async def _return_response(response: aiohttp.ClientResponse) -> Any:
    # If there's nothing to return, the Azure APIs will return content-type:
    # application/octet-stream with no actual content. It's easier to just return this
    # as None rather than having to fully support another content type
    if response.content_length == 0:
        return None

    # this is a bit sloppy, but sometimes content-length is not set, content-type is
    # text/plain, and the actual content is empty. We don't want response.json() to
    # throw in that scenario either
    if response.content_type.startswith("text/plain"):
        return await response.text()

    if response.content_type.startswith("application/xml"):
        return await response.text()

    if response.content_type.startswith("application/json"):
        return await response.json()

    # assuming binary
    return await response.content.read()


def _prepare_request(
    method: str,
    url_path: str,
    api_version: str,
    token: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    json_content: Any = None,
    base_url: Optional[str] = None,
) -> Dict[str, Any]:
    if not base_url:
        base_url = _BASE_URL
    url = f"{base_url}/{url_path}"
    headers = {"Authorization": f"Bearer {token}"}
    parameters = {"api-version": api_version}
    if query_parameters:
        parameters.update(query_parameters)

    return {
        "method": method,
        "url": url,
        "params": parameters,
        "headers": headers,
        "json": json_content,
    }


async def azure_rest_api(
    method: str,
    url_path: str,
    api_version: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    json_content: Any = None,
    base_url: Optional[str] = None,
    token: Optional[str] = None,
    token_scope: Optional[str] = None,
) -> Any:
    """
    Supports any Azure REST API call, returns the JSON-deserialized content of the
    response
    """

    if token is None:
        token = await get_token(token_scope)
    async with aiohttp.request(
        **_prepare_request(
            method,
            url_path,
            api_version,
            token,
            query_parameters=query_parameters,
            json_content=json_content,
            base_url=base_url,
        )
    ) as response:
        await raise_for_status(response)
        return await _return_response(response)


async def azure_rest_api_paged(
    method: str,
    url_path: str,
    api_version: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    base_url: Optional[str] = None,
    token: Optional[str] = None,
) -> AsyncIterator[Any]:
    """
    Supports any Azure REST API call that takes a $maxpagesize parameter and returns a
    json body with a nextLink parameter. Returns the json-deserialized body of each
    call. Typical usage will be [item async for page in azure_rest_api_paged(...) for
    item in page["value"]]
    """
    if token is None:
        token = await get_token()
    prepared_request = _prepare_request(
        method,
        url_path,
        api_version,
        token,
        query_parameters=query_parameters,
        base_url=base_url,
    )
    prepared_request["params"].setdefault("$maxpagesize", "1000")

    async with aiohttp.request(**prepared_request) as response:
        await raise_for_status(response)
        response_json = await response.json()
        next_link = response_json.get("nextLink")
        yield response_json

    while next_link:
        async with aiohttp.request(
            method, next_link, headers=prepared_request["headers"]
        ) as response:
            await raise_for_status(response)
            response_json = await response.json()
            next_link = response_json.get("nextLink")
            yield response_json


def azure_rest_api_paged_sync(
    method: str,
    url_path: str,
    api_version: str,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    base_url: Optional[str] = None,
    token: Optional[str] = None,
) -> Iterable[Any]:
    if token is None:
        token = get_token_sync()
    prepared_request = _prepare_request(
        method,
        url_path,
        api_version,
        token,
        query_parameters=query_parameters,
        base_url=base_url,
    )
    prepared_request["params"].setdefault("$maxpagesize", "1000")

    response = requests.request(**prepared_request)
    raise_for_status_sync(response)
    response_json = response.json()
    next_link = response_json.get("nextLink")
    yield response_json

    while next_link:
        response = requests.request(
            method, next_link, headers=prepared_request["headers"]
        )
        raise_for_status_sync(response)
        response_json = response.json()
        next_link = response_json.get("nextLink")
        yield response_json


DEFAULT_TOTAL_POLL_TIMEOUT_SECONDS = 30 * 60  # 30 minutes
# only used if the Retry-After header is missing
DEFAULT_RETRY_AFTER_SECONDS: float = 1


# according to the docs, these are the only permitted completed statues
_SUCCEEDED_STATUS = "Succeeded"
_FAILED_STATUSES = ("Failed", "Canceled")

_POLL_SCHEMES = Literal[
    "AsyncOperationJsonStatus", "LocationStatusCode", "GetProvisioningState"
]


@dataclasses.dataclass(frozen=True)
class _PreparedPollRequest:
    retry_after: float
    poll_request_args: Dict[str, Any]


def _prepare_poll_request(
    poll_scheme: str,
    prev_poll_request: Optional[_PreparedPollRequest],
    response: aiohttp.ClientResponse,
    response_json: Any,
    token: str,
    original_url: str,
    api_version: str,
) -> Optional[_PreparedPollRequest]:
    """
    If prev_poll_request is None, this means that this is the initial response, which is
    treated differently in some schemes.
    """

    # common to all poll schemes

    retry_after_str = response.headers.get("Retry-After")
    if retry_after_str is None:
        retry_after = DEFAULT_RETRY_AFTER_SECONDS
    else:
        try:
            retry_after = float(retry_after_str)
        except ValueError:
            retry_after = DEFAULT_RETRY_AFTER_SECONDS

    headers = {"Authorization": f"Bearer {token}"}

    request_id = response.headers.get("x-ms-request-id")
    if request_id:
        headers["request-id"] = request_id

    if poll_scheme == "AsyncOperationJsonStatus":
        # check to see if further polling is needed
        if prev_poll_request is None:
            # on the initial response, status codes are used to indicate whether polling
            # is needed
            if response.status not in (201, 202):
                return None
        else:
            # on polling response, status code will always be 200, and we need to check
            # the returned content
            if response_json["status"] == "Succeeded":
                return None
            if response_json["status"] in _FAILED_STATUSES:
                raise AzureRestApiError(
                    response.status, response_json["status"], "Failure while polling"
                )

        # prepare the poll request if polling is needed
        if prev_poll_request is None:
            url = response.headers["Azure-AsyncOperation"]
        else:
            # the Azure-AsyncOperation header will only be provided on the initial
            # response. After that, we have to "remember" it from the previous call
            url = prev_poll_request.poll_request_args["url"]
        return _PreparedPollRequest(retry_after, {"url": url, "headers": headers})
    elif poll_scheme == "LocationStatusCode":
        if response.status not in (201, 202):
            return None

        return _PreparedPollRequest(
            retry_after, {"url": response.headers["Location"], "headers": headers}
        )
    elif poll_scheme == "GetProvisioningState":
        provisioning_state = response_json["properties"]["provisioningState"]
        if provisioning_state == _SUCCEEDED_STATUS:
            return None
        elif provisioning_state in _FAILED_STATUSES:
            raise AzureRestApiError(
                response.status, provisioning_state, "Failure while polling"
            )

        return _PreparedPollRequest(
            retry_after,
            {
                "url": original_url,
                "params": {"api-version": api_version},
                "headers": headers,
            },
        )
    else:
        raise ValueError(f"Unexpected poll_scheme {poll_scheme}")


async def azure_rest_api_poll(
    method: str,
    url_path: str,
    api_version: str,
    poll_scheme: _POLL_SCHEMES,
    *,
    query_parameters: Optional[Dict[str, str]] = None,
    json_content: Any = None,
    total_timeout_seconds: int = DEFAULT_TOTAL_POLL_TIMEOUT_SECONDS,
) -> Tuple[Any, Optional[Coroutine[Any, Any, Any]]]:
    """
    Supports any Azure REST API that requires polling. Polling in the Azure REST API is
    very complicated:

    https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/async-operations#create-storage-account-202-with-location-and-retry-after
    The documentation is pretty fuzzy, but it seems like there are in practice 3 polling
    schemes in the Azure API:
    - AsyncOperationJsonStatus: Initial response is 201, has an Azure-AsyncOperation
      header that points us to the polling url, and the content contains information
      about the resource. The polling URL always returns 200, and indicates completion
      by response_json["status"] being a completed status. If polling isn't needed, the
      initial response will be 200. If the documentation for a method says that it can
      return 201, it may use this scheme:
      https://docs.microsoft.com/en-us/rest/api/compute/virtual-machines/create-or-update
      But other methods like
      https://docs.microsoft.com/en-us/rest/api/resources/resource-groups/create-or-update
      return 201, but do not include an Azure-AsyncOperation header. If anything, they
      theoretically fall under the GetProvisioningState scheme, but that example seems
      to always return a completed provisioningState on the initial call.
    - LocationStatusCode: Initial response is 202, has a Location header that points us
      to the polling url, and the content is empty. The polling URL will return 202 with
      empty content if not complete, and 200 once complete with the content containing
      information about the resource. If polling isn't needed, the initial response will
      be 200. It seems like the documentation for a method will say that it can return
      202 if this scheme is used:
      https://docs.microsoft.com/en-us/rest/api/resources/resource-groups/delete
    - GetProvisioningState: Initial response is 200 or 201, does not have any special
      headers. response_json["properties"]["provisioningState"] is populated. We can
      repeatedly call GET on the resource to check this same provisioningState value.
      (This one doesn't seem to be documented, but we've seen it happen in practice.)

    If the initial response indicates completion (i.e. no polling required), then this
    function will return (body of initial call, None). If polling is required, this will
    return (body of initial call, Awaitable[body of final polling call]). Use this with
    wait_for_poll to instead always get back (body of initial call, body of final
    polling call if polling was required).

    total_timeout_seconds indicates how long from start to finish we are willing to wait
    and poll for.
    """

    t0 = time.time()

    token = await get_token()
    prepared_request = _prepare_request(
        method,
        url_path,
        api_version,
        token,
        query_parameters=query_parameters,
        json_content=json_content,
    )

    async with aiohttp.request(**prepared_request) as response:
        await raise_for_status(response)
        response_json = await _return_response(response)

        prepared_poll_request = _prepare_poll_request(
            poll_scheme,
            None,
            response,
            response_json,
            token,
            prepared_request["url"],
            api_version,
        )
        if prepared_poll_request is None:
            # response indicates that there's no need for polling
            return response_json, None

        return response_json, _azure_rest_api_poll_continuation(
            prepared_poll_request,
            poll_scheme,
            token,
            t0,
            total_timeout_seconds,
            prepared_request["url"],
            api_version,
        )


async def _azure_rest_api_poll_continuation(
    prepared_poll_request: _PreparedPollRequest,
    poll_scheme: _POLL_SCHEMES,
    token: str,
    t0: float,
    total_timeout_seconds: int,
    original_url: str,
    api_version: str,
) -> Any:
    # https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/async-operations
    while time.time() - t0 < total_timeout_seconds:
        print(
            f"Waiting for a long-running Azure operation "
            f"({prepared_poll_request.retry_after}s)"
        )
        await asyncio.sleep(prepared_poll_request.retry_after)
        async with aiohttp.request(
            "GET", **prepared_poll_request.poll_request_args
        ) as response:
            await raise_for_status(response)
            response_json = await _return_response(response)

            next_poll_request = _prepare_poll_request(
                poll_scheme,
                prepared_poll_request,
                response,
                response_json,
                token,
                original_url,
                api_version,
            )
            if next_poll_request is None:
                return response_json
            prepared_poll_request = next_poll_request

    raise TimeoutError(
        f"azure_rest_api_poll to {original_url} timed out after {total_timeout_seconds}"
        " seconds"
    )


async def wait_for_poll(
    poll_result: Tuple[Any, Optional[Coroutine[Any, Any, Any]]]
) -> Tuple[Any, Any]:
    """See azure_rest_api_poll"""
    first_result, continuation = poll_result
    if continuation is not None:
        second_result = await continuation
    else:
        second_result = None

    return first_result, second_result


# these functions really belong in azure_identity.py, but they depend on azure_rest_api
# and friends


async def get_tenant_id() -> str:
    """Gets the tenant id corresponding to the subscription from get_subscription_id"""
    # transitively can modify CACHED_AZURE_VARIABLES.subscription_id as well

    if CACHED_AZURE_VARIABLES.tenant_id is None:
        if CACHED_AZURE_VARIABLES.subscription_id is None:
            await get_subscription_id()

        # it's possible that get_subscription_id populated _TENANT_ID in which case we
        # don't need to do anything else
        if CACHED_AZURE_VARIABLES.tenant_id is None:
            # in this case we need to look up the tenant id from the subscription id
            CACHED_AZURE_VARIABLES.tenant_id = (
                await azure_rest_api(
                    "GET",
                    f"subscriptions/{CACHED_AZURE_VARIABLES.subscription_id}",
                    "2019-06-01",
                )
            )["tenantId"]
    return CACHED_AZURE_VARIABLES.tenant_id


async def get_subscription_id() -> str:
    if CACHED_AZURE_VARIABLES.subscription_id is None:
        # first, get_token might populate the subscription and tenant ids, because the
        # CLI token comes with that information
        await get_token()
        if CACHED_AZURE_VARIABLES.subscription_id is None:
            subscriptions = []
            async for page in azure_rest_api_paged(
                "GET", "subscriptions", "2021-01-01"
            ):
                for sub in page["value"]:
                    if sub["state"] == "Enabled":
                        subscriptions.append(sub)

            if len(subscriptions) > 1:
                raise ValueError(
                    "Please specify a subscription via the "
                    "AZURE_SUBSCRIPTION_ID environment variable from among the "
                    "available subscription ids: "
                    + ", ".join([sub["subscriptionId"] for sub in subscriptions])
                )
            elif len(subscriptions) == 0:
                raise ValueError("There are no subscriptions available")
            else:
                CACHED_AZURE_VARIABLES.subscription_id = cast(
                    str, subscriptions[0]["subscriptionId"]
                )
                CACHED_AZURE_VARIABLES.tenant_id = cast(
                    str, subscriptions[0]["tenantId"]
                )

    return CACHED_AZURE_VARIABLES.subscription_id


def get_subscription_id_sync() -> str:
    if CACHED_AZURE_VARIABLES.subscription_id is None:
        # first, get_token might populate the subscription and tenant ids, because the
        # CLI token comes with that information
        get_token_sync()
        if CACHED_AZURE_VARIABLES.subscription_id is None:
            subscriptions = []
            for page in azure_rest_api_paged_sync("GET", "subscriptions", "2021-01-01"):
                for sub in page["value"]:
                    if sub["state"] == "Enabled":
                        subscriptions.append(sub)

            if len(subscriptions) > 1:
                raise ValueError(
                    "Please specify a subscription via the "
                    "AZURE_SUBSCRIPTION_ID environment variable from among the "
                    "available subscription ids: "
                    + ", ".join([sub["subscriptionId"] for sub in subscriptions])
                )
            elif len(subscriptions) == 0:
                raise ValueError("There are no subscriptions available")
            else:
                CACHED_AZURE_VARIABLES.subscription_id = cast(
                    str, subscriptions[0]["subscriptionId"]
                )
                CACHED_AZURE_VARIABLES.tenant_id = cast(
                    str, subscriptions[0]["tenantId"]
                )

    return CACHED_AZURE_VARIABLES.subscription_id


# this is kind of a bit of miscellaneous functionality


def parse_azure_timestamp(s: str) -> datetime.datetime:
    """
    Azure Table timestamps are always in UTC.

    This isn't a simple parse because Azure Table timestamps have more than 1/10^6
    second precision, so we need to chop off the extra digits before python's
    datetime.strptime will work.
    """
    main, sep, frac = s.rpartition(".")
    # frac will be e.g. 0.3535722Z
    if sep != ".":
        raise ValueError(f"Unable to parse Timestamp from Azure table: {s}")

    if frac.endswith("Z"):
        frac = frac[:-1]  # drop the Z, so 0.3535722
    elif frac.endswith("+00:00"):
        frac = frac[:-6]
    else:
        raise ValueError(f"Unable to parse Timestamp from Azure table: {s}")

    frac_rounded = round(float(f"0.{frac}"), 6)  # round to 6 digits, so 0.353572
    frac = str(frac_rounded)[2:]  # drop the 0., so 353572

    # recombine and parse
    return datetime.datetime.strptime(f"{main}.{frac}", "%Y-%m-%dT%H:%M:%S.%f")
