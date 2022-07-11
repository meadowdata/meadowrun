from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING, Optional, Tuple, List, Any

if TYPE_CHECKING:
    from typing_extensions import Literal

import aiohttp

from .azure_exceptions import AzureRestApiError, raise_for_status
from .azure_identity import get_token
from .azure_rest_api import (
    azure_rest_api,
    azure_rest_api_paged,
    get_tenant_id,
)


async def get_acr_token(
    registry_name: str,
    token_type: Literal["refresh", "access"],
    method: Optional[str] = None,
    url_path: Optional[str] = None,
) -> str:
    """
    Gets tokens for interacting with the specified Azure Container Registry. Based on an
    incomplete understanding of OAuth:

    If token_type == "refresh", then this function will exchange the "default" Azure
    token from get_token for a container registry-specific refresh token and then return
    that.

    If token_type == "access", then this function will get the refresh token as
    described above, and then get a container registry-specific access token and return
    that. Getting an access token requires a scope, which we'll only have if
    method/url_path are provided. method/url_path should be what you will provide to
    azure_rest_api for the actual API call you want to make (i.e. url_path is everything
    after the domain name in the URL). We use this method/url_path to get a
    WWW-Authenticate header which will have a scope. (For refresh tokens, scopes aren't
    necessary, so we just query a generic endpoint that gives us the rest of the
    WWW-Authenticate header without the scope, which we won't need.)

    Confusingly, the original "default" Azure token appears to also be called an access
    token, but this is distinct from the container registry-specific access token
    returned by this function.
    """

    # References to the official Azure python SDK implementation
    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L115
    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli-core/azure/cli/core/_profile.py#L349
    # https://github.com/Azure/azure-cli/blob/02cb00efbf661a1c18bab4a1fe03f2b1c59e30ef/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L365
    # https://github.com/Azure/azure-cli/blob/02cb00efbf661a1c18bab4a1fe03f2b1c59e30ef/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L403

    orig_token = await get_token()

    realm, service, scope = await _get_auth_parameters(registry_name, method, url_path)
    realm_url = urllib.parse.urlparse(realm)

    auth_host_exchange = urllib.parse.urlunparse(
        (realm_url[0], realm_url[1], "/oauth2/exchange", "", "", "")
    )
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    content_exchange = {
        # I think this refers to the type of token we're providing below, not the type
        # of token we're requesting
        "service": service,
        "tenant": await get_tenant_id(),
        "grant_type": "access_token",
        "access_token": orig_token,
    }
    async with aiohttp.request(
        "POST",
        auth_host_exchange,
        data=urllib.parse.urlencode(content_exchange),
        headers=headers,
    ) as response:
        response.raise_for_status()
        refresh_token = (await response.json())["refresh_token"]

    if token_type == "refresh":
        return refresh_token

    if scope is None:
        raise ValueError(
            "Requested an access token, but there was no scope in the WWW-Authenticate "
            "header. This probably means you did not pass in an appropriate url_path to"
            " _get_token"
        )
    auth_host_access = urllib.parse.urlunparse(
        (realm_url[0], realm_url[1], "/oauth2/token", "", "", "")
    )
    content_access = {
        "service": service,
        "scope": scope,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    async with aiohttp.request(
        "POST",
        auth_host_access,
        data=urllib.parse.urlencode(content_access),
        headers=headers,
    ) as response:
        await raise_for_status(response)
        access_token = (await response.json())["access_token"]

    if token_type == "access":
        return access_token

    raise ValueError(f"Unexpected value for token_type: {token_type}")


async def _get_auth_parameters(
    registry_name: str, method: Optional[str] = None, url_path: Optional[str] = None
) -> Tuple[str, str, Optional[str]]:
    """
    Returns realm, service, scope. Basically just parses the WWW-Authenticate header
    based on method/url_path. Supplies a reasonable default method/url_path if None are
    provided.
    """

    # https://github.com/Azure/azure-cli/blob/284d6131679cecc26f928c78f1e4c29e2dd66405/src/azure-cli/azure/cli/command_modules/acr/_docker_utils.py#L63

    if method is None:
        method = "GET"
    if url_path is None:
        url_path = "v2/"

    async with aiohttp.request(
        method, f"https://{registry_name}.azurecr.io/{url_path}"
    ) as response:
        if response.status != 401:
            raise ValueError(
                f"Unexpected response from ACR login server, status {response.status}"
            )
        if "WWW-Authenticate" not in response.headers:
            raise ValueError(
                "Unexpected response from ACR login server, no WWW-Authenticate header"
            )
        authenticate = response.headers["WWW-Authenticate"]

    auth_type, sep, auth_parameters = authenticate.partition(" ")
    if sep != " ":
        raise ValueError(
            "Unexpected response from ACR login server, WWW-Authenticate header has no "
            "space"
        )
    if auth_type.lower() != "bearer":
        raise ValueError(
            "Unexpected response from ACR login server, WWW-Authenticate header does "
            f"not start with Bearer, instead starts with {auth_type}"
        )
    auth_parameters_dict = {}
    for pair in auth_parameters.split(","):
        key, sep, value = pair.partition("=")
        if sep != "=":
            raise ValueError(
                "Unexpected response from ACR login server. Auth parameters were not in"
                f" the expected format: {pair}"
            )
        auth_parameters_dict[key] = value.strip('"')

    if "realm" not in auth_parameters or "service" not in auth_parameters:
        raise ValueError(
            "Unexpected response from ACR login server, both realm and service auth "
            "parameter expected"
        )

    return (
        auth_parameters_dict["realm"],
        auth_parameters_dict["service"],
        auth_parameters_dict.get("scope"),
    )


# high-level ACR APIs


async def get_tags_in_repository(registry_name: str, repository: str) -> List[Any]:
    """
    Lists the tags in the specified repository. See
    https://docs.microsoft.com/en-us/rest/api/containerregistry/tag/get-list for
    documentation on what is returned. Returns a list of TagAttributesBase
    """
    method, url_path = "GET", f"acr/v1/{repository}/_tags"

    token = await get_acr_token(
        registry_name, "access", method=method, url_path=url_path
    )

    results = []
    try:
        async for page in azure_rest_api_paged(
            method,
            url_path,
            "",
            base_url=f"https://{registry_name}.azurecr.io",
            token=token,
        ):
            tags = page.get("tags")
            if tags:
                results.extend(tags)
    except AzureRestApiError as exn:
        # repository does not exist, i.e. no image has been pushed yet.
        if exn.status != 404:
            raise
    return results


async def delete_tag(registry_name: str, repository: str, tag: str) -> None:
    """
    Deletes the specified tag. See
    https://docs.microsoft.com/en-us/rest/api/containerregistry/tag/delete
    """
    method, url_path = "DELETE", f"acr/v1/{repository}/_tags/{tag}"

    token = await get_acr_token(
        registry_name, "access", method=method, url_path=url_path
    )

    await azure_rest_api(
        method,
        url_path,
        "2021-07-01",
        base_url=f"https://{registry_name}.azurecr.io",
        token=token,
    )
