import asyncio.subprocess
import dataclasses
import datetime
import json
import os
import subprocess
import traceback
from typing import Optional, Tuple, Any, Dict

import aiohttp
import requests

from .azure_exceptions import raise_for_status, raise_for_status_sync


IMDS_AUTHORITY = "http://169.254.169.254"
IMDS_TOKEN_PATH = "/metadata/identity/oauth2/token"


@dataclasses.dataclass
class CachedAzureVariables:
    subscription_id: Optional[str]
    tenant_id: Optional[str]


CACHED_AZURE_VARIABLES = CachedAzureVariables(None, None)


_DEFAULT_SCOPE = "https://management.azure.com/.default"


def _scope_to_resource(scope: str) -> str:
    """
    Based on
    https://github.com/Azure/azure-sdk-for-python/blob/83964018f39b7702659d208cd2640f5eea7400fc/sdk/identity/azure-identity/azure/identity/_internal/__init__.py#L97

    which says "Convert an AADv2 scope to an AADv1 resource"
    """
    if scope.endswith("/.default"):
        return scope[: -len("/.default")]
    else:
        return scope


# loosely based on
# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/azure/identity/_credentials/default.py

_CACHED_CLI_TOKEN: Optional[Tuple[str, datetime.datetime]] = None
# 5 minutes makes sense here because get-access-token is guaranteed to always give a
# token that has at least 5 minutes left on it
_CACHED_CLI_TOKEN_CUTOFF = datetime.timedelta(minutes=5)
_MANAGED_IDENTITY_CREDENTIAL: Optional[str] = None


async def get_token(scope: Optional[str] = None) -> str:
    global _MANAGED_IDENTITY_CREDENTIAL, _CACHED_CLI_TOKEN

    if scope is None:
        scope = _DEFAULT_SCOPE

    try:
        if scope == _DEFAULT_SCOPE:
            # If the user is logged in on the command line, will populate
            # _CACHED_CLI_TOKEN, CachedAzureVariables

            # if we don't have a cached token or the token is about to expire:
            if (
                _CACHED_CLI_TOKEN is None
                or _CACHED_CLI_TOKEN[1] - datetime.datetime.now()
                < _CACHED_CLI_TOKEN_CUTOFF
            ):
                token, expires_on, subscription_id, tenant_id = await _get_cli_token(
                    _DEFAULT_SCOPE
                )

                # according to https://github.com/Azure/azure-sdk-for-net/issues/15801
                # this will be a local datetime
                _CACHED_CLI_TOKEN = token, expires_on
                # not sure if this is the right behavior--need to figure out scenarios
                # where the subscription id would change
                if CACHED_AZURE_VARIABLES.subscription_id is None:
                    CACHED_AZURE_VARIABLES.subscription_id = subscription_id
                if CACHED_AZURE_VARIABLES.tenant_id is None:
                    CACHED_AZURE_VARIABLES.tenant_id = tenant_id

            return _CACHED_CLI_TOKEN[0]
        else:
            return (await _get_cli_token(scope))[0]
    except Exception:
        cli_exception = traceback.format_exc()

    try:
        if scope == _DEFAULT_SCOPE:
            if _MANAGED_IDENTITY_CREDENTIAL is None:
                _MANAGED_IDENTITY_CREDENTIAL = await _get_managed_identity_token(scope)

            return _MANAGED_IDENTITY_CREDENTIAL
        else:
            return await _get_managed_identity_token(scope)
    except Exception:
        managed_exception = traceback.format_exc()

    raise ValueError(
        "Unable to get a token for Azure. Error trying to get managed identity token:\n"
        f"{managed_exception}\n"
        f"Error trying to get CLI token:\n{cli_exception}"
    )


def get_token_sync(scope: Optional[str] = None) -> str:
    global _MANAGED_IDENTITY_CREDENTIAL, _CACHED_CLI_TOKEN

    if scope is None:
        scope = _DEFAULT_SCOPE

    try:
        if scope == _DEFAULT_SCOPE:
            if (
                _CACHED_CLI_TOKEN is None
                or _CACHED_CLI_TOKEN[1] - datetime.datetime.now()
                < _CACHED_CLI_TOKEN_CUTOFF
            ):
                token, expires_on, subscription_id, tenant_id = _get_cli_token_sync(
                    _DEFAULT_SCOPE
                )

                _CACHED_CLI_TOKEN = token, expires_on
                if CACHED_AZURE_VARIABLES.subscription_id is None:
                    CACHED_AZURE_VARIABLES.subscription_id = subscription_id
                if CACHED_AZURE_VARIABLES.tenant_id is None:
                    CACHED_AZURE_VARIABLES.tenant_id = tenant_id

            return _CACHED_CLI_TOKEN[0]
        else:
            return _get_cli_token_sync(scope)[0]
    except Exception:
        cli_exception = traceback.format_exc()

    try:
        if scope == _DEFAULT_SCOPE:
            if _MANAGED_IDENTITY_CREDENTIAL is None:
                _MANAGED_IDENTITY_CREDENTIAL = _get_managed_identity_token_sync(scope)

            return _MANAGED_IDENTITY_CREDENTIAL
        else:
            return _get_managed_identity_token_sync(scope)
    except Exception:
        managed_exception = traceback.format_exc()

    raise ValueError(
        "Unable to get a token for Azure. Error trying to get managed identity token:\n"
        f"{managed_exception}\n"
        f"Error trying to get CLI token:\n{cli_exception}"
    )


# roughly based on
# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/azure/identity/_credentials/azure_cli.py

_CLI_TOKEN_EXPIRES_ON_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_CLI_TOKEN_COMMAND_LINE = "az account get-access-token --output json --resource {}"


async def _get_cli_token(scope: str) -> Tuple[str, datetime.datetime, str, str]:
    """Returns token, expires on, subscription id, tenant id"""
    proc = await asyncio.subprocess.create_subprocess_shell(
        _CLI_TOKEN_COMMAND_LINE.format(_scope_to_resource(scope)),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), 10)
    output = stdout.decode()

    if proc.returncode != 0:
        raise ValueError(f"Unable to get CLI token: {output}")

    json_output = json.loads(output)

    return (
        json_output["accessToken"],
        datetime.datetime.strptime(
            json_output["expiresOn"], _CLI_TOKEN_EXPIRES_ON_FORMAT
        ),
        json_output["subscription"],
        json_output["tenant"],
    )


def _get_cli_token_sync(scope: str) -> Tuple[str, datetime.datetime, str, str]:
    result = subprocess.run(
        _CLI_TOKEN_COMMAND_LINE.format(_scope_to_resource(scope)),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    output = result.stdout.decode()

    if result.returncode != 0:
        raise ValueError(f"Unable to get CLI token: {output}")

    json_output = json.loads(output)

    return (
        json_output["accessToken"],
        datetime.datetime.strptime(
            json_output["expiresOn"], _CLI_TOKEN_EXPIRES_ON_FORMAT
        ),
        json_output["subscription"],
        json_output["tenant"],
    )


# based on
# https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/azure/identity/_credentials/managed_identity.py


class EnvironmentVariables:
    AZURE_CLIENT_ID = "AZURE_CLIENT_ID"
    AZURE_TENANT_ID = "AZURE_TENANT_ID"

    AZURE_POD_IDENTITY_AUTHORITY_HOST = "AZURE_POD_IDENTITY_AUTHORITY_HOST"
    IDENTITY_ENDPOINT = "IDENTITY_ENDPOINT"
    IDENTITY_HEADER = "IDENTITY_HEADER"
    IDENTITY_SERVER_THUMBPRINT = "IDENTITY_SERVER_THUMBPRINT"
    IMDS_ENDPOINT = "IMDS_ENDPOINT"
    MSI_ENDPOINT = "MSI_ENDPOINT"
    MSI_SECRET = "MSI_SECRET"

    AZURE_AUTHORITY_HOST = "AZURE_AUTHORITY_HOST"

    AZURE_FEDERATED_TOKEN_FILE = "AZURE_FEDERATED_TOKEN_FILE"
    TOKEN_EXCHANGE_VARS = (
        AZURE_AUTHORITY_HOST,
        AZURE_TENANT_ID,
        AZURE_FEDERATED_TOKEN_FILE,
    )


def _get_managed_identity_token_helper(scope: str) -> Tuple[Dict[str, Any], bool]:
    scope = _scope_to_resource(scope)

    method = "GET"
    data = None
    disable_verify = False
    parameters = {}

    azure_client_id = os.environ.get(EnvironmentVariables.AZURE_CLIENT_ID)
    if azure_client_id is not None:
        parameters["client_id"] = azure_client_id

    if os.environ.get(EnvironmentVariables.IDENTITY_ENDPOINT):
        if os.environ.get(EnvironmentVariables.IDENTITY_HEADER):
            if os.environ.get(EnvironmentVariables.IDENTITY_SERVER_THUMBPRINT):
                # not tested: ServiceFabricCredential
                url = os.environ.get(EnvironmentVariables.IDENTITY_ENDPOINT)
                secret = os.environ.get(EnvironmentVariables.IDENTITY_HEADER)
                # thumbprint = os.environ.get(
                #     EnvironmentVariables.IDENTITY_SERVER_THUMBPRINT)
                # instead of using the thumbprint, it seems like we just ignore the
                # connection
                disable_verify = True
                headers = {"Secret": secret}
                parameters.update(
                    {"api-version": "2019-07-01-preview", "resource": scope}
                )
            else:
                # AppServiceCredential
                url = os.environ.get(EnvironmentVariables.IDENTITY_ENDPOINT)
                secret = os.environ.get(EnvironmentVariables.IDENTITY_HEADER)
                headers = {"X-IDENTITY-HEADER": secret}
                parameters.update({"api-version": "2019-08-01", "resource": scope})
        elif os.environ.get(EnvironmentVariables.IMDS_ENDPOINT):
            # not tested: AzureArcCredential
            raise NotImplementedError("AzureArcCredential is not implemented")
        else:
            # not tested: CloudShellCredential
            method = "POST"
            url = os.environ.get(EnvironmentVariables.MSI_ENDPOINT)
            headers = {"Metadata": "true"}
            data = {"resource": scope}
    elif os.environ.get(EnvironmentVariables.MSI_ENDPOINT):
        if os.environ.get(EnvironmentVariables.MSI_SECRET):
            # not tested: AzureMLCredential
            url = os.environ.get(EnvironmentVariables.MSI_ENDPOINT)
            secret = os.environ.get(EnvironmentVariables.MSI_SECRET)
            headers = {"secret": secret}
            parameters.update({"api-version": "2017-09-01", "resource": scope})
        else:
            # not tested: CloudShellCredential
            method = "POST"
            url = os.environ.get(EnvironmentVariables.MSI_ENDPOINT)
            headers = {"Metadata": "true"}
            data = {"resource": scope}
    elif all(os.environ.get(var) for var in EnvironmentVariables.TOKEN_EXCHANGE_VARS):
        raise NotImplementedError("Need to implement TokenExchangeCredential")
    else:
        url = (
            os.environ.get(
                EnvironmentVariables.AZURE_POD_IDENTITY_AUTHORITY_HOST, IMDS_AUTHORITY
            ).strip("/")
            + IMDS_TOKEN_PATH
        )
        parameters.update({"api-version": "2018-02-01", "resource": scope})
        headers = {"Metadata": "true"}

    return {
        "method": method,
        "url": url,
        "params": parameters,
        "headers": headers,
        "data": data,
    }, disable_verify


# on WSL, the IMDS endpoint will hang rather than failing to connect immediately, so put
# a relatively short timeout as it generally shouldn't require a long time to connect
_MANAGED_IDENTITY_TIMEOUT_SECS = 1


async def _get_managed_identity_token(scope: str) -> str:
    request_parameters, disable_verify = _get_managed_identity_token_helper(scope)
    if disable_verify:
        connector = aiohttp.TCPConnector(verify_ssl=False)
    else:
        connector = None

    async with aiohttp.request(
        **request_parameters,
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=_MANAGED_IDENTITY_TIMEOUT_SECS),
    ) as response:
        await raise_for_status(response)
        return (await response.json())["access_token"]


def _get_managed_identity_token_sync(scope: str) -> str:
    request_parameters, disable_verify = _get_managed_identity_token_helper(scope)
    if disable_verify:
        request_parameters["verify"] = False

    response = requests.request(
        **request_parameters, timeout=_MANAGED_IDENTITY_TIMEOUT_SECS
    )
    raise_for_status_sync(response)
    return response.json()["access_token"]


async def get_current_user_id() -> str:
    """
    This functions gets the id of the current user that is signed in to the Azure CLI.

    In order to get this information, it looks like there are two different services,
    "Microsoft Graph" (developer.microsoft.com/graph) and "Azure AD Graph"
    (graph.windows.net), the latter being deprecated
    (https://devblogs.microsoft.com/microsoft365dev/microsoft-graph-or-azure-ad-graph/).
    I think these services correspond to two different python libraries, msal
    (https://docs.microsoft.com/en-us/python/api/overview/azure/active-directory?view=azure-python)
    and adal (https://docs.microsoft.com/en-us/python/api/adal/adal?view=azure-python),
    but these libraries don't appear to do anything super useful on their own.

    The deprecated Azure Graph API seems to correspond to a higher-level library
    azure-graphrbac, which does seem to have the functionality we need:
    azure.graphrbac.GraphRbacManagementClient.signed_in_user, but is deprecated along
    with Azure Graph
    (https://github.com/Azure/azure-sdk-for-python/issues/14022#issuecomment-752279618).

    The msgraph library that we use here seems to be a not-very-high-level library
    for Microsoft Graph (https://github.com/microsoftgraph/msgraph-sdk-python-core).

    As a side note, another way to get this information is to use the command line to
    call `az ad signed-in-user show`, but that appears to be relying on the deprecated
    Azure Graph API as it gives a deprecation warning.
    """

    token = await get_token("https://graph.microsoft.com")
    # https://docs.microsoft.com/en-us/graph/api/user-get?view=graph-rest-1.0&tabs=http
    async with aiohttp.request(
        "GET",
        "https://graph.microsoft.com/v1.0/me",
        headers={
            "Authorization": f"Bearer {token}",
        },
    ) as response:
        response.raise_for_status()
        return (await response.json())["id"]
