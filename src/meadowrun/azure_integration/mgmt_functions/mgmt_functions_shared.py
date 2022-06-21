"""Code needed both by clean_up and vm_adjust"""

import os

from .azure_core.azure_storage_api import StorageAccount
from .azure_constants import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE,
    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE,
    MEADOWRUN_SUBSCRIPTION_ID,
)


def get_storage_account() -> StorageAccount:
    return StorageAccount(
        os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE],
        os.environ[MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE],
        None,  # this isn't needed by mgmt_functions
    )


def get_resource_group_path() -> str:
    return (
        f"subscriptions/{os.environ[MEADOWRUN_SUBSCRIPTION_ID]}/resourceGroups"
        f"/{MEADOWRUN_RESOURCE_GROUP_NAME}"
    )
