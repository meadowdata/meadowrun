"""
Things that should be in azure_instance_allocation but are also needed by the code in
mgmt_functions
"""

# this should almost never be used directly instead of ensure_meadowrun_resource_group.
from __future__ import annotations

from typing import cast, Literal

import azure.identity.aio
from azure.core.credentials_async import AsyncTokenCredential


MEADOWRUN_RESOURCE_GROUP_NAME = "Meadowrun-rg"

VM_ALLOC_TABLE_NAME = "meadowrunVmAlloc"
LAST_USED_TABLE_NAME = "meadowrunLastUsed"

LAST_USED = "last_used"
RESOURCE_TYPES_TYPE = Literal["GRID_TASK_QUEUE", "CONTAINER_IMAGE"]
GRID_TASK_QUEUE: RESOURCE_TYPES_TYPE = "GRID_TASK_QUEUE"
CONTAINER_IMAGE: RESOURCE_TYPES_TYPE = "CONTAINER_IMAGE"

_REQUEST_QUEUE_NAME_PREFIX = "mrgtrequest"
_RESULT_QUEUE_NAME_PREFIX = "mrgtresult"

QUEUE_NAME_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

ALLOCATED_TIME = "allocated_time"
JOB_ID = "job_id"
LAST_UPDATE_TIME = "last_update_time"
LOGICAL_CPU_ALLOCATED = "logical_cpu_allocated"
LOGICAL_CPU_AVAILABLE = "logical_cpu_available"
MEMORY_GB_ALLOCATED = "memory_gb_allocated"
MEMORY_GB_AVAILABLE = "memory_gb_available"
RUNNING_JOBS = "running_jobs"
SINGLE_PARTITION_KEY = "_"
VM_NAME = "vm_name"


_DEFAULT_CREDENTIAL_OPTIONS = {
    "exclude_visual_studio_code_credential": True,
    "exclude_environment_credential": True,
    "exclude_shared_token_cache_credential": True,
}


def get_credential_aio() -> AsyncTokenCredential:
    return cast(
        AsyncTokenCredential,
        azure.identity.aio.DefaultAzureCredential(**_DEFAULT_CREDENTIAL_OPTIONS),
    )


MEADOWRUN_STORAGE_ACCOUNT_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT"
MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT_KEY"
MEADOWRUN_SUBSCRIPTION_ID = "MEADOWRUN_SUBSCRIPTION_ID"

_MEADOWRUN_GENERATED_DOCKER_REPO = "meadowrun_generated"


def meadowrun_registry_name(subscription_id: str) -> str:
    """
    Azure Container registry names must be 5 to 50 characters, alphanumeric, and
    globally unique.
    """
    return "mr" + subscription_id.replace("-", "")
