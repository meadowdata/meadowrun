"""
Constants needed both by mgmt_functions and the "regular" code outside of
mgmt_functions.
"""

from __future__ import annotations

from typing_extensions import Literal

# this should almost never be used directly instead of ensure_meadowrun_resource_group.
MEADOWRUN_RESOURCE_GROUP_NAME = "Meadowrun-rg"

VM_ALLOC_TABLE_NAME = "meadowrunVmAlloc"
LAST_USED_TABLE_NAME = "meadowrunLastUsed"

RESOURCE_TYPES_TYPE = Literal["GRID_TASK_QUEUE", "CONTAINER_IMAGE"]
GRID_TASK_QUEUE: RESOURCE_TYPES_TYPE = "GRID_TASK_QUEUE"
CONTAINER_IMAGE: RESOURCE_TYPES_TYPE = "CONTAINER_IMAGE"

_REQUEST_QUEUE_NAME_PREFIX = "mrgtrequest"
_RESULT_QUEUE_NAME_PREFIX = "mrgtresult"

QUEUE_NAME_TIMESTAMP_FORMAT = "%Y%m%d%H%M%S"

ALLOCATED_TIME = "allocated_time"
JOB_ID = "job_id"
LAST_UPDATE_TIME = "last_update_time"
RESOURCES_AVAILABLE = "resources_available"
NON_CONSUMABLE_RESOURCES = "non_consumable_resources"
RESOURCES_ALLOCATED = "resources_allocated"
RUNNING_JOBS = "running_jobs"
PREVENT_FURTHER_ALLOCATION = "prevent_further_allocation"
SINGLE_PARTITION_KEY = "_"
VM_NAME = "vm_name"


MEADOWRUN_STORAGE_ACCOUNT_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT"
MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE = "MEADOWRUN_STORAGE_ACCOUNT_KEY"
MEADOWRUN_SUBSCRIPTION_ID = "MEADOWRUN_SUBSCRIPTION_ID"

_MEADOWRUN_GENERATED_DOCKER_REPO = "meadowrun_generated"


def meadowrun_container_registry_name(subscription_id: str) -> str:
    """
    Azure Container registry names must be 5 to 50 characters, alphanumeric, and
    globally unique.
    """
    return "mr" + subscription_id.replace("-", "")
