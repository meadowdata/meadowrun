"""
Things that should be in azure_instance_allocation but are also needed by the code in
mgmt_functions
"""

# this should almost never be used directly instead of ensure_meadowrun_resource_group.
from __future__ import annotations

from typing import cast

import azure.identity.aio
from azure.core.credentials_async import AsyncTokenCredential


MEADOWRUN_RESOURCE_GROUP_NAME = "Meadowrun-rg"

VM_ALLOC_TABLE_NAME = "meadowrunVmAlloc"

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
