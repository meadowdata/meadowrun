"""
This code cannot reference anything outside of mgmt_functions (as that's what gets
uploaded to the Azure function). We use relative imports which will work both in the
"regular" environment as well as in the Azure function
"""

import asyncio
import datetime
import logging
import os
from typing import Any, Dict, List

import aiohttp

from ..azure_core.azure_exceptions import (
    AzureRestApiError,
)

from ..azure_core.azure_acr import get_tags_in_repository, delete_tag
from ..azure_core.azure_rest_api import (
    azure_rest_api_paged,
    azure_rest_api,
    parse_azure_timestamp,
)
from ..azure_core.azure_storage_api import (
    StorageAccount,
    azure_table_api,
    azure_table_api_paged,
    table_key_url,
)
from ..azure_constants import (
    CONTAINER_IMAGE,
    GRID_TASK_QUEUE,
    LAST_USED_TABLE_NAME,
    MEADOWRUN_SUBSCRIPTION_ID,
    QUEUE_NAME_TIMESTAMP_FORMAT,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    _REQUEST_QUEUE_NAME_PREFIX,
    _RESULT_QUEUE_NAME_PREFIX,
    RESOURCE_TYPES_TYPE,
    meadowrun_container_registry_name,
)
from ..mgmt_functions_shared import get_storage_account, get_resource_group_path

# a queue that has been inactive for this time will get cleaned up
_QUEUE_INACTIVE_TIME = datetime.timedelta(hours=4)
# a container image has not been used for this time will get cleaned up
_CONTAINER_IMAGE_UNUSED_TIME = datetime.timedelta(days=4)


async def _get_last_used_records(
    storage_account: StorageAccount, resource_type: RESOURCE_TYPES_TYPE
) -> Dict[Any, datetime.datetime]:
    try:
        last_used_records = {
            item["RowKey"]: parse_azure_timestamp(item["Timestamp"])
            async for page in azure_table_api_paged(
                "GET",
                storage_account,
                f"{LAST_USED_TABLE_NAME}()",
                query_parameters={"$filter": f"PartitionKey eq '{resource_type}'"},
            )
            for item in page["value"]
        }
        return last_used_records
    except AzureRestApiError as exn:
        if exn.status != 404 or exn.code != "TableNotFound":
            raise
        return {}


async def delete_old_task_queues() -> List[str]:
    """See _deregister_and_terminate_vms for why we return log statements"""
    logs = []

    storage_account = get_storage_account()
    resource_group_path = get_resource_group_path()

    # the last used records are keyed off of the job_id for the queue, whereas the queue
    # names are in the form of {prefix}-{job_id}-{created_timestamp}

    now = datetime.datetime.utcnow()
    delete_tasks = []

    last_used_records = await _get_last_used_records(storage_account, GRID_TASK_QUEUE)

    surviving_job_ids = set()
    deleted_job_ids = set()

    queues_path = (
        f"{resource_group_path}/providers/Microsoft.Storage/storageAccounts"
        f"/{storage_account.name}/queueServices/default/queues"
    )

    async for page in azure_rest_api_paged("GET", queues_path, "2021-09-01"):
        for queue in page["value"]:
            queue_name = queue["name"]
            if not queue_name.startswith(
                _REQUEST_QUEUE_NAME_PREFIX
            ) and not queue_name.startswith(_RESULT_QUEUE_NAME_PREFIX):
                # this is not a meadowrun grid task queue
                continue

            # first parse the queue names, while deleting any queues that don't fit the
            # expected patterns

            prefix, sep, remainder = queue_name.partition("-")
            job_id, sep, created_timestamp_string = remainder.rpartition("-")
            if sep != "-":
                logs.append(
                    "Queue name was not in the expected prefix-job_id-timestamp format:"
                    f" {queue_name}, deleting it"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        azure_rest_api(
                            "DELETE", f"{queues_path}/{queue_name}", "2021-09-01"
                        )
                    )
                )
                continue

            # next, if we have a last-used record that's too old, delete the queue,
            # otherwise mark it as a "surviving" queue so that we know to keep the
            # last_used record around
            if job_id in last_used_records:
                last_used = last_used_records[job_id]
                if now - last_used > _QUEUE_INACTIVE_TIME:
                    logs.append(
                        f"Queue {queue_name} was last used at {last_used}, deleting"
                    )
                    deleted_job_ids.add(job_id)
                    delete_tasks.append(
                        asyncio.create_task(
                            azure_rest_api(
                                "DELETE", f"{queues_path}/{queue_name}", "2021-09-01"
                            )
                        )
                    )
                else:
                    surviving_job_ids.add(job_id)
                continue

            # finally, we don't have any last-used records, so we have to use the
            # created timestamp
            try:
                created_timestamp = datetime.datetime.strptime(
                    created_timestamp_string, QUEUE_NAME_TIMESTAMP_FORMAT
                )
            except ValueError:
                logs.append(
                    f"Queue name {queue_name} is in the format prefix-job_id-timestamp,"
                    " but the timestamp cannot be parsed, deleting the queue"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        azure_rest_api(
                            "DELETE", f"{queues_path}/{queue_name}", "2021-09-01"
                        )
                    )
                )
                continue

            if now - created_timestamp > _QUEUE_INACTIVE_TIME:
                logs.append(
                    f"Queue {queue_name} has no last used records and was created at "
                    f"{created_timestamp}, deleting"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        azure_rest_api(
                            "DELETE", f"{queues_path}/{queue_name}", "2021-09-01"
                        )
                    )
                )
                continue

    # now delete last_used records that don't correspond to any existing queues
    for job_id in last_used_records.keys():
        if job_id in surviving_job_ids:
            continue

        if job_id not in deleted_job_ids:
            logs.append(
                f"job_id {job_id} has a last_used record, but no existing queues, "
                "deleting the last_used record now"
            )

        # if we did delete the corresponding queue, still delete the last_used
        # record, just no need to log
        delete_tasks.append(
            asyncio.create_task(
                azure_table_api(
                    "DELETE",
                    storage_account,
                    table_key_url(LAST_USED_TABLE_NAME, GRID_TASK_QUEUE, job_id),
                    additional_headers={"If-Match": "*"},
                )
            )
        )

    if delete_tasks:
        await asyncio.wait(delete_tasks)

    return logs


async def delete_unused_images() -> List[str]:
    """See _deregister_and_terminate_vms for why we return log statements"""
    logs = []

    storage_account = get_storage_account()

    registry_name = meadowrun_container_registry_name(
        os.environ[MEADOWRUN_SUBSCRIPTION_ID]
    )

    delete_tasks = []

    now = datetime.datetime.now()

    last_used_records = await _get_last_used_records(storage_account, CONTAINER_IMAGE)

    deleted_tags = set()
    surviving_tags = set()

    try:
        tags = await get_tags_in_repository(
            registry_name, _MEADOWRUN_GENERATED_DOCKER_REPO
        )
    except aiohttp.ClientConnectorError:
        # assume the registry does not exist => no tags
        tags = []

    for tag in tags:
        tag_name = tag["name"]

        # first see if we have a last used record for this tag
        if tag_name in last_used_records:
            last_used = last_used_records[tag_name]
            if now - last_used > _CONTAINER_IMAGE_UNUSED_TIME:
                logs.append(
                    f"Image {tag_name} will be deleted, was last used at {last_used}"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        delete_tag(
                            registry_name, _MEADOWRUN_GENERATED_DOCKER_REPO, tag_name
                        )
                    )
                )
                deleted_tags.add(tag_name)
            else:
                surviving_tags.add(tag_name)
            continue

        # if we don't have a last used record, use the last_updated property
        if (
            now - parse_azure_timestamp(tag["lastUpdateTime"])
            > _CONTAINER_IMAGE_UNUSED_TIME
        ):
            logs.append(
                f"Image {tag_name} will be deleted, has not been used and last "
                f"updated at {tag['lastUpdateTime']}"
            )
            delete_tasks.append(
                asyncio.create_task(
                    delete_tag(
                        registry_name, _MEADOWRUN_GENERATED_DOCKER_REPO, tag_name
                    )
                )
            )

    for tag_name in last_used_records.keys():
        if tag_name in surviving_tags:
            continue

        if tag_name not in deleted_tags:
            logs.append(
                f"Image {tag_name} has a last_used record but the image does not "
                "exist. Deleting the last_used record now"
            )
        # if we did delete the corresponding image, still delete the last_used
        # record, just no need to log
        delete_tasks.append(
            asyncio.create_task(
                azure_table_api(
                    "DELETE",
                    storage_account,
                    table_key_url(LAST_USED_TABLE_NAME, CONTAINER_IMAGE, tag_name),
                    additional_headers={"If-Match": "*"},
                )
            )
        )

    if delete_tasks:
        await asyncio.wait(delete_tasks)

    return logs


def main(myTimer) -> None:  # type: ignore
    """
    The entry point for the Azure function (see function.json). Configured to run every
    minute by default.

    myTime should be annotated as azure.functions.TimerRequest, but we'd rather not add
    that dependency (it exists by default in the function runtime where this function
    will actually run). Also, the variable cannot be snake_case, and can only be changed
    if the corresponding name in function.json is changed.
    """
    logs = asyncio.run(delete_old_task_queues())
    for log in logs:
        logging.info(log)
    logs = asyncio.run(delete_unused_images())
    for log in logs:
        logging.info(log)
