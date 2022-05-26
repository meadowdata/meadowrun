"""
This code cannot reference anything outside of mgmt_functions (as that's what gets
uploaded to the Azure function). We use relative imports which will work both in the
"regular" environment as well as in the Azure function
"""

import asyncio
import datetime
import logging
import os
from typing import List

from azure.containerregistry import ArtifactTagProperties
from azure.containerregistry.aio import ContainerRegistryClient
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables.aio import TableClient
from azure.mgmt.storage.aio import StorageManagementClient

from ..azure_instance_alloc_stub import (
    CONTAINER_IMAGE,
    GRID_TASK_QUEUE,
    LAST_USED_TABLE_NAME,
    MEADOWRUN_RESOURCE_GROUP_NAME,
    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE,
    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE,
    MEADOWRUN_SUBSCRIPTION_ID,
    QUEUE_NAME_TIMESTAMP_FORMAT,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    _REQUEST_QUEUE_NAME_PREFIX,
    _RESULT_QUEUE_NAME_PREFIX,
    get_credential_aio,
    meadowrun_registry_name,
)

# a queue that has been inactive for this time will get cleaned up
_QUEUE_INACTIVE_TIME = datetime.timedelta(hours=4)
# a container image has not been used for this time will get cleaned up
_CONTAINER_IMAGE_UNUSED_TIME = datetime.timedelta(days=4)


async def delete_old_task_queues() -> List[str]:
    """See _deregister_and_terminate_vms for why we return log statements"""
    logs = []

    storage_account_name = os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE]
    storage_account_key = os.environ[MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE]
    async with TableClient(
        f"https://{storage_account_name}.table.core.windows.net/",
        LAST_USED_TABLE_NAME,
        credential=AzureNamedKeyCredential(storage_account_name, storage_account_key),
    ) as table_client, StorageManagementClient(
        get_credential_aio(), os.environ[MEADOWRUN_SUBSCRIPTION_ID]
    ) as queue_client:
        # the last used records are keyed off of the job_id for the queue, whereas
        # the queue names are in the form of {prefix}-{job_id}-{created_timestamp}

        now = datetime.datetime.utcnow()
        now_with_timezone = datetime.datetime.now(datetime.timezone.utc)
        delete_tasks = []

        last_used_records = {
            item["RowKey"]: item.metadata["timestamp"]
            async for item in table_client.query_entities(
                f"PartitionKey eq '{GRID_TASK_QUEUE}'"
            )
        }

        surviving_job_ids = set()
        deleted_job_ids = set()

        async for queue in queue_client.queue.list(
            MEADOWRUN_RESOURCE_GROUP_NAME,
            os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE],
        ):
            if not queue.name.startswith(
                _REQUEST_QUEUE_NAME_PREFIX
            ) and not queue.name.startswith(_RESULT_QUEUE_NAME_PREFIX):
                # this is not a meadowrun grid task queue
                continue

            # first parse the queue names, while deleting any queues that don't fit the
            # expected patterns

            prefix, sep, remainder = queue.name.partition("-")
            job_id, sep, created_timestamp_string = remainder.rpartition("-")
            if sep != "-":
                logs.append(
                    "Queue name was not in the expected prefix-job_id-timestamp format:"
                    f" {queue.name}, deleting it"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        queue_client.queue.delete(
                            MEADOWRUN_RESOURCE_GROUP_NAME,
                            storage_account_name,
                            queue.name,
                        )
                    )
                )
                continue

            # next, if we have a last-used record that's too old, delete the queue,
            # otherwise mark it as a "surviving" queue so that we know to keep the
            # last_used record around
            if job_id in last_used_records:
                last_used = last_used_records[job_id]
                if now_with_timezone - last_used > _QUEUE_INACTIVE_TIME:
                    logs.append(
                        f"Queue {queue.name} was last used at {last_used}, deleting"
                    )
                    deleted_job_ids.add(job_id)
                    delete_tasks.append(
                        asyncio.create_task(
                            queue_client.queue.delete(
                                MEADOWRUN_RESOURCE_GROUP_NAME,
                                storage_account_name,
                                queue.name,
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
                    f"Queue name {queue.name} is in the format prefix-job_id-timestamp,"
                    " but the timestamp cannot be parsed, deleting the queue"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        queue_client.queue.delete(
                            MEADOWRUN_RESOURCE_GROUP_NAME,
                            storage_account_name,
                            queue.name,
                        )
                    )
                )
                continue

            if now - created_timestamp > _QUEUE_INACTIVE_TIME:
                logs.append(
                    f"Queue {queue.name} has no last used records and was created at "
                    f"{created_timestamp}, deleting"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        queue_client.queue.delete(
                            MEADOWRUN_RESOURCE_GROUP_NAME,
                            storage_account_name,
                            queue.name,
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
                asyncio.create_task(table_client.delete_entity(GRID_TASK_QUEUE, job_id))
            )

        if delete_tasks:
            await asyncio.wait(delete_tasks)

    return logs


async def delete_unused_images() -> List[str]:
    """See _deregister_and_terminate_vms for why we return log statements"""
    logs = []

    storage_account_name = os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE]
    storage_account_key = os.environ[MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE]
    registry_name = meadowrun_registry_name(os.environ[MEADOWRUN_SUBSCRIPTION_ID])
    async with TableClient(
        f"https://{storage_account_name}.table.core.windows.net/",
        LAST_USED_TABLE_NAME,
        credential=AzureNamedKeyCredential(storage_account_name, storage_account_key),
    ) as table_client, ContainerRegistryClient(
        f"{registry_name}.azurecr.io",
        get_credential_aio(),
        audience="https://management.azure.com",
    ) as acr_client:
        delete_tasks = []

        now = datetime.datetime.now(datetime.timezone.utc)

        last_used_records = {
            item["RowKey"]: item.metadata["timestamp"]
            async for item in table_client.query_entities(
                f"PartitionKey eq '{CONTAINER_IMAGE}'"
            )
        }

        deleted_tags = set()
        surviving_tags = set()

        async for tag in acr_client.list_tag_properties(
            _MEADOWRUN_GENERATED_DOCKER_REPO,
            # copied and modified from
            # azure.containerregistry.aio._async_container_registry_client.py:474 Can be
            # deleted when
            # https://github.com/Azure/azure-sdk-for-python/pull/24621/files is merged
            cls=lambda objs: [
                ArtifactTagProperties._from_generated(
                    o, repository=_MEADOWRUN_GENERATED_DOCKER_REPO  # type: ignore
                )
                for o in objs
            ]
            if objs
            else [],
        ):
            # first see if we have a last used record for this tag
            if tag.name in last_used_records:
                last_used = last_used_records[tag.name]
                if now - last_used > _CONTAINER_IMAGE_UNUSED_TIME:
                    logs.append(
                        f"Image {tag.name} will be deleted, was last used at "
                        f"{last_used}"
                    )
                    delete_tasks.append(
                        asyncio.create_task(
                            acr_client.delete_tag(
                                _MEADOWRUN_GENERATED_DOCKER_REPO, tag.name
                            )
                        )
                    )
                    deleted_tags.add(tag.name)
                else:
                    surviving_tags.add(tag.name)
                continue

            # if we don't have a last used record, use the last_updated property
            if now - tag.last_updated_on > _CONTAINER_IMAGE_UNUSED_TIME:
                logs.append(
                    f"Image {tag.name} will be deleted, has not been used and last "
                    f"updated at {tag.last_updated_on}"
                )
                delete_tasks.append(
                    asyncio.create_task(
                        acr_client.delete_tag(
                            _MEADOWRUN_GENERATED_DOCKER_REPO, tag.name
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
                    table_client.delete_entity(CONTAINER_IMAGE, tag_name)
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
