import datetime
import os
from typing import Any, Dict, Iterable

import boto3

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    ignore_boto3_error_code,
)

# queues will automatically be deleted 3 days after being created
_QUEUE_DELETION_TIMEOUT = datetime.timedelta(days=3)

# ECR images will be automatically deleted 2 days after last being created/used
_ECR_DELETION_TIMEOUT = datetime.timedelta(days=2)


def _get_meadowrun_task_queue_urls(sqs_client: Any) -> Iterable[str]:
    """Gets all SQS queue URLs created for meadowrun tasks"""
    for page in sqs_client.get_paginator("list_queues").paginate(
        QueueNamePrefix="meadowrun-task"
    ):
        if "QueueUrls" in page:
            for queue_url in page["QueueUrls"]:
                yield queue_url


def delete_old_task_queues(region_name: str) -> None:
    # TODO this would be better if the job clients could somehow keep their queues alive
    # while they're still being used rather than always having a very long timeout. We
    # could maybe abuse a random attribute which would probably cause the
    # LastModifiedTimestamp to update.
    now = datetime.datetime.utcnow()
    client = boto3.client("sqs", region_name=region_name)
    for queue_url in _get_meadowrun_task_queue_urls(client):
        success, response = ignore_boto3_error_code(
            lambda: client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "CreatedTimestamp",
                    "LastModifiedTimestamp",
                ],
            ),
            "AWS.SimpleQueueService.NonExistentQueue",
        )
        if not success:
            continue
        assert response is not None  # just for mypy
        attributes = response["Attributes"]
        # we should only need to check LastModifiedTimestamp, but we include
        # CreatedTimestamp just in case
        last_modified = max(
            datetime.datetime.fromtimestamp(int(attributes["CreatedTimestamp"])),
            datetime.datetime.fromtimestamp(int(attributes["LastModifiedTimestamp"])),
        )
        if (
            attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
            and now - last_modified > _QUEUE_DELETION_TIMEOUT
        ):
            print(f"Deleting queue {queue_url}, was last modified at {last_modified}")
            client.delete_queue(QueueUrl=queue_url)


def delete_all_task_queues(region_name: str) -> None:
    """WARNING this will cause running run_map jobs to fail"""
    client = boto3.client("sqs", region_name=region_name)
    for queue_url in _get_meadowrun_task_queue_urls(client):
        ignore_boto3_error_code(
            lambda: client.delete_queue(QueueUrl=queue_url),
            "AWS.SimpleQueueService.NonExistentQueue",
        )


def delete_unused_images(region_name: str) -> None:
    """
    Deletes images in the _MEADOWRUN_GENERATED_DOCKER_REPO that haven't been
    pushed/pulled for _ECR_DELETION_TIMEOUT. The last pull time from AWS can be up to 24
    hours off (see describe_images documentation), so _ECR_DELETION_TIMEOUT should
    usually be about 24 hours longer than you actually want it to be.
    """
    images_to_delete = []

    client = boto3.client("ecr", region_name=region_name)

    success, result = ignore_boto3_error_code(
        lambda: client.describe_repositories(
            repositoryNames=[_MEADOWRUN_GENERATED_DOCKER_REPO]
        ),
        "RepositoryNotFoundException",
    )
    if not success:
        print(
            f"Repository {_MEADOWRUN_GENERATED_DOCKER_REPO} has not been created yet, "
            "so no images to clean up."
        )
        return

    cutoff_datetime = (
        datetime.datetime.now(datetime.timezone.utc) - _ECR_DELETION_TIMEOUT
    )
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html#ECR.Client.describe_images
    for page in client.get_paginator("describe_images").paginate(
        repositoryName=_MEADOWRUN_GENERATED_DOCKER_REPO
    ):
        for image in page["imageDetails"]:
            if "lastRecordedPullTime" in image:
                last_touched = image["lastRecordedPullTime"]
            else:
                last_touched = image["imagePushedAt"]

            if last_touched < cutoff_datetime:
                if "imageTags" not in image or len(image["imageTags"]) == 0:
                    print(
                        "Cannot delete image with no tags, you must delete it manually."
                        f" Digest is {image['imageDigest']}"
                    )
                else:
                    print(
                        "Will delete image with tag(s) "
                        f"{';'.join(image['imageTags'])} and digest "
                        f"{image['imageDigest']} which was last pulled/pushed at "
                        f"{last_touched}"
                    )
                    images_to_delete.append(image["imageTags"][0])

    if images_to_delete:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html#ECR.Client.batch_delete_image
        response = client.batch_delete_image(
            repositoryName=_MEADOWRUN_GENERATED_DOCKER_REPO,
            imageIds=[{"imageTag": tag} for tag in images_to_delete],
        )
        if response.get("failures"):
            raise ValueError(f"Failures: {response.get('failures')}")


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    region_name = os.environ["AWS_REGION"]

    exceptions = []
    try:
        delete_old_task_queues(region_name)
    except Exception as e1:
        exceptions.append(e1)

    try:
        delete_unused_images(region_name)
    except Exception as e2:
        exceptions.append(e2)

    if len(exceptions) >= 1:
        if len(exceptions) > 1:
            # if only python had a way to combine exceptions...
            print(f"Ignoring second exception: {exceptions[1]})")
        raise exceptions[0]

    return {"statusCode": 200, "body": ""}
