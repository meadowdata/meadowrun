import datetime
import os
from typing import Any, Dict

import boto3

# queues will automatically be deleted 3 days after being created
_QUEUE_DELETION_TIMEOUT = datetime.timedelta(days=3)


def delete_old_task_queues() -> None:
    # TODO this would be better if the job clients could somehow keep their queues alive
    # while they're still being used rather than always having a very long timeout. We
    # could maybe abuse a random attribute which would probably cause the
    # LastModifiedTimestamp to update.
    now = datetime.datetime.utcnow()
    client = boto3.client("sqs", region_name=os.environ["AWS_REGION"])
    for page in client.get_paginator("list_queues").paginate(
        QueueNamePrefix="meadowrunTask"
    ):
        if "QueueUrls" in page:
            for queue_url in page["QueueUrls"]:
                response = client.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "CreatedTimestamp",
                        "LastModifiedTimestamp",
                    ],
                )
                attributes = response["Attributes"]
                # we should only need to check LastModifiedTimestamp, but we include
                # CreatedTimestamp just in case
                last_modified = max(
                    datetime.datetime.fromtimestamp(
                        int(attributes["CreatedTimestamp"])
                    ),
                    datetime.datetime.fromtimestamp(
                        int(attributes["LastModifiedTimestamp"])
                    ),
                )
                if (
                    attributes["ApproximateNumberOfMessages"] == "0"
                    and attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
                    and now - last_modified > _QUEUE_DELETION_TIMEOUT
                ):
                    print(
                        f"Deleting queue {queue_url}, was last modified at "
                        f"{last_modified}"
                    )
                    client.delete_queue(QueueUrl=queue_url)


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    delete_old_task_queues()
    return {"statusCode": 200, "body": ""}
