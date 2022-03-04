"""
This code will run in the generic AWS Lambda environment, so it should not import any
code outside this folder.
"""
import datetime
import os
from typing import Dict, Tuple, Any

import boto3

from meadowgrid.ec2_alloc_lambda.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
    _LAST_UPDATE_TIME,
    _PUBLIC_ADDRESS,
    _RUNNING_JOBS,
    ignore_boto3_error_code,
)


# Terminate instances if they haven't run any jobs in the last 30 seconds
_TERMINATE_INSTANCES_IF_IDLE_FOR = datetime.timedelta(seconds=30)
# If we see instances running that aren't registered, we assume there is something wrong
# and they need to be terminated. However, it's possible that we happen to query between
# when an instance is launched and when it's registered. So for the first 30 seconds
# after an instance is launched, we don't terminate it even if it's not registered.
_LAUNCH_REGISTER_DELAY = datetime.timedelta(seconds=30)


def _get_ec2_alloc_table() -> Any:
    """
    Very similar to _ensure_ec2_alloc_table, but assumes the table already exists and
    assumes we're running in an AWS lambda
    """

    # the AWS_REGION environment variable should be populated for lambdas
    db = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"])
    return db.Table(_EC2_ALLOC_TABLE_NAME)


def _get_ec2_instances() -> Dict[str, Tuple[datetime.datetime, int]]:
    """
    Similar to _get_ec2_instances in ec2_alloc, but instead of getting the available
    resources, returns {public_address: (last time a job was allocated or deallocated on
    this instance, number of currently running jobs)}
    """

    response = _get_ec2_alloc_table().scan(
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression=",".join(
            [_PUBLIC_ADDRESS, _LAST_UPDATE_TIME, _RUNNING_JOBS]
        ),
    )

    # see comment on ec2_alloc._get_ec2_instances
    if response.get("LastEvaluatedKey"):
        raise NotImplementedError(
            "We don't currently support a very large number of EC2 instances"
        )

    return {
        item[_PUBLIC_ADDRESS]: (
            datetime.datetime.fromisoformat(item[_LAST_UPDATE_TIME]),
            len(item[_RUNNING_JOBS]),
        )
        for item in response["Items"]
    }


def _deregister_ec2_instance(
    public_address: str, require_no_running_jobs: bool
) -> bool:
    """
    Deregisters an EC2 instance. If require_no_running_jobs is true, then only
    deregisters if there are no currently running jobs on the instance. If
    require_no_running_jobs is false, we will deregister the instance even if there are
    jobs running on it.
    """

    optional_args: Dict[str, Any] = {}
    if require_no_running_jobs:
        optional_args["ConditionExpression"] = f"size({_RUNNING_JOBS}) = :zero"
        optional_args["ExpressionAttributeValues"] = {":zero": 0}

    success, result = ignore_boto3_error_code(
        lambda: _get_ec2_alloc_table().delete_item(
            Key={_PUBLIC_ADDRESS: public_address}, **optional_args
        ),
        "ConditionalCheckFailedException",
    )
    return success


_NON_TERMINATED_EC2_STATES = [
    "pending",
    "running",
    "shutting-down",
    "stopping",
    "stopped",
]


def adjust() -> None:
    """
    1. Compares running vs registered instances and terminates/deregisters instances
    to get running/registered instances back in sync
    2. Terminates and deregisters idle instances
    TODO 3. Launches instances based on pre-provisioning policy
    """
    ec2 = boto3.resource("ec2")

    # by "running" here we mean anything that's not terminated
    running_instances = ec2.instances.filter(
        Filters=[
            {"Name": f"tag:{_EC2_ALLOC_TAG}", "Values": [_EC2_ALLOC_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": _NON_TERMINATED_EC2_STATES},
        ]
    )
    running_instances_dict = {
        instance.public_dns_name: instance for instance in running_instances
    }

    registered_instances = _get_ec2_instances()

    now = datetime.datetime.utcnow()
    now_with_timezone = datetime.datetime.now(datetime.timezone.utc)

    for public_address, (last_updated, num_jobs) in registered_instances.items():
        if public_address not in running_instances_dict:
            print(
                f"{public_address} is registered but does not seem to be running, so we"
                f" will deregister it"
            )
            _deregister_ec2_instance(public_address, False)
        elif num_jobs == 0 and (now - last_updated) > _TERMINATE_INSTANCES_IF_IDLE_FOR:
            success = _deregister_ec2_instance(public_address, True)
            if success:
                print(
                    f"{public_address} is not running any jobs and has not run anything"
                    f" since {last_updated} so we will deregister and terminate it"
                )
                running_instances_dict[public_address].terminate()

    for public_address, instance in running_instances_dict.items():
        if (
            public_address not in registered_instances
            and (now_with_timezone - instance.launch_time) > _LAUNCH_REGISTER_DELAY
        ):
            print(f"{public_address} is running but is not registered, will terminate")
            instance.terminate()


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    adjust()
    return {"statusCode": 200, "body": ""}
