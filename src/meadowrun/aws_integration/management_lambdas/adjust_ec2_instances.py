"""
This code will run in the generic AWS Lambda environment, so it should not import any
code outside this folder.
"""
import datetime
import os
from typing import Dict, Tuple, Any, Iterable

import boto3

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _INSTANCE_ID,
    _LAST_UPDATE_TIME,
    _MEADOWRUN_TAG,
    _MEADOWRUN_TAG_VALUE,
    _RUNNING_JOBS,
    ignore_boto3_error_code,
)
from meadowrun.aws_integration.management_lambdas.config import (
    TERMINATE_INSTANCES_IF_IDLE_FOR_SECS,
)


# Terminate instances if they haven't run any jobs recently
_TERMINATE_INSTANCES_IF_IDLE_FOR = datetime.timedelta(
    seconds=TERMINATE_INSTANCES_IF_IDLE_FOR_SECS
)
# If we see instances running that aren't registered, we assume there is something wrong
# and they need to be terminated. However, it's possible that we happen to query between
# when an instance is launched and when it's registered. So for the first 30 seconds
# after an instance is launched, we don't terminate it even if it's not registered.
_LAUNCH_REGISTER_DELAY = datetime.timedelta(minutes=5)


def _get_ec2_alloc_table(region_name: str) -> Any:
    """
    Very similar to ensure_ec2_alloc_table, but assumes the table already exists and
    assumes we're running in an AWS lambda
    """

    # the AWS_REGION environment variable should be populated for lambdas
    db = boto3.resource("dynamodb", region_name=region_name)
    return db.Table(_EC2_ALLOC_TABLE_NAME)


def _get_registered_ec2_instances(
    region_name: str,
) -> Dict[str, Tuple[datetime.datetime, int]]:
    """
    Similar to _get_ec2_instances in ec2_alloc, but instead of getting the available
    resources, returns {instance_id): (last time a job was allocated or deallocated on
    this instance, number of currently running jobs)}
    """

    response = _get_ec2_alloc_table(region_name).scan(
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression=",".join([_INSTANCE_ID, _LAST_UPDATE_TIME, _RUNNING_JOBS]),
    )

    # see comment on ec2_alloc._get_ec2_instances
    if response.get("LastEvaluatedKey"):
        raise NotImplementedError(
            "We don't currently support a very large number of EC2 instances"
        )

    return {
        item[_INSTANCE_ID]: (
            datetime.datetime.fromisoformat(item[_LAST_UPDATE_TIME]).replace(
                tzinfo=datetime.timezone.utc
            ),
            len(item[_RUNNING_JOBS]),
        )
        for item in response["Items"]
    }


def _deregister_ec2_instance(
    instance_id: str, require_no_running_jobs: bool, region_name: str
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
        lambda: _get_ec2_alloc_table(region_name).delete_item(
            Key={_INSTANCE_ID: instance_id}, **optional_args
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


def adjust(region_name: str) -> None:
    _deregister_and_terminate_instances(
        region_name, _TERMINATE_INSTANCES_IF_IDLE_FOR, _LAUNCH_REGISTER_DELAY
    )
    # TODO this should also launch instances based on pre-provisioning policy


def _get_non_terminated_instances(ec2_resource: Any) -> Iterable[Any]:
    """Gets all meadowrun-launched EC2 instances that aren't terminated"""
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_MEADOWRUN_TAG}", "Values": [_MEADOWRUN_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": _NON_TERMINATED_EC2_STATES},
        ]
    )


def _get_running_instances(ec2_resource: Any) -> Iterable[Any]:
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_MEADOWRUN_TAG}", "Values": [_MEADOWRUN_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )


def _deregister_and_terminate_instances(
    region_name: str,
    terminate_instances_if_idle_for: datetime.timedelta,
    launch_register_delay: datetime.timedelta = _LAUNCH_REGISTER_DELAY,
) -> None:
    """
    1. Compares running vs registered instances and terminates/deregisters instances
    to get running/registered instances back in sync
    2. Terminates and deregisters idle instances
    """

    ec2_resource = boto3.resource("ec2", region_name=region_name)
    non_terminated_instances = {
        instance.instance_id: instance
        for instance in _get_non_terminated_instances(ec2_resource)
    }
    running_instances = {
        instance.instance_id: instance
        for instance in _get_running_instances(ec2_resource)
    }

    registered_instances = _get_registered_ec2_instances(region_name)

    now = datetime.datetime.now(datetime.timezone.utc)

    for instance_id, (last_updated, num_jobs) in registered_instances.items():
        if instance_id not in running_instances:
            print(
                f"{instance_id} is registered but does not seem to be running, so we "
                "will deregister it"
            )
            _deregister_ec2_instance(instance_id, False, region_name)
            if instance_id in non_terminated_instances:
                # just in case
                non_terminated_instances[instance_id].terminate()
        elif num_jobs == 0 and (now - last_updated) > terminate_instances_if_idle_for:
            success = _deregister_ec2_instance(instance_id, True, region_name)
            if success:
                print(
                    f"{instance_id} is not running any jobs and has not run anything "
                    f"since {last_updated} so we will deregister and terminate it"
                )
                non_terminated_instances[instance_id].terminate()
        else:
            print(f"Letting {instance_id} continue to run--this instance is active")

    for instance_id, instance in non_terminated_instances.items():
        if instance_id not in registered_instances:
            if (now - instance.launch_time) > launch_register_delay:
                print(
                    f"{instance_id} is running but is not registered, will terminate. "
                    f"Was launched at {instance.launch_time}."
                )
                instance.terminate()
            else:
                print(
                    f"{instance_id} is running but is not registered, not terminating "
                    f"because it was launched recently at {instance.launch_time}"
                )


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    adjust(os.environ["AWS_REGION"])
    return {"statusCode": 200, "body": ""}
