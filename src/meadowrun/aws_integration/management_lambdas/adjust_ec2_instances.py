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
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
    _LAST_UPDATE_TIME,
    _PUBLIC_ADDRESS,
    _RUNNING_JOBS,
    ignore_boto3_error_code,
)


# Terminate instances if they haven't run any jobs in the last 5 minutes
_TERMINATE_INSTANCES_IF_IDLE_FOR = datetime.timedelta(minutes=5)
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
    resources, returns {public_address: (last time a job was allocated or deallocated on
    this instance, number of currently running jobs)}
    """

    response = _get_ec2_alloc_table(region_name).scan(
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
    public_address: str, require_no_running_jobs: bool, region_name: str
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


def adjust(region_name: str) -> None:
    _deregister_and_terminate_instances(
        region_name, _TERMINATE_INSTANCES_IF_IDLE_FOR, _LAUNCH_REGISTER_DELAY
    )
    # TODO this should also launch instances based on pre-provisioning policy


def _get_non_terminated_instances(ec2_resource: Any) -> Iterable[Any]:
    """Gets all meadowrun-launched EC2 instances that aren't terminated"""
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_EC2_ALLOC_TAG}", "Values": [_EC2_ALLOC_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": _NON_TERMINATED_EC2_STATES},
        ]
    )


def _get_running_instances(ec2_resource: Any) -> Iterable[Any]:
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_EC2_ALLOC_TAG}", "Values": [_EC2_ALLOC_TAG_VALUE]},
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
        instance.public_dns_name: instance
        for instance in _get_non_terminated_instances(ec2_resource)
    }
    running_instances = {
        instance.public_dns_name: instance
        for instance in _get_running_instances(ec2_resource)
    }

    registered_instances = _get_registered_ec2_instances(region_name)

    now = datetime.datetime.utcnow()
    now_with_timezone = datetime.datetime.now(datetime.timezone.utc)

    for public_address, (last_updated, num_jobs) in registered_instances.items():
        if public_address not in running_instances:
            print(
                f"{public_address} is registered but does not seem to be running, so we"
                f" will deregister it"
            )
            _deregister_ec2_instance(public_address, False, region_name)
            if public_address in non_terminated_instances:
                # just in case
                non_terminated_instances[public_address].terminate()
        elif num_jobs == 0 and (now - last_updated) > terminate_instances_if_idle_for:
            success = _deregister_ec2_instance(public_address, True, region_name)
            if success:
                print(
                    f"{public_address} is not running any jobs and has not run anything"
                    f" since {last_updated} so we will deregister and terminate it"
                )
                non_terminated_instances[public_address].terminate()
        else:
            print(f"Letting {public_address} continue to run--this instance is active")

    for public_address, instance in non_terminated_instances.items():
        if (
            public_address not in registered_instances
            and (now_with_timezone - instance.launch_time) > launch_register_delay
        ):
            print(
                f"{public_address} is running but is not registered, will terminate. "
                f"Was launched at {instance.launch_time}."
            )
            instance.terminate()

    # TOOD terminate stopped instances


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    adjust(os.environ["AWS_REGION"])
    return {"statusCode": 200, "body": ""}
