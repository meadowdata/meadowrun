"""
This code will run in the generic AWS Lambda environment. It has access to code in
meadowrun, meadowrun.aws_integration, and meadowrun.aws_integration.management_lambdas.
However, no 3rd party dependencies are added.
"""
from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
)

import boto3
from meadowrun.aws_integration.ec2_pricing import get_ec2_instance_types
from meadowrun.aws_integration.management_lambdas.config import (
    INSTANCE_THRESHOLDS,
    TERMINATE_INSTANCES_IF_IDLE_FOR,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    MACHINE_AGENT_QUEUE_PREFIX,
    _EC2_ALLOC_TABLE_NAME,
    _INSTANCE_ID,
    _LAST_UPDATE_TIME,
    _MEADOWRUN_TAG,
    _MEADOWRUN_TAG_VALUE,
    _RUNNING_JOBS,
    _get_account_number,
    ignore_boto3_error_code,
)
from meadowrun.aws_integration.management_lambdas.provisioning import (
    Threshold,
    shutdown_thresholds,
)
from meadowrun.instance_selection import ResourcesInternal

if TYPE_CHECKING:
    from meadowrun.instance_selection import CloudInstanceType, OnDemandOrSpotType
    from mypy_boto3_ec2.service_resource import EC2ServiceResource, Instance


# If we see instances running that aren't registered, we assume there is something wrong
# and they need to be terminated. However, it's possible that we happen to query between
# when an instance is launched and when it's registered. So for some time after an
# instance is launched, we don't terminate it even if it's not registered.
_LAUNCH_REGISTER_DELAY = dt.timedelta(minutes=5)


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
) -> Dict[str, Tuple[dt.datetime, int]]:
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
            dt.datetime.fromisoformat(item[_LAST_UPDATE_TIME]).replace(
                tzinfo=dt.timezone.utc
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


async def adjust(region_name: str) -> None:
    await _deregister_and_terminate_instances(
        region_name,
        TERMINATE_INSTANCES_IF_IDLE_FOR,
        INSTANCE_THRESHOLDS,
        _LAUNCH_REGISTER_DELAY,
    )
    # TODO this should also launch instances based on pre-provisioning policy


def _get_non_terminated_instances(
    ec2_resource: EC2ServiceResource,
) -> Iterable[Instance]:
    """Gets all meadowrun-launched EC2 instances that aren't terminated"""
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_MEADOWRUN_TAG}", "Values": [_MEADOWRUN_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": _NON_TERMINATED_EC2_STATES},
        ]
    )


def get_running_instances(ec2_resource: EC2ServiceResource) -> Iterable[Instance]:
    return ec2_resource.instances.filter(
        Filters=[
            {"Name": f"tag:{_MEADOWRUN_TAG}", "Values": [_MEADOWRUN_TAG_VALUE]},
            {"Name": "instance-state-name", "Values": ["running"]},
        ]
    )


def _delete_abandoned_machine_queues(sqs_client: Any, instance_ids: Set[str]) -> None:
    queue_urls_to_delete = []

    for page in sqs_client.get_paginator("list_queues").paginate(
        QueueNamePrefix=MACHINE_AGENT_QUEUE_PREFIX
    ):
        for machine_queue_url in page.get("QueueUrls", []):
            queue_instance_id = machine_queue_url.split("/")[-1][
                len(MACHINE_AGENT_QUEUE_PREFIX) :
            ]
            if queue_instance_id not in instance_ids:
                queue_urls_to_delete.append(machine_queue_url)

    for queue_url in queue_urls_to_delete:
        # we will pick up queues that are already being deleted as they can take up to
        # 60 seconds to delete
        print(
            "Deleting machine queue corresponding to machine that is no longer running "
            f"{queue_url}"
        )
        ignore_boto3_error_code(
            lambda: sqs_client.delete_queue(QueueUrl=queue_url),
            "AWS.SimpleQueueService.NonExistentQueue",
        )


def _delete_machine_queue_if_exists(
    sqs_client: Any, instance_id: str, region_name: str
) -> None:
    ignore_boto3_error_code(
        lambda: sqs_client.delete_queue(
            QueueUrl=(
                f"https://sqs.{region_name}.amazonaws.com/{_get_account_number()}/"
                f"{MACHINE_AGENT_QUEUE_PREFIX}{instance_id}"
            )
        ),
        "AWS.SimpleQueueService.NonExistentQueue",
    )


@dataclasses.dataclass
class _EC2_Instance:
    instance_id: str
    instance: Optional[Instance]
    registration: Optional[Tuple[dt.datetime, int]]
    action: Optional[Callable[[_EC2_Instance], None]] = None
    reason: Optional[str] = None

    @property
    def state(self) -> str:
        assert self.instance is not None
        return self.instance.state["Name"]

    def add_action(self, action: Callable[[_EC2_Instance], None], reason: str) -> None:
        assert (
            self.action is None and self.reason is None
        ), "Instance already has an action"

        self.action = action
        self.reason = reason

    def do_action(self) -> None:
        assert self.action is not None and self.reason is not None
        print(self.reason)
        self.action(self)


def _get_ec2_instances(region_name: str) -> Dict[str, _EC2_Instance]:
    ec2_resource = boto3.resource("ec2", region_name=region_name)
    non_terminated_instances = {
        instance.instance_id: _EC2_Instance(instance.instance_id, instance, None)
        for instance in _get_non_terminated_instances(ec2_resource)
    }
    registered_instances = _get_registered_ec2_instances(region_name)

    for instance_id, registration in registered_instances.items():
        ec2_instance = non_terminated_instances.get(instance_id, None)
        if ec2_instance is None:
            non_terminated_instances[instance_id] = _EC2_Instance(
                instance_id, None, registration
            )
        else:
            ec2_instance.registration = registration
    return non_terminated_instances


async def _get_instance_type_info(
    region_name: str,
) -> Dict[Tuple[str, OnDemandOrSpotType], CloudInstanceType]:
    # gather some info to apply thresholds.
    cloud_instance_types: List[CloudInstanceType] = await get_ec2_instance_types(
        region_name
    )
    return {
        (instance_type.name, instance_type.on_demand_or_spot): instance_type
        for instance_type in cloud_instance_types
    }


async def _deregister_and_terminate_instances(
    region_name: str,
    terminate_instances_if_idle_for: dt.timedelta,
    thresholds: List[Threshold],
    launch_register_delay: dt.timedelta = _LAUNCH_REGISTER_DELAY,
) -> None:
    """
    1. Compares running vs registered instances and terminates/deregisters instances
    to get running/registered instances back in sync
    2. Checks thresholds for instances to keep
    3. Terminates and deregisters idle instances
    """

    ec2_instances = _get_ec2_instances(region_name)
    instance_type_info = await _get_instance_type_info(region_name)
    now = dt.datetime.now(dt.timezone.utc)

    _add_deregister_and_terminate_actions(
        region_name,
        terminate_instances_if_idle_for,
        thresholds,
        launch_register_delay,
        now,
        ec2_instances,
        instance_type_info,
    )

    for instance_id, instance in ec2_instances.items():
        if instance.action is None:
            print(f"Letting {instance_id} continue to run--this instance is active")
            continue

        instance.do_action()


def _add_deregister_and_terminate_actions(
    region_name: str,
    terminate_instances_if_idle_for: dt.timedelta,
    thresholds: List[Threshold],
    launch_register_delay: dt.timedelta,
    now: dt.datetime,
    ec2_instances: Dict[str, _EC2_Instance],
    instance_type_info: Dict[Tuple[str, OnDemandOrSpotType], CloudInstanceType],
) -> None:
    """Modifies ec2_instances to add terminate or deregister actions where necessary.
    This is separated from the actual execution of the actions for testability."""

    _sync_registration_and_actual_state(
        region_name, ec2_instances, now, launch_register_delay
    )

    # build instance id to an identifier we can look up in instance_type_info,
    # to look up instance type specific resources, and price.
    instance_id_to_type = {
        instance_id: (
            instance.instance.instance_type,
            instance.instance.instance_lifecycle or "on_demand",
        )
        for instance_id, instance in ec2_instances.items()
        # for shutdown thresholds, we only consider instances that were
        # not processed by _sync_registration_and_actual_state.
        if instance.action is None and instance.instance is not None
    }

    # build instance id to instance-specific resources
    instance_id_to_resources = {
        instance_id: ResourcesInternal.from_ec2_instance_resource(instance.instance)
        for instance_id, instance in ec2_instances.items()
        if instance.action is None and instance.instance is not None
    }

    # Check thresholds. Returns any instances that may be shut down.
    excess_instances = shutdown_thresholds(
        thresholds,
        instance_id_to_type,  # type: ignore[arg-type]
        instance_type_info,
        instance_id_to_resources,
    )

    _delete_abandoned_machine_queues(sqs_client, set(running_instances.keys()))

    # Apply idle timeout on the rest, if registered.
    def deregister_and_terminate(instance: _EC2_Instance) -> None:
        if instance.instance is None:
            return
        success = _deregister_ec2_instance(instance.instance_id, True, region_name)
        if success:
            instance.instance.terminate()

    for instance_id in excess_instances:
        instance = ec2_instances.get(instance_id)
        if instance is None or instance.registration is None:
            # this should not happen given all of the above,
            # but just in case.
            continue
        (last_updated, num_jobs) = instance.registration
        if num_jobs == 0 and (now - last_updated) > terminate_instances_if_idle_for:
            instance.add_action(
                action=deregister_and_terminate,
                reason=f"{instance_id} is not running any jobs and has not run anything"
                f" since {last_updated} so we will deregister and terminate it",
            )


def _sync_registration_and_actual_state(
    region_name: str,
    ec2_instances: Dict[str, _EC2_Instance],
    now: dt.datetime,
    launch_register_delay: dt.timedelta,
) -> None:
    """Adds actions to either deregister or terminate where instances appear in the
    alloc table but are not present according to AWS, or vice versa."""

    def deregister_and_terminate(instance: _EC2_Instance) -> None:
        _deregister_ec2_instance(instance.instance_id, False, region_name)
        if instance.instance is not None:
            # just in case
            instance.instance.terminate()

    def terminate(instance: _EC2_Instance) -> None:
        if instance.instance is None:
            return
        _delete_machine_queue_if_exists(sqs_client, instance_id, region_name)
        instance.instance.terminate()

    for instance_id, instance in ec2_instances.items():
        if instance.registration is None:
            assert instance.instance is not None
            if (now - instance.instance.launch_time) > launch_register_delay:
                instance.add_action(
                    action=terminate,
                    reason=f"{instance_id} is running but is not registered, will "
                    f"terminate. Was launched at {instance.instance.launch_time}.",
                )
            else:
                instance.add_action(
                    action=lambda _: None,
                    reason=f"{instance_id} is running but is not registered, not "
                    "terminating because it was launched recently at "
                    f"{instance.instance.launch_time}",
                )
        elif instance.instance is None or instance.state != "running":
            instance.add_action(
                action=deregister_and_terminate,
                reason=f"{instance_id} is registered but does not seem to be "
                "running, so we will deregister it",
            )


def lambda_handler(event: Any, context: Any) -> Dict[str, Any]:
    """The handler for AWS lambda"""
    asyncio.run(adjust(os.environ["AWS_REGION"]))
    return {"statusCode": 200, "body": ""}
