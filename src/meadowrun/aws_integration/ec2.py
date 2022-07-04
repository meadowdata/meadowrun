from __future__ import annotations

import asyncio
import ipaddress
import threading
from typing import Sequence, Optional, Dict, Any, TypeVar, Awaitable

import boto3

from meadowrun.aws_integration.aws_core import (
    MeadowrunNotInstalledError,
    _get_current_ip_for_ssh,
    _get_default_region_name,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)
from meadowrun.aws_integration.quotas import SpotQuotaException
from meadowrun.instance_selection import (
    CloudInstance,
    OnDemandOrSpotType,
    Resources,
    choose_instance_types_for_job,
)


_T = TypeVar("_T")


# A security group that allows SSH, clients' IP addresses get added as needed
_MEADOWRUN_SSH_SECURITY_GROUP = "meadowrun_ssh_security_group"


def get_ssh_security_group_id(region_name: str) -> str:
    """
    Gets the id of the meadowrun SSH security group, which determines which IPs are
    allowed to SSH into the Meadowrun-managed instances.
    """
    ec2_resource = boto3.resource("ec2", region_name=region_name)
    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWRUN_SSH_SECURITY_GROUP
    )
    if security_group is None:
        raise MeadowrunNotInstalledError(
            f"security group {_MEADOWRUN_SSH_SECURITY_GROUP}"
        )
    return security_group.id


async def authorize_current_ip_helper(region_name: str) -> None:
    """
    Tries to add the current IP address to the list of IPs allowed to SSH into the
    meadowrun SSH security group (will warn rather than raise on error). Returns the
    security group id of the meadowrun SSH security group.
    """
    current_ip_for_ssh = await _get_current_ip_for_ssh()

    ec2_resource = boto3.resource("ec2", region_name=region_name)
    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWRUN_SSH_SECURITY_GROUP
    )
    if security_group is None:
        raise MeadowrunNotInstalledError(
            f"security group {_MEADOWRUN_SSH_SECURITY_GROUP}"
        )

    # check if our ip is already in the security group. This isn't watertight, as we
    # might interpret these rules differently from how AWS does, and there are other
    # options for granting access, e.g. via a security group. So we will just warn
    # rather than raising an exception
    already_authorized = False
    current_ip_for_ssh_address = ipaddress.ip_address(current_ip_for_ssh)
    for permission in security_group.ip_permissions:
        if permission.get("FromPort", 100000) <= 22 <= permission.get("ToPort", -1):
            for ip_range in permission.get("IpRanges", ()):
                cidr_ip = ip_range.get("CidrIp")
                if cidr_ip and current_ip_for_ssh_address in ipaddress.ip_network(
                    cidr_ip
                ):
                    already_authorized = True

    if not already_authorized:
        try:
            ignore_boto3_error_code(
                lambda: security_group.authorize_ingress(
                    IpProtocol="tcp",
                    CidrIp=f"{current_ip_for_ssh}/32",
                    FromPort=22,
                    ToPort=22,
                ),
                "InvalidPermission.Duplicate",
            )
            print(
                f"Authorized the current ip address {current_ip_for_ssh} to SSH into "
                f"the {_MEADOWRUN_SSH_SECURITY_GROUP} security group"
            )
        except Exception as e:
            print(
                "Warning, failed to authorize current IP address for SSH. Connecting to"
                " Meadowrun instances will fail unless your connection has been "
                "authorized in a different way. Most likely, meadowrun was installed "
                "with `meadowrun-manage-ec2 install --allow-authorize-ips False`. If "
                "this is the case, you can rerun with `--allow-authorize-ips True`, or "
                "have an administrator manually authorize your connection to the "
                f"{_MEADOWRUN_SSH_SECURITY_GROUP} security group by running something "
                f"like: `aws ec2 authorize-security-group-ingress --group-name "
                f"{_MEADOWRUN_SSH_SECURITY_GROUP} --protocol tcp --port 22 --cidr "
                f"{current_ip_for_ssh}/32` {e}"
            )


def ensure_security_group(group_name: str) -> str:
    """
    Creates the specified security group if it doesn't exist. If it does exist, does not
    modify it (as there may be existing rules that should not be deleted).

    Returns the id of the security group.
    """
    ec2_resource = boto3.resource("ec2")
    security_group = _get_ec2_security_group(ec2_resource, group_name)
    if security_group is None:
        security_group = ec2_resource.create_security_group(
            Description=group_name, GroupName=group_name
        )
    return security_group.id


async def launch_ec2_instance(
    region_name: str,
    instance_type: str,
    on_demand_or_spot: OnDemandOrSpotType,
    ami_id: str,
    security_group_ids: Optional[Sequence[str]] = None,
    iam_role_name: Optional[str] = None,
    user_data: Optional[str] = None,
    key_name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Optional[Awaitable[str]]:
    """
    Launches the specified EC2 instance. Returns None if the instance cannot be launched
    because of capacity issues. If the instance can be launched, returns a continuation
    that can be awaited to get the public dns name of the launched instance.
    """

    optional_args: Dict[str, Any] = {
        # TODO allow users to specify the size of the EBS they need
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": 16,
                    "VolumeType": "gp2",
                },
            }
        ]
    }
    if security_group_ids:
        optional_args["SecurityGroupIds"] = security_group_ids
    if iam_role_name:
        optional_args["IamInstanceProfile"] = {"Name": iam_role_name}
    if key_name:
        optional_args["KeyName"] = key_name
    if user_data:
        optional_args["UserData"] = user_data
    if tags:
        optional_args["TagSpecifications"] = [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": key, "Value": value} for key, value in tags.items()],
            }
        ]

    if on_demand_or_spot == "on_demand":
        pass
    elif on_demand_or_spot == "spot":
        optional_args["InstanceMarketOptions"] = {"MarketType": "spot"}
    else:
        raise ValueError(f"Unexpected value for on_demand_or_spot {on_demand_or_spot}")

    ec2_resource = boto3.resource("ec2", region_name=region_name)
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
    success, instances, error_code = ignore_boto3_error_code(
        lambda: ec2_resource.create_instances(
            ImageId=ami_id,
            MinCount=1,
            MaxCount=1,
            InstanceType=instance_type,
            **optional_args,
        ),
        {"InsufficientInstanceCapacity", "MaxSpotInstanceCountExceeded"},
        True,
    )
    if not success:
        if error_code == "InsufficientInstanceCapacity":
            # returning None means there's not enough capacity
            return None
        elif error_code == "MaxSpotInstanceCountExceeded":
            raise SpotQuotaException(instance_type, region_name)
        else:
            raise ValueError(f"Unexpected boto3 error code {error_code}")

    assert instances is not None  # just for mypy
    return _launch_instance_continuation(instances[0])


async def _launch_instance_continuation(instance: Any) -> str:
    """
    instance should be a boto3 EC2 instance, waits for the instance to be running and
    then returns its public DNS name
    """

    # boto3 doesn't have any async APIs, which means that in order to run more than one
    # launch_ec2_instance at the same time, we need to have a thread that waits. We use
    # an asyncio.Future here to make the API async, so from the user perspective, it
    # feels like this function is async

    # boto3 should be threadsafe:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
    instance_running_future: asyncio.Future = asyncio.Future()
    event_loop = asyncio.get_running_loop()

    def wait_until_running() -> None:
        try:
            instance.wait_until_running()
            event_loop.call_soon_threadsafe(
                lambda: instance_running_future.set_result(None)
            )
        except Exception as e:
            exception = e
            event_loop.call_soon_threadsafe(
                lambda: instance_running_future.set_exception(exception)
            )

    threading.Thread(target=wait_until_running).start()
    await instance_running_future

    instance.load()
    if not instance.public_dns_name:
        raise ValueError("Waited until running, but still no IP address!")
    return instance.public_dns_name


async def launch_ec2_instances(
    logical_cpu_required_per_job: int,
    memory_gb_required_per_job: float,
    num_jobs: int,
    interruption_probability_threshold: float,
    ami_id: str,
    region_name: Optional[str] = None,
    security_group_ids: Optional[Sequence[str]] = None,
    iam_role_name: Optional[str] = None,
    user_data: Optional[str] = None,
    key_name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
) -> Sequence[CloudInstance]:
    """
    Launches enough EC2 instances to run num_jobs jobs that each require the specified
    amount of CPU/memory. Returns a sequence of CloudInstance.
    CloudInstance.instance_type.workers_per_instance_full indicates the maximum number
    of jobs that can run on that instance. The sum of all of the
    workers_per_instance_full will be greater than or equal to the original num_jobs
    parameter.
    """

    if region_name is None:
        region_name = await _get_default_region_name()

    instance_types = await _get_ec2_instance_types(region_name)

    num_jobs_left_to_allocate = num_jobs
    public_dns_name_tasks = []
    launched_instance_types_repeated = []
    unusable_instance_types = set()

    while num_jobs_left_to_allocate > 0:
        chosen_instance_types = choose_instance_types_for_job(
            Resources(memory_gb_required_per_job, logical_cpu_required_per_job, {}),
            num_jobs_left_to_allocate,
            interruption_probability_threshold,
            [
                i
                for i in instance_types
                if (i.name, i.on_demand_or_spot) not in unusable_instance_types
            ],
        )
        if len(chosen_instance_types) < 1:
            raise ValueError(
                "There were no instance types that could be selected for "
                f"memory={memory_gb_required_per_job}, "
                f"cpu={logical_cpu_required_per_job}"
            )

        num_jobs_left_to_allocate = 0
        for instance_type in chosen_instance_types:
            for _ in range(instance_type.num_instances):
                launch_ec2_continuation = await launch_ec2_instance(
                    region_name,
                    instance_type.instance_type.name,
                    instance_type.instance_type.on_demand_or_spot,
                    ami_id=ami_id,
                    security_group_ids=security_group_ids,
                    iam_role_name=iam_role_name,
                    user_data=user_data,
                    key_name=key_name,
                    tags=tags,
                )
                if launch_ec2_continuation is None:
                    print(
                        "Warning, no capacity to allocate "
                        f"{instance_type.instance_type.name}/"
                        f"{instance_type.instance_type.on_demand_or_spot}"
                    )
                    # TODO this isn't exactly right, we can end up over-allocating
                    # because we might choose a larger instance than we "actually"
                    # needed, but this shouldn't be a huge factor
                    num_jobs_left_to_allocate += instance_type.workers_per_instance_full
                    unusable_instance_types.add(
                        (
                            instance_type.instance_type.name,
                            instance_type.instance_type.on_demand_or_spot,
                        )
                    )
                else:
                    public_dns_name_tasks.append(launch_ec2_continuation)
                    launched_instance_types_repeated.append(instance_type)

    public_dns_names = await asyncio.gather(*public_dns_name_tasks)

    return [
        CloudInstance(public_dns_name, "", instance_type)
        for public_dns_name, instance_type in zip(
            public_dns_names, launched_instance_types_repeated
        )
    ]


def _get_ec2_security_group(ec2_resource: Any, name: str) -> Any:
    """
    Gets the specified security group if it exists. Returns an
    Optional[boto3.resources.factory.ec2.SecurityGroup] (not in the type signature
    because boto3 uses dynamic types).
    """
    success, groups = ignore_boto3_error_code(
        lambda: list(ec2_resource.security_groups.filter(GroupNames=[name])),
        "InvalidGroup.NotFound",
    )
    if not success:
        return None

    assert groups is not None  # just for mypy
    if len(groups) == 0:
        return None
    elif len(groups) > 1:
        raise ValueError(
            "Found multiple security groups with the same name which was unexpected"
        )
    else:
        return groups[0]
