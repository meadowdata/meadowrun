from __future__ import annotations

import asyncio
import threading
from typing import Sequence, Tuple, Callable, Optional, Dict, Any, TypeVar

import boto3
import botocore.exceptions

from meadowrun.aws_integration.aws_core import (
    _get_current_ip_for_ssh,
    _get_default_region_name,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)
from meadowrun.instance_selection import (
    CloudInstance,
    OnDemandOrSpotType,
    Resources,
    choose_instance_types_for_job,
)


_T = TypeVar("_T")


# A security group that allows SSH, clients' IP addresses get added as needed
_MEADOWRUN_SSH_SECURITY_GROUP = "meadowrunSshSecurityGroup"


async def ensure_meadowrun_ssh_security_group() -> str:
    """
    Creates the _MEADOWRUN_SSH_SECURITY_GROUP if it doesn't exist. If it does exist,
    make sure the current IP address is allowed to SSH into instances in that security
    group.

    Returns the security group id of the _MEADOWRUN_SSH_SECURITY_GROUP
    """
    current_ip_for_ssh = await _get_current_ip_for_ssh()
    return ensure_security_group(
        _MEADOWRUN_SSH_SECURITY_GROUP, [(22, 22, f"{current_ip_for_ssh}/32")], []
    )


def ensure_security_group(
    group_name: str,
    open_port_cidr_block: Sequence[Tuple[int, int, str]],
    open_port_group: Sequence[Tuple[int, int, str]],
) -> str:
    """
    Creates the specified security group if it doesn't exist. If it does exist, adds the
    specified ingress rules. E.g. open_port_cidr_block=[(8000, 8010, "8.8.8.8/32")]
    allows incoming traffic on ports 8000 to 8010 (inclusive) from the 8.8.8.8 ip
    address. open_port_group works similarly, but instead of an IP address you can
    specify the name of another security group.

    Returns the id of the security group.
    """
    ec2_resource = boto3.resource("ec2")
    security_group = _get_ec2_security_group(ec2_resource, group_name)
    if security_group is None:
        security_group = ec2_resource.create_security_group(
            Description=group_name, GroupName=group_name
        )

    for from_port, to_port, cidr_ip in open_port_cidr_block:
        ignore_boto3_error_code(
            lambda: security_group.authorize_ingress(
                IpProtocol="tcp",
                CidrIp=cidr_ip,
                FromPort=from_port,
                ToPort=to_port,
            ),
            "InvalidPermission.Duplicate",
        )

    for from_port, to_port, group_id in open_port_group:
        ignore_boto3_error_code(
            lambda: security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "FromPort": from_port,
                        "ToPort": to_port,
                        "IpProtocol": "tcp",
                        "UserIdGroupPairs": [{"GroupId": group_id}],
                    }
                ]
            ),
            "InvalidPermission.Duplicate",
        )

    return security_group.id


async def _retry_iam_instance_profile(func: Callable[[], _T]) -> _T:
    """
    There is an issue where when you create a new IAM instance profile then launch an
    EC2 instance with that profile, launching the EC2 instance will fail for the first
    few seconds. This function will retry func for "Invalid IAM Instance Profile" errors
    """
    attempts = 7
    seconds_to_wait = 2

    for i in range(attempts):
        try:
            return func()
        except botocore.exceptions.ClientError as e:
            if "Error" in e.response:
                error = e.response["Error"]
                if (
                    "Code" in error
                    and error["Code"] == "InvalidParameterValue"
                    and "Invalid IAM Instance Profile" in error["Message"]
                ):
                    if i < attempts - 1:
                        print(
                            "Retrying because IAM instance profile is not available yet"
                        )
                        await asyncio.sleep(seconds_to_wait)
                        continue

            raise

    raise ValueError("This should never happen")


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
    wait_for_dns_name: bool = True,
) -> Optional[str]:
    """
    Launches the specified EC2 instance. If wait_for_dns_name is True, waits for the
    instance to get a public dns name assigned, and then returns that. Otherwise returns
    None.

    One wrinkle is that if you specify tags for a spot instance, we have to wait for it
    to launch, as there's no way to tag a spot instance before it's running.
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
    instance = (
        await _retry_iam_instance_profile(
            lambda: ec2_resource.create_instances(
                ImageId=ami_id,
                MinCount=1,
                MaxCount=1,
                InstanceType=instance_type,
                **optional_args,
            )
        )
    )[0]

    if wait_for_dns_name:
        # boto3 doesn't have any async APIs, which means that in order to run more than
        # one launch_ec2_instance at the same time, we need to have a thread that waits.
        # We use an asyncio.Future here to make the API async, so from the user
        # perspective, it feels like this function is async

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
    else:
        return None


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

    chosen_instance_types = choose_instance_types_for_job(
        Resources(memory_gb_required_per_job, logical_cpu_required_per_job, {}),
        num_jobs,
        interruption_probability_threshold,
        await _get_ec2_instance_types(region_name),
    )
    if len(chosen_instance_types) < 1:
        raise ValueError(
            f"There were no instance types that could be selected for "
            f"memory={memory_gb_required_per_job}, cpu={logical_cpu_required_per_job}"
        )

    public_dns_name_tasks = []
    chosen_instance_types_repeated = []

    for instance_type in chosen_instance_types:
        for _ in range(instance_type.num_instances):
            # should really launch these with a single API call
            public_dns_name_tasks.append(
                launch_ec2_instance(
                    region_name,
                    instance_type.instance_type.name,
                    instance_type.instance_type.on_demand_or_spot,
                    ami_id=ami_id,
                    security_group_ids=security_group_ids,
                    iam_role_name=iam_role_name,
                    user_data=user_data,
                    key_name=key_name,
                    tags=tags,
                    wait_for_dns_name=True,
                )
            )
            chosen_instance_types_repeated.append(instance_type)

    public_dns_names = await asyncio.gather(*public_dns_name_tasks)

    return [
        CloudInstance(public_dns_name, "", instance_type)
        for public_dns_name, instance_type in zip(
            public_dns_names, chosen_instance_types_repeated
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
