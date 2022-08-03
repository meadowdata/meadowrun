from __future__ import annotations

import abc
import asyncio
import dataclasses
import ipaddress
import threading
from typing import (
    Any,
    Awaitable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
)

import boto3

from meadowrun.aws_integration.aws_core import (
    MeadowrunAWSAccessError,
    MeadowrunNotInstalledError,
    _get_current_ip_for_ssh,
    _get_default_region_name,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
    ignore_boto3_error_code,
)
from meadowrun.aws_integration.quotas import (
    get_quota_for_instance_type,
    instance_family_from_type,
)
from meadowrun.instance_selection import (
    CloudInstance,
    CloudInstanceType,
    OnDemandOrSpotType,
    ResourcesInternal,
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


def _authorize_current_ip_for_security_group(
    security_group: Any, from_port: int, to_port: int, current_ip: str
) -> None:
    # check if our ip is already in the security group. This isn't watertight, as we
    # might interpret these rules differently from how AWS does, and there are other
    # options for granting access, e.g. via a security group. So we should usually catch
    # any exceptions in the caller
    already_authorized = False
    current_ip_for_ssh_address = ipaddress.ip_address(current_ip)
    for permission in security_group.ip_permissions:
        if permission["FromPort"] <= from_port and to_port <= permission["ToPort"]:
            for ip_range in permission.get("IpRanges", ()):
                cidr_ip = ip_range.get("CidrIp")
                if cidr_ip and current_ip_for_ssh_address in ipaddress.ip_network(
                    cidr_ip
                ):
                    already_authorized = True
                    break
        if already_authorized:
            break

    if not already_authorized:
        ignore_boto3_error_code(
            lambda: security_group.authorize_ingress(
                IpProtocol="tcp",
                CidrIp=f"{current_ip}/32",
                FromPort=from_port,
                ToPort=to_port,
            ),
            "InvalidPermission.Duplicate",
        )
        print(
            f"Authorized the current ip address {current_ip} for ports "
            f"{from_port}-{to_port} to the {security_group.group_name} security group"
        )


async def authorize_current_ip_for_meadowrun_ssh(region_name: str) -> None:
    """
    Tries to add the current IP address to the list of IPs allowed to SSH into the
    meadowrun SSH security group (will warn rather than raise on error). Returns the
    security group id of the meadowrun SSH security group.
    """

    ec2_resource = boto3.resource("ec2", region_name=region_name)
    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWRUN_SSH_SECURITY_GROUP
    )
    if security_group is None:
        raise MeadowrunNotInstalledError(
            f"security group {_MEADOWRUN_SSH_SECURITY_GROUP}"
        )

    current_ip = await _get_current_ip_for_ssh()
    try:
        _authorize_current_ip_for_security_group(security_group, 22, 22, current_ip)
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
            f"{current_ip}/32` {e}"
        )


def ensure_security_group(group_name: str, region_name: str) -> str:
    """
    Creates the specified security group if it doesn't exist. If it does exist, does not
    modify it (as there may be existing rules that should not be deleted).

    Returns the id of the security group.
    """
    ec2_resource = boto3.resource("ec2", region_name=region_name)
    security_group = _get_ec2_security_group(ec2_resource, group_name)
    if security_group is None:
        security_group = ec2_resource.create_security_group(
            Description=group_name,
            GroupName=group_name,
            TagSpecifications=[
                {
                    "ResourceType": "security-group",
                    "Tags": [{"Key": _EC2_ALLOC_TAG, "Value": _EC2_ALLOC_TAG_VALUE}],
                }
            ],
        )
    return security_group.id


async def ensure_port_security_group(ports: str, region_name: str) -> str:
    """
    Creates a security group for opening the specified ports to the current ip address
    as well as other machines in that security group. Returns a security group id.
    """
    group_name = f"meadowrun{ports}"

    from_port, sep, to_port = ports.partition("-")
    if sep == "-":
        from_port_int = int(from_port)
        to_port_int = int(to_port)
    else:
        from_port_int = int(ports)
        to_port_int = from_port_int

    ec2_resource = boto3.resource("ec2", region_name)
    security_group = _get_ec2_security_group(ec2_resource, group_name)
    if security_group is None:
        try:
            security_group = ec2_resource.create_security_group(
                Description=group_name,
                GroupName=group_name,
                TagSpecifications=[
                    {
                        "ResourceType": "security-group",
                        "Tags": [
                            {"Key": _EC2_ALLOC_TAG, "Value": _EC2_ALLOC_TAG_VALUE}
                        ],
                    }
                ],
            )
        except Exception as e:
            raise MeadowrunAWSAccessError(
                "Unable to create a security group. Please ask an administrator to run "
                "`meadowrun-manage-ec2 install --allow-authorize-ips` or alternatively,"
                " ask them to manually create this security group and authorize your "
                "access to it with the following commands: `aws ec2 "
                f"create-security-group --description {group_name} --group-name "
                f"{group_name}` then `aws ec2 authorize-security-group-ingress "
                f"--group-name {group_name} --protocol tcp --port {ports} "
                f"--source-group {group_name}` then finally `aws ec2 "
                f"authorize-security-group-ingress --group-name {group_name} --protocol"
                f" tcp --port {ports} --cidr {await _get_current_ip_for_ssh()}/32`",
                True,
            ) from e

        ignore_boto3_error_code(
            lambda: security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "FromPort": from_port_int,
                        "ToPort": to_port_int,
                        "IpProtocol": "tcp",
                        "UserIdGroupPairs": [{"GroupId": security_group.id}],
                    }
                ]
            ),
            "InvalidPermission.Duplicate",
        )

    current_ip = await _get_current_ip_for_ssh()
    try:
        _authorize_current_ip_for_security_group(
            security_group, from_port_int, to_port_int, current_ip
        )
    except Exception as e:
        print(
            f"Warning, failed to authorize current IP address for ports {ports}. "
            "Most likely you will fail to connect to your services on these ports. "
            "Please ask an administrator to run `meadowrun-manage-ec2 install "
            "--allow-authorize-ips` or alternatively, ask them to manually authorize "
            "your access by running: `aws ec2 authorize-security-group-ingress "
            f"--group-name {group_name} --protocol tcp --port {ports} --cidr "
            f"{current_ip}/32` {e}",
        )

    return security_group.id


class LaunchEC2InstanceResult(abc.ABC):
    pass


@dataclasses.dataclass(frozen=True)
class LaunchEC2InstanceSuccess(LaunchEC2InstanceResult):
    """
    Indicates that launch_ec2_instance succeeded. public_address_continuation can be
    awaited to get the public address of the launched instance.
    """

    public_address_continuation: Awaitable[str]
    instance_id: str


class LaunchEC2InstanceTypeUnavailable(LaunchEC2InstanceResult):
    """
    Indicates that launch_ec2_instance failed because the specified instance type is not
    available.
    """

    @abc.abstractmethod
    def get_message(self) -> str:
        """Explains the failure"""
        pass

    @abc.abstractmethod
    def unusable_instance_types(
        self, all_instance_types: Iterable[CloudInstanceType]
    ) -> Iterable[Tuple[str, OnDemandOrSpotType]]:
        """
        Given all instance types, returns the instance types that are now considered
        unusable because we encountered this error. E.g. for a quota issue, we should
        ignore any instance types that are under the same quota that is being hit.
        """
        pass


@dataclasses.dataclass(frozen=True)
class LaunchEC2InstanceAwsCapacity(LaunchEC2InstanceTypeUnavailable):
    """
    Indicates that an EC2 instance couldn't be launched because of AWS capacity
    constraints
    """

    message: str
    instance_type: str
    on_demand_or_spot: OnDemandOrSpotType

    def get_message(self) -> str:
        return self.message

    def unusable_instance_types(
        self, all_instance_types: Iterable[CloudInstanceType]
    ) -> Iterable[Tuple[str, OnDemandOrSpotType]]:
        yield self.instance_type, self.on_demand_or_spot


@dataclasses.dataclass(frozen=True)
class LaunchEC2InstanceQuota(LaunchEC2InstanceTypeUnavailable):
    """Indicates that an EC2 instance couldn't be launched because a quota was hit"""

    message: str
    quota_code: str
    quota_instance_families: Tuple[str, ...]

    def get_message(self) -> str:
        return self.message

    def unusable_instance_types(
        self, all_instance_types: Iterable[CloudInstanceType]
    ) -> Iterable[Tuple[str, OnDemandOrSpotType]]:
        return (
            (instance_type.name, instance_type.on_demand_or_spot)
            for instance_type in all_instance_types
            if instance_family_from_type(instance_type.name)
            in self.quota_instance_families
        )


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
    volume_size_gb: int = 100,
) -> LaunchEC2InstanceResult:
    """
    Launches the specified EC2 instance. See derived classes of LaunchEC2InstanceResult
    for possible return values.
    """

    optional_args: Dict[str, Any] = {
        # TODO allow users to specify the size of the EBS they need
        "BlockDeviceMappings": [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": volume_size_gb,
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
        {
            "InsufficientInstanceCapacity",
            "MaxSpotInstanceCountExceeded",
            "VcpuLimitExceeded",
        },
        True,
    )
    if not success:
        if error_code == "InsufficientInstanceCapacity":
            return LaunchEC2InstanceAwsCapacity(
                f"AWS does not have enough capacity in region {region_name} to create a"
                f" {on_demand_or_spot} instance of type {instance_type}",
                instance_type,
                on_demand_or_spot,
            )
        elif error_code in ("MaxSpotInstanceCountExceeded", "VcpuLimitExceeded"):
            # I believe these error codes have the same meaning for spot/on-demand
            # instances
            return LaunchEC2InstanceQuota(
                *get_quota_for_instance_type(
                    instance_type, on_demand_or_spot, region_name
                )
            )
        else:
            raise ValueError(f"Unexpected boto3 error code {error_code}")

    assert instances is not None  # just for mypy
    return LaunchEC2InstanceSuccess(
        _launch_instance_continuation(instances[0]), instances[0].id
    )


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
        except asyncio.CancelledError:
            raise
        except Exception as e:
            exception = e
            event_loop.call_soon_threadsafe(
                lambda: instance_running_future.set_exception(exception)
            )

    threading.Thread(target=wait_until_running).start()
    await instance_running_future

    instance.load()
    if not instance.public_dns_name:
        # try one more time after waiting 1 seconds
        await asyncio.sleep(1.0)
        instance.load()
        if not instance.public_dns_name:
            raise ValueError("Waited until running, but still no IP address!")
    return instance.public_dns_name


async def launch_ec2_instances(
    resources_required_per_job: ResourcesInternal,
    num_jobs: int,
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
    instance_ids = []
    launched_instance_types_repeated = []

    # As we try to launch instances, we will realize that some of them cannot be
    # launched due to lack of capacity in AWS or quotas. We'll keep track of what
    # instance types aren't actually available in this variable
    unusable_instance_types: Set[Tuple[str, OnDemandOrSpotType]] = set()

    while num_jobs_left_to_allocate > 0:
        if unusable_instance_types:
            print(
                "Trying to proceed with less optimal instance types. This can result in"
                " significantly more expensive instances"
            )

        chosen_instance_types = choose_instance_types_for_job(
            resources_required_per_job,
            num_jobs_left_to_allocate,
            (
                i
                for i in instance_types
                if (i.name, i.on_demand_or_spot) not in unusable_instance_types
            ),
        )

        at_least_one_chosen_instance_type = False
        num_jobs_left_to_allocate = 0
        for instance_type in chosen_instance_types:
            at_least_one_chosen_instance_type = True
            for _ in range(instance_type.num_instances):
                launch_ec2_result = await launch_ec2_instance(
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
                if isinstance(launch_ec2_result, LaunchEC2InstanceSuccess):
                    public_dns_name_tasks.append(
                        launch_ec2_result.public_address_continuation
                    )
                    instance_ids.append(launch_ec2_result.instance_id)
                    launched_instance_types_repeated.append(instance_type)
                elif isinstance(launch_ec2_result, LaunchEC2InstanceTypeUnavailable):
                    print(launch_ec2_result.get_message())

                    # TODO this isn't exactly right, we can end up over-allocating
                    # because we might choose a larger instance than we "actually"
                    # needed, but this shouldn't be a huge factor
                    num_jobs_left_to_allocate += instance_type.workers_per_instance_full
                    unusable_instance_types.update(
                        launch_ec2_result.unusable_instance_types(instance_types)
                    )
                else:
                    raise ValueError(
                        "Unexpected type of launch_ec2_result "
                        f"{type(launch_ec2_result)}"
                    )

        if not at_least_one_chosen_instance_type:
            raise ValueError(
                "There were no instance types that could be selected for: "
                f"{resources_required_per_job}"
            )

    public_dns_names = await asyncio.gather(*public_dns_name_tasks)

    return [
        CloudInstance(public_dns_name, instance_id, instance_type)
        for public_dns_name, instance_id, instance_type in zip(
            public_dns_names, instance_ids, launched_instance_types_repeated
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
