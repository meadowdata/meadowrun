from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime
import json
import threading
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

import aiohttp
import aiohttp.client_exceptions
import boto3
import botocore.exceptions
from pkg_resources import resource_filename

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)
from meadowrun.instance_selection import (
    ChosenEC2InstanceType,
    EC2InstanceType,
    OnDemandOrSpotType,
    Resources,
    choose_instance_types_for_job,
)

# A security group that allows SSH, clients' IP addresses get added as needed
_MEADOWRUN_SSH_SECURITY_GROUP = "meadowrunSshSecurityGroup"

_EC2_ASSUME_ROLE_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"}
        }
    ]
}"""

_LAMBDA_ASSUME_ROLE_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" }
        }
    ]
}"""


_T = TypeVar("_T")


def _boto3_paginate(method: Any, **kwargs: Any) -> Iterable[Any]:
    paginator = method.__self__.get_paginator(method.__name__)
    for page in paginator.paginate(**kwargs).result_key_iters():
        for item in page:
            yield item


async def _get_ec2_metadata(url_suffix: str) -> Optional[str]:
    """
    Queries the EC2 metadata endpoint, which gives us information about the EC2 instance
    we're currently running on:
    https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
    Returns None if the endpoint is not available because e.g. we're not running on an
    EC2 instance.
    """
    try:
        async with aiohttp.request(
            "GET", f"http://169.254.169.254/latest/meta-data/{url_suffix}"
        ) as response:
            return await response.text()
    except aiohttp.client_exceptions.ClientConnectorError:
        # the AWS metadata endpoint is not available, probably because we're not on
        # an EC2 instance.
        pass

    return None


async def _get_default_region_name() -> str:
    """
    Tries to get the default region name. E.g. us-east-2. First sees if the AWS CLI is
    set up, and returns the equivalent of `aws configure get region`. Then checks if we
    are running on an EC2 instance in which case we check the AWS metadata endpoint

    TODO this function is overused almost everywhere. Currently, we just always use the
    default region for everything and don't support multi-region deployments, but region
    should be a first-class concept.
    """

    default_session = boto3._get_default_session()
    if default_session is not None and default_session.region_name:
        # equivalent of `aws configure get region`
        return default_session.region_name
    else:
        result = await _get_ec2_metadata("placement/region")
        if result:
            return result
        else:
            raise ValueError(
                "region_name was not specified, we are not logged into AWS locally, and"
                " we're not running on an EC2 instance, so we have no way of picking a "
                "default region."
            )


def _iam_role_exists(iam_client: Any, role_name: str) -> bool:
    """Returns True if the specified IAM role exists, otherwise returns False"""
    try:
        iam_client.get_role(RoleName=role_name)
        return True
    except Exception as e:
        # Unfortunately boto3 appears to have dynamic exception types. So type(e) would
        # be "botocore.errorfactory.NoSuchEntityException", but NoSuchEntityException
        # can't be imported from botocore.errorfactory.
        if type(e).__name__ == "NoSuchEntityException":
            return False

        raise


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


async def _get_current_ip_for_ssh() -> str:
    """
    Gets an ip address for the current machine that is likely to work for allowing SSH
    into an EC2 instance.
    """
    # if we're already in an EC2 instance, use the EC2 metadata to get our private IP
    private_ip = await _get_ec2_metadata("local-ipv4")
    if private_ip:
        return private_ip

    # otherwise, we'll use checkip.amazonaws.com to figure out how AWS sees our IP
    async with aiohttp.request("GET", "https://checkip.amazonaws.com/") as response:
        return (await response.text()).strip()


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

    if on_demand_or_spot == "on_demand":
        if user_data:
            optional_args["UserData"] = user_data
        if tags:
            optional_args["TagSpecifications"] = [
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": key, "Value": value} for key, value in tags.items()
                    ],
                }
            ]

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
            # boto3 doesn't have any async APIs, which means that in order to run more
            # than one launch_ec2_instance at the same time, we need to have a thread
            # that waits. We use an asyncio.Future here to make the API async, so from
            # the user perspective, it feels like this function is async

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
    elif on_demand_or_spot == "spot":
        if user_data:
            optional_args["UserData"] = base64.b64encode(
                user_data.encode("utf-8")
            ).decode("utf-8")

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.request_spot_instances
        client = boto3.client("ec2", region_name=region_name)
        spot_instance_request = await _retry_iam_instance_profile(
            lambda: client.request_spot_instances(
                InstanceCount=1,
                LaunchSpecification={
                    "ImageId": ami_id,
                    "InstanceType": instance_type,
                    **optional_args,
                },
            )
        )

        if wait_for_dns_name or tags:
            # see above for comment about boto3 async/threads
            spot_instance_request_id = spot_instance_request["SpotInstanceRequests"][0][
                "SpotInstanceRequestId"
            ]

            instance_running_future = asyncio.Future()
            event_loop = asyncio.get_running_loop()
            waiter = client.get_waiter("spot_instance_request_fulfilled")

            def wait_until_running() -> None:
                try:
                    waiter.wait(SpotInstanceRequestIds=[spot_instance_request_id])
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

            instance_id = client.describe_spot_instance_requests(
                SpotInstanceRequestIds=[spot_instance_request_id]
            )["SpotInstanceRequests"][0]["InstanceId"]

            # now that we have an instance id, we can add our tags
            # TODO if we don't manage to tag our instance before the process crashes, we
            # need to keep track of that
            if tags:
                client.create_tags(
                    Resources=[instance_id],
                    Tags=[{"Key": key, "Value": value} for key, value in tags.items()],
                )

            return client.describe_instances(InstanceIds=[instance_id])["Reservations"][
                0
            ]["Instances"][0]["PublicDnsName"]
        else:
            return None
    else:
        raise ValueError(f"Unexpected value for on_demand_or_spot {on_demand_or_spot}")


@dataclasses.dataclass(frozen=True)
class EC2Instance:
    """
    Represents an EC2 instance launched by launch_ec2_instances, see that function for
    details
    """

    public_dns_name: str

    # TODO instance_type.interruption_probability should always use the latest data
    # rather than always using the number from when the instance was launched
    instance_type: ChosenEC2InstanceType


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
) -> Sequence[EC2Instance]:
    """
    Launches enough EC2 instances to run num_jobs jobs that each require the specified
    amount of CPU/memory. Returns a sequence of EC2Instances. EC2Instance.max_jobs
    indicates the maximum number of jobs that can run on that instance. The sum of all
    of the EC2Instance.max_jobs will be greater than or equal to the original num_jobs
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
                    instance_type.ec2_instance_type.name,
                    instance_type.ec2_instance_type.on_demand_or_spot,
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
        EC2Instance(public_dns_name, instance_type)
        for public_dns_name, instance_type in zip(
            public_dns_names, chosen_instance_types_repeated
        )
    ]


async def _get_ec2_instance_types(region_name: str) -> List[EC2InstanceType]:
    """
    Gets a dataframe describing EC2 instance types and their prices in the format
    expected by agent_creator:choose_instance_types_for_job
    """

    # TODO at some point add cross-region optimization

    result = list(_get_ec2_on_demand_prices(region_name))
    on_demand_instance_types = {
        instance_type.name: instance_type for instance_type in result
    }
    result.extend(await _get_ec2_spot_prices(region_name, on_demand_instance_types))
    return result


def _get_region_description_for_pricing(region_code: str) -> str:
    """
    Mostly copy/pasted from
    https://stackoverflow.com/questions/51673667/use-boto3-to-get-current-price-for-given-ec2-instance-type

    Converts something like us-east-2 to US East (Ohio). Almost all APIs use
    region_code, but the pricing API weirdly uses description.
    """
    endpoint_file = resource_filename("botocore", "data/endpoints.json")
    with open(endpoint_file, "r") as f:
        data = json.load(f)
    # Botocore is using Europe while Pricing API using EU...sigh...
    return data["partitions"][0]["regions"][region_code]["description"].replace(
        "Europe", "EU"
    )


def _get_ec2_on_demand_prices(region_name: str) -> Iterable[EC2InstanceType]:
    """
    Returns a dataframe with columns instance_type, memory_gb, logical_cpu, and price
    where price is the on-demand price
    """

    # All comments about the pricing API are based on
    # https://www.sentiatechblog.com/using-the-ec2-price-list-api

    # us-east-1 is the only region this pricing API is available and the pricing
    # endpoint in us-east-1 has pricing data for all regions.
    pricing_client = boto3.client("pricing", region_name="us-east-1")

    filters = [
        # only get prices for the specified region
        {
            "Type": "TERM_MATCH",
            "Field": "location",
            "Value": _get_region_description_for_pricing(region_name),
        },
        # filter out instance types that come with SQL Server pre-installed
        {
            "Type": "TERM_MATCH",
            "Field": "preInstalledSw",
            "Value": "NA",
        },
        # limit ourselves to just Linux instances for now
        # TODO add support for Windows eventually
        {
            "Type": "TERM_MATCH",
            "Field": "operatingSystem",
            "Value": "Linux",
        },
        # Shared is a "regular" EC2 instance, as opposed to Dedicated and Host
        {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
        # This relates to EC2 capacity reservations. Used is correct for when we don't
        # have any reservations
        {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
    ]

    for product_json in _boto3_paginate(
        pricing_client.get_products,
        Filters=filters,
        ServiceCode="AmazonEC2",
        FormatVersion="aws_v1",
    ):
        product = json.loads(product_json)
        attributes = product["product"]["attributes"]
        instance_type = attributes["instanceType"]

        # We don't expect the "warnings" to get hit, we just don't want to get thrown
        # off if the data format changes unexpectedly or something like that.

        if "physicalProcessor" not in attributes:
            print(
                f"Warning, skipping {instance_type} because physicalProcessor is not "
                "specified"
            )
            continue

        # effectively, this skips Graviton (ARM-based) processors
        # TODO eventually support Graviton processors.
        if (
            "intel" not in attributes["physicalProcessor"].lower()
            and "amd" not in attributes["physicalProcessor"].lower()
        ):
            # only log if we see non-Graviton processors
            if "AWS Graviton" not in attributes["physicalProcessor"]:
                print(
                    "Skipping non-Intel/AMD processor "
                    f"{attributes['physicalProcessor']} in {instance_type}"
                )
            continue

        if "OnDemand" not in product["terms"]:
            print(
                f"Warning, skipping {instance_type} because there was no OnDemand terms"
            )
            continue
        on_demand = list(product["terms"]["OnDemand"].values())
        if len(on_demand) != 1:
            print(
                f"Warning, skipping {instance_type} because there was more than one "
                "OnDemand SKU"
            )
            continue

        price_dimensions = list(on_demand[0]["priceDimensions"].values())
        if len(price_dimensions) != 1:
            print(
                f"Warning, skipping {instance_type} because there was more than one "
                "priceDimensions"
            )
            continue
        pricing = price_dimensions[0]

        if pricing["unit"] != "Hrs":
            print(
                f"Warning, skipping {instance_type} because the pricing unit is not "
                f"Hrs: {pricing['unit']}"
            )
            continue
        if "USD" not in pricing["pricePerUnit"]:
            print(
                f"Warning, skipping {instance_type} because the pricing is not in USD"
            )
            continue
        usd_price = pricing["pricePerUnit"]["USD"]

        try:
            usd_price_float = float(usd_price)
        except ValueError:
            print(
                f"Warning, skipping {instance_type} because the price is not a float: "
                f"{usd_price}"
            )
            continue

        memory = attributes["memory"]
        if not memory.endswith(" GiB"):
            print(
                f"Warning, skipping {instance_type} because memory doesn't end in GiB: "
                f"{memory}"
            )
            continue
        try:
            memory_gb_float = float(memory[: -len(" GiB")])
        except ValueError:
            print(
                f"Warning, skipping {instance_type} because memory isn't an float: "
                f"{memory}"
            )
            continue

        try:
            vcpu_int = int(attributes["vcpu"])
        except ValueError:
            print(
                f"Warning, skipping {instance_type} because vcpu isn't an int: "
                f"{attributes['vcpu']}"
            )
            continue

        yield EC2InstanceType(
            instance_type, memory_gb_float, vcpu_int, usd_price_float, 0, "on_demand"
        )


async def _get_ec2_spot_prices(
    region_name: str, on_demand_instance_types: Dict[str, EC2InstanceType]
) -> List[EC2InstanceType]:
    """
    Returns a dataframe with columns instance_type and price, where price is the latest
    spot price
    """
    ec2_client = boto3.client("ec2", region_name=region_name)

    # There doesn't appear to be an API for "give me the latest spot price for each
    # instance type". Instead, there's an API to get the spot price history. We query
    # for the last hour, assuming that all the instances we care about will have prices
    # within that last hour (no way to know whether that's actually true or not).
    start_time = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
    # For each instance type, maps to the price, and the timestamp for that price. We're
    # going to always keep just the latest price. If we have multiple prices at the same
    # timestamp, we'll just take the largest one (this could happen because we get
    # different prices for different availability zones, e.g. us-east-2b vs us-east-2c).
    # TODO eventually account for AvailabilityZone?
    spot_prices: Dict[str, Tuple[float, datetime.datetime]] = {}
    for spot_price_record in _boto3_paginate(
        ec2_client.describe_spot_price_history,
        ProductDescriptions=["Linux/UNIX"],
        StartTime=start_time,
        MaxResults=10000,
    ):
        instance_type = spot_price_record["InstanceType"]
        timestamp = spot_price_record["Timestamp"]
        price = float(spot_price_record["SpotPrice"])
        prev_value = spot_prices.get(instance_type)

        if (
            prev_value is None
            or prev_value[1] < timestamp
            or (prev_value[1] == timestamp and prev_value[0] < price)
        ):
            spot_prices[instance_type] = price, timestamp

    # get interruption probabilities
    interruption_probabilities = await _get_ec2_interruption_probabilities(region_name)

    # TODO we should consider warning if we get spot prices or interruption
    # probabilities where we don't have on_demand_prices or spot_prices respectively,
    # right now we just drop that data

    results = []
    for instance_type, (price, timestamp) in spot_prices.items():
        # drop rows where we don't have the corresponding on_demand instance type
        # information
        if instance_type in on_demand_instance_types:
            on_demand_instance_type = on_demand_instance_types[instance_type]
            results.append(
                dataclasses.replace(
                    on_demand_instance_type,
                    price=price,
                    # if interruption_probability is missing, just default to 80%
                    interruption_probability=interruption_probabilities.get(
                        instance_type, 80
                    ),
                    on_demand_or_spot="spot",
                )
            )

    return results


async def _get_ec2_interruption_probabilities(region_name: str) -> Dict[str, float]:
    """
    Returns a dataframe with columns instance_type, interruption_probability.
    interruption_probability is a percent, so values range from 0 to 100
    """

    # this is the data that drives https://aws.amazon.com/ec2/spot/instance-advisor/
    # according to
    # https://blog.doit-intl.com/spotinfo-a-new-cli-for-aws-spot-a9748bbe338f
    async with aiohttp.request(
        "GET", "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"
    ) as response:
        data = await response.json()

    # The data we get isn't documented, but appears straightforward and can be checked
    # against the Spot Instance Advisor webpage. Each instance type gets an "r" which
    # corresponds to a range of interruption probabilities. The ranges are defined in
    # data["ranges"]. Each range has a "human readable label" like 15-20% and a "max"
    # like 22 (even though 20 != 22). We take an average interruption probability based
    # on the range implied by the maxes.

    # Get the average interruption probability for each range
    range_maxes = {r["index"]: r["max"] for r in data["ranges"]}
    range_keys_sorted = list(sorted(range_maxes.keys()))
    if range_keys_sorted != list(range(max(range_maxes.keys()) + 1)):
        raise ValueError(
            "Unexpected: ranges are not indexed contiguously from 0: "
            + ", ".join(str(key) for key in range_maxes.keys())
        )

    range_averages = {}
    for key in range_keys_sorted:
        if key == 0:
            range_min = 0
        else:
            range_min = range_maxes[key - 1]
        range_averages[key] = (range_maxes[key] + range_min) / 2

    # Get the average interruption probability for Linux instance_types in the specified
    # region
    return {
        instance_type: range_averages[values["r"]]
        for instance_type, values in data["spot_advisor"][region_name]["Linux"].items()
    }
