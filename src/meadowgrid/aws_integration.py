from __future__ import annotations

import asyncio
import base64
import datetime
import json
import traceback
from types import TracebackType
from typing import Optional, Any, Callable, TypeVar, Tuple, Type, Generator, Iterable

import aiohttp
import aiohttp.client_exceptions
import boto3
import botocore.client
import botocore.exceptions
import pandas as pd

from meadowgrid.agent_creator import AgentCreator, OnDemandOrSpotType
from meadowgrid.config import DEFAULT_COORDINATOR_PORT, EC2_PRICES_UPDATE_SECS
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientAsync


_MEADOWGRID_COORDINATOR_ROLE = "meadowgridCoordinatorRole"
_MEADOWGRID_COORDINATOR_SECURITY_GROUP = "meadowgridCoordinatorSecurityGroup"
_MEADOWGRID_AGENT_SECURITY_GROUP = "meadowgridAgentSecurityGroup"
_COORDINATOR_AWS_AMI = "ami-0e2b160b07bab8a4b"
_AGENT_AWS_AMI = "ami-00fefcea9b035e2d6"


_T = TypeVar("_T")


def _ignore_boto3_error_code(
    func: Callable[[], _T], error_code: str
) -> Tuple[bool, Optional[_T]]:
    """
    Calls func. If func succeeds, return (True, result of func). If func raises a boto3
    error with the specified code, returns False, None. If func raises any other type of
    exception (i.e. a boto3 error with a different code or a different type of error
    altogether), then the exception is raised normally.
    """
    try:
        return True, func()
    except botocore.exceptions.ClientError as e:
        if "Error" in e.response:
            error = e.response["Error"]
            if "Code" in error and error["Code"] == error_code:
                return False, None

        raise


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
    """

    if boto3.DEFAULT_SESSION is not None:
        # equivalent of `aws configure get region`
        return boto3.DEFAULT_SESSION.region_name
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


def _ensure_meadowgrid_coordinator_iam_role(region_name: str) -> None:
    """
    Creates the meadowgrid coordinator IAM role if it doesn't exist, give it permissions
    to create EC2 instances and get prices

    TODO does not try to update the role if/when we change the policies below
    """

    iam = boto3.client("iam", region_name=region_name)
    if not _iam_role_exists(iam, _MEADOWGRID_COORDINATOR_ROLE):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
        # TODO look into MaxSessionDuration parameter, roles potentially expiring?
        iam.create_role(
            RoleName=_MEADOWGRID_COORDINATOR_ROLE,
            # allow EC2 instances to assume this role
            AssumeRolePolicyDocument=(
                '{"Statement": '
                "["
                '{"Action": "sts:AssumeRole", "Effect": "Allow", '
                '"Principal": {"Service": "ec2.amazonaws.com"}}'
                "], "
                '"Version": "2012-10-17"}'
            ),
            Description="Enables the meadowgrid coordinator to create EC2 instances and"
            " get prices",
        )

        # enable the coordinator to create instances
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.attach_role_policy
        iam.attach_role_policy(
            RoleName=_MEADOWGRID_COORDINATOR_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess",
        )

        # enable the coordinator to get prices
        iam.attach_role_policy(
            RoleName=_MEADOWGRID_COORDINATOR_ROLE,
            PolicyArn="arn:aws:iam::aws:policy/AWSPriceListServiceFullAccess",
        )


def _get_ec2_security_group(ec2_resource: Any, name: str) -> Any:
    """
    Gets the specified security group if it exists. Returns an
    Optional[boto3.resources.factory.ec2.SecurityGroup] (not in the type signature
    because boto3 uses dynamic types).
    """
    success, groups = _ignore_boto3_error_code(
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


def _ensure_meadowgrid_agent_security_group(
    ec2_resource: Any, current_ip_for_ssh: str
) -> str:
    """
    Creates the meadowgrid agent security group if it doesn't exist. The main purpose of
    this security group is to allow inbound traffic on the meadowgrid coordinator
    security group so that the agents can communicate with the coordinator (see
    _ensure_meadowgrid_coordinator_security_group). We also enable SSH from the current
    IP. Returns the id of the meadowgrid agent security group
    """
    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWGRID_AGENT_SECURITY_GROUP
    )
    if security_group is None:
        security_group = ec2_resource.create_security_group(
            Description="security group for meadowgrid agents",
            GroupName=_MEADOWGRID_AGENT_SECURITY_GROUP,
        )

    # enable SSH. TODO we should be able to turn this on/off
    _ignore_boto3_error_code(
        lambda: security_group.authorize_ingress(
            IpProtocol="tcp",
            CidrIp=f"{current_ip_for_ssh}/32",
            FromPort=22,
            ToPort=22,
        ),
        "InvalidPermission.Duplicate",
    )

    return security_group.id


async def _ensure_meadowgrid_security_groups(ec2_resource: Any) -> str:
    """
    Creates the meadowgrid coordinator security group and meadowgrid agent security
    group if they doesn't exist. The coordinator security group allows meadowgrid agents
    and the current ip to access the coordinator, as well as allowing the current ip to
    ssh. See also _ensure_meadowgrid_agent_security_group.
    """
    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWGRID_COORDINATOR_SECURITY_GROUP
    )
    if security_group is None:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.create_security_group
        security_group = ec2_resource.create_security_group(
            Description="security group for meadowgrid coordinator",
            GroupName=_MEADOWGRID_COORDINATOR_SECURITY_GROUP,
        )

    current_ip_for_ssh = await _get_current_ip_for_ssh()

    # allow meadowgrid traffic from the meadowgrid agent security group
    agent_security_group_id = _ensure_meadowgrid_agent_security_group(
        ec2_resource, current_ip_for_ssh
    )
    _ignore_boto3_error_code(
        lambda: security_group.authorize_ingress(
            IpPermissions=[
                {
                    "FromPort": DEFAULT_COORDINATOR_PORT,
                    "ToPort": DEFAULT_COORDINATOR_PORT,
                    "IpProtocol": "tcp",
                    "UserIdGroupPairs": [{"GroupId": agent_security_group_id}],
                }
            ]
        ),
        "InvalidPermission.Duplicate",
    )

    # allow meadowgrid traffic from the current machine
    # TODO this is a bit hacky, probably the user should be able to specify what IP
    # addresses are able to interact with the coordinator. Also if we're allowing
    # external traffic, then we should probably be encrypting all of the traffic.
    _ignore_boto3_error_code(
        lambda: security_group.authorize_ingress(
            IpProtocol="tcp",
            CidrIp=f"{current_ip_for_ssh}/32",
            FromPort=DEFAULT_COORDINATOR_PORT,
            ToPort=DEFAULT_COORDINATOR_PORT,
        ),
        "InvalidPermission.Duplicate",
    )

    # allow SSH from the current machine TODO this should be configurable
    _ignore_boto3_error_code(
        lambda: security_group.authorize_ingress(
            IpProtocol="tcp", CidrIp=f"{current_ip_for_ssh}/32", FromPort=22, ToPort=22
        ),
        "InvalidPermission.Duplicate",
    )

    return security_group.id


async def launch_meadowgrid_coordinator(region_name: Optional[str]) -> str:
    """
    Launches a meadowgrid coordinator in AWS. Returns the address of the coordinator,
    e.g. 1.1.1.1:15319

    TODO the coordinator will never get shutdown automatically. Also, there should be a
    way to share coordinators.
    """
    if region_name is None:
        region_name = await _get_default_region_name()

    ec2_resource = boto3.resource("ec2", region_name=region_name)
    _ensure_meadowgrid_coordinator_iam_role(region_name)
    security_group_id = await _ensure_meadowgrid_security_groups(ec2_resource)

    # Create the coordinator instance
    # TODO we've just hardcoded the instance type for the coordinator for now
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
    instance = ec2_resource.create_instances(
        ImageId=_COORDINATOR_AWS_AMI,
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
        SecurityGroupIds=[security_group_id],
        IamInstanceProfile={"Name": _MEADOWGRID_COORDINATOR_ROLE},
    )[0]

    # wait until it gets a public IP address
    while not instance.public_ip_address:
        # TODO add a (configurable) timeout
        await asyncio.sleep(0.2)
        instance.load()

    # now wait until check() returns True
    coordinator_address = f"{instance.public_ip_address}:{DEFAULT_COORDINATOR_PORT}"

    async with MeadowGridCoordinatorClientAsync(coordinator_address) as client:
        # TODO add a (configurable) timeout
        while True:
            await asyncio.sleep(0.2)
            try:
                if await client.check():
                    break
            except Exception:
                # TODO there are probably some exceptions we shouldn't ignore
                pass

    return instance.public_ip_address


class AwsAgentCreator(AgentCreator):
    """
    Allows the coordinator to create agents in AWS. This only works if the coordinator
    is also running in EC2 with the right IAM role and security group ( see
    launch_meadowgrid_coordinator).
    """

    def __init__(self, region_name: Optional[str]) -> None:
        """
        Creates instances in the specified region. If no region is specified, we'll use
        _get_default_region_name.
        """
        self._awaited = False
        self._region_name = region_name

    async def __aenter__(self) -> AwsAgentCreator:
        if self._awaited:
            return self

        if self._region_name is None:
            self._region_name = await _get_default_region_name()

        # describes the available instance types in EC2 including their costs. See
        # agent_creator:choose_instance_types_for_job for the columns this dataframe has
        self._ec2_instance_types: Optional[pd.DataFrame] = None
        # a permanently running task that periodically gets EC2 instance type data. The
        # things that will change are the prices and interruption probabilities.
        self._update_ec2_instance_types_task = asyncio.create_task(
            self._update_ec2_instance_types()
        )
        # An event that tells us when we've gotten at least one download of the EC2
        # instance types. Warning, if there's an issue getting the instance types data,
        # this event will just never trigger.
        self._first_update_of_ec2_instance_types = asyncio.Event()

        # get an address that agents we create can use to talk to us (the coordinator)
        private_ip = await _get_ec2_metadata("local-ipv4")
        if private_ip is None:
            raise ValueError(
                "The AwsAgentCreator can only be used from an EC2 instance."
            )
        # TODO add support for running outside of EC2. Usually requires non-trivial
        # network setup by the user.
        self._coordinator_host_for_agents = private_ip

        self._awaited = True
        return self

    async def _update_ec2_instance_types(self) -> None:
        """Refresh EC2 instance types data (i.e. prices) in a loop"""
        assert self._region_name is not None  # just for mypy

        first = True
        while True:
            try:
                self._ec2_instance_types = await _get_ec2_instance_types(
                    self._region_name
                )
            except Exception:
                # TODO this should probably be more prominent somehow
                print("Error trying to get EC2 prices")
                traceback.print_exc()
            finally:
                if first:
                    self._first_update_of_ec2_instance_types.set()
                    first = False

            await asyncio.sleep(EC2_PRICES_UPDATE_SECS)

    async def get_instance_types(self) -> Optional[pd.DataFrame]:
        # if we haven't gotten anything in 5 minutes, something is probably wrong, and
        # we should give up on being able to create agents
        await asyncio.wait_for(self._first_update_of_ec2_instance_types.wait(), 60 * 5)
        return self._ec2_instance_types

    async def launch_job_specific_agent(
        self,
        agent_id: str,
        job_id: str,
        instance_type: str,
        on_demand_or_spot: OnDemandOrSpotType,
    ) -> None:
        assert self._region_name is not None  # just for mypy

        await _launch_job_specific_agent(
            agent_id,
            job_id,
            instance_type,
            self._coordinator_host_for_agents,
            on_demand_or_spot,
            self._region_name,
        )

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    def __await__(self) -> Generator[Any, None, AwsAgentCreator]:
        return self.__aenter__().__await__()

    async def close(self) -> None:
        # cancel and wait until the loop exits.
        # If we don't wait we get "Task was destroyed but it is pending" warnings.
        self._update_ec2_instance_types_task.cancel()
        try:
            await self._update_ec2_instance_types_task
        except asyncio.exceptions.CancelledError:
            pass


async def _launch_job_specific_agent(
    agent_id: str,
    job_id: str,
    instance_type: str,
    coordinator_host: str,
    on_demand_or_spot: OnDemandOrSpotType,
    region_name: str,
) -> None:
    ec2_resource = boto3.resource("ec2", region_name=region_name)

    security_group = _get_ec2_security_group(
        ec2_resource, _MEADOWGRID_AGENT_SECURITY_GROUP
    )
    if security_group is None:
        raise ValueError(
            f"{_MEADOWGRID_AGENT_SECURITY_GROUP} doesn't exist, this should have been "
            "created along with the coordinator security group"
        )

    # this file will get picked up by the systemd definition as an EnvironmentFile and
    # used to populate command line arguments to the agent process (see
    # build_meadowdata_amis.md)
    user_data = f"""#!/bin/bash
echo COORDINATOR_HOST={coordinator_host} > /meadowgrid/agent.conf
echo AGENT_ID={agent_id} >> /meadowgrid/agent.conf
echo JOB_ID={job_id} >> /meadowgrid/agent.conf
"""

    if on_demand_or_spot == "on_demand":
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
        _ = ec2_resource.create_instances(
            ImageId=_AGENT_AWS_AMI,
            MinCount=1,
            MaxCount=1,
            InstanceType=instance_type,
            SecurityGroupIds=[security_group.id],
            UserData=user_data,
        )[0]
    elif on_demand_or_spot == "spot":
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.request_spot_instances
        client = boto3.client("ec2", region_name=region_name)
        _ = client.request_spot_instances(
            InstanceCount=1,
            LaunchSpecification={
                "SecurityGroupIds": [security_group.id],
                "ImageId": _AGENT_AWS_AMI,
                "InstanceType": instance_type,
                "UserData": base64.b64encode(user_data.encode("utf-8")).decode("utf-8"),
            },
        )
    else:
        raise ValueError(f"Unexpected value for on_demand_or_spot {on_demand_or_spot}")


async def _get_ec2_instance_types(region_name: str) -> pd.DataFrame:
    """
    Gets a dataframe describing EC2 instance types and their prices in the format
    expected by agent_creator:choose_instance_types_for_job
    """

    # TODO at some point add cross-region optimization

    # the on_demand_prices dataframe also contains e.g. CPU/memory information
    on_demand_prices = _get_ec2_on_demand_prices(region_name)
    spot_prices = _get_ec2_spot_prices(region_name)
    interruption_probabilities = await _get_ec2_interruption_probability(region_name)

    # Enrich the spot_prices data with CPU/memory information from on_demand_prices
    # and interruption_probabilities
    # TODO we should consider warning if we get spot prices or interruption
    # probabilities where we don't have on_demand_prices or spot_prices respectively,
    # right now we just drop that data
    spot_prices = (
        on_demand_prices.drop(["price"], axis=1)
        .merge(spot_prices, on="instance_type", how="inner")
        .merge(interruption_probabilities, on="instance_type", how="left")
    )

    # If we have spot instances that don't have interruption probabilities, just assume
    # a relatively high interruption_probability.
    spot_prices["interruption_probability"] = spot_prices[
        "interruption_probability"
    ].fillna(80)

    # combine on_demand and spot data and return
    prices = pd.concat(
        [
            spot_prices.assign(on_demand_or_spot="spot"),
            on_demand_prices.assign(
                on_demand_or_spot="on_demand", interruption_probability=0
            ),
        ]
    )
    return prices


def _get_ec2_on_demand_prices(region_name: str) -> pd.DataFrame:
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
            "Field": "regionCode",
            "Value": region_name,
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

    records = []
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
            print(
                f"Skipping non-Intel/AMD processor {attributes['physicalProcessor']} in"
                f" {instance_type}"
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

        records.append((instance_type, memory_gb_float, vcpu_int, usd_price_float))

    return pd.DataFrame.from_records(
        records, columns=["instance_type", "memory_gb", "logical_cpu", "price"]
    )


def _get_ec2_spot_prices(region_name: str) -> pd.DataFrame:
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
    column_names = None
    values = []
    for price in _boto3_paginate(
        ec2_client.describe_spot_price_history,
        ProductDescriptions=["Linux/UNIX"],
        StartTime=start_time,
        MaxResults=10000,
    ):
        if column_names is None:
            column_names = list(price.keys())
        values.append(list(price.values()))
    # The columns at this point are AvailabilityZone, InstanceType, ProductDescription,
    # SpotPrice, Timestamp
    spot_prices = pd.DataFrame(values, columns=column_names)
    spot_prices["SpotPrice"] = spot_prices["SpotPrice"].astype(float)

    # We just want one spot price per instance type, so take the latest spot price for
    # each instance type, and if there are multiple spot prices for the same instance
    # type at the same timestamp, just take the largest one. We ignore AvailabilityZone
    # (e.g. the same instance type could have different prices in us-east-2b and
    # us-east-2c) because we assume the differences are small there.
    # TODO eventually account for AvailabilityZone?
    return (
        spot_prices.sort_values(["Timestamp", "SpotPrice"], ascending=False)
        .drop_duplicates(["InstanceType"], keep="first")[["InstanceType", "SpotPrice"]]
        .rename(columns={"InstanceType": "instance_type", "SpotPrice": "price"})
    )


async def _get_ec2_interruption_probability(region_name: str) -> pd.DataFrame:
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
    r_to_interruption_probability = (
        pd.DataFrame.from_records(
            [(r["index"], r["max"]) for r in data["ranges"]], columns=["index", "max"]
        )
        .sort_values("index")
        .set_index("index")
    )
    r_to_interruption_probability["min"] = (
        r_to_interruption_probability["max"].shift().fillna(0)
    )
    r_to_interruption_probability["average"] = (
        r_to_interruption_probability["min"] + r_to_interruption_probability["max"]
    ) / 2

    # Get the average interruption probability for Linux instance_types in the specified
    # region
    return pd.DataFrame.from_records(
        [
            (instance_type, r_to_interruption_probability["average"][values["r"]])
            for instance_type, values in data["spot_advisor"][region_name][
                "Linux"
            ].items()
        ],
        columns=["instance_type", "interruption_probability"],
    )
