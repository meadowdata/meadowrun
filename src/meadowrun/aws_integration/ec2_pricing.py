from __future__ import annotations

import dataclasses
import datetime
import io
import json
import logging
from typing import List, Iterable, Dict, Tuple

import aiohttp
import boto3
import pkg_resources

from meadowrun.aws_integration.aws_core import _boto3_paginate
from meadowrun.config import (
    AVX,
    AvxVersion,
    GPU,
    GPU_MEMORY,
    INTEL_DEEP_LEARNING_BOOST,
    EVICTION_RATE_INVERSE,
    LOGICAL_CPU,
    MEMORY_GB,
)
from meadowrun.instance_selection import CloudInstanceType, ResourcesInternal
from meadowrun.local_cache import get_cached_json, save_json_to_cache, clear_cache


_CACHED_EC2_PRICES_FILENAME = "aws_ec2_prices.json"


async def _get_ec2_instance_types(region_name: str) -> List[CloudInstanceType]:
    """
    Gets a list of EC2 instance types and their prices in the format expected by
    agent_creator:choose_instance_types_for_job
    """

    # TODO at some point add cross-region optimization

    try:
        cached = get_cached_json(
            _CACHED_EC2_PRICES_FILENAME, datetime.timedelta(hours=4)
        )
        if cached:
            return [CloudInstanceType.from_dict(cit) for cit in cached]
    except Exception as e:
        print(
            "Warning, cached EC2 prices file appears to be created by an old version "
            f"of Meadowrun or is corrupted, will regenerate: {e}"
        )

    result = list(_get_ec2_on_demand_prices(region_name))
    on_demand_instance_types = {
        instance_type.name: instance_type for instance_type in result
    }
    result.extend(await _get_ec2_spot_prices(region_name, on_demand_instance_types))

    save_json_to_cache(_CACHED_EC2_PRICES_FILENAME, [cit.to_dict() for cit in result])

    return result


def _get_region_description_for_pricing(region_code: str) -> str:
    """
    Mostly copy/pasted from
    https://stackoverflow.com/questions/51673667/use-boto3-to-get-current-price-for-given-ec2-instance-type

    Converts something like us-east-2 to US East (Ohio). Almost all APIs use
    region_code, but the pricing API weirdly uses description.
    """
    endpoint_file = pkg_resources.resource_filename("botocore", "data/endpoints.json")
    with open(endpoint_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Botocore is using Europe while Pricing API using EU...sigh...
    return data["partitions"][0]["regions"][region_code]["description"].replace(
        "Europe", "EU"
    )


def _get_ec2_on_demand_prices(region_name: str) -> Iterable[CloudInstanceType]:
    """
    Returns a dataframe with columns instance_type, memory_gb, logical_cpu, and price
    where price is the on-demand price
    """

    try:
        s3_client = boto3.client("s3", region_name="us-east-2")
        with io.BytesIO() as buffer:
            s3_client.download_fileobj("meadowrunprod", "aws_gpu_memory.json", buffer)
            buffer.seek(0)
            gpu_memory = json.loads(buffer.getvalue().decode("utf-8"))
    except Exception as e:
        logging.warning(
            "Unable to get most recent GPU memory data, will fall back to data in "
            f"package: {e}"
        )
        with open(
            pkg_resources.resource_filename(
                "meadowrun", "aws_integration/aws_gpu_memory.json"
            ),
            "r",
            encoding="utf-8",
        ) as file:
            gpu_memory = json.loads(file.read())

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

        consumable_resources = {}
        non_consumable_resources = {EVICTION_RATE_INVERSE: 100.0}

        # We don't expect the "warnings" to get hit, we just don't want to get thrown
        # off if the data format changes unexpectedly or something like that.

        if "physicalProcessor" not in attributes:
            print(
                f"Warning, skipping {instance_type} because physicalProcessor is not "
                "specified"
            )
            continue

        physical_processor = attributes["physicalProcessor"].lower()
        if "intel" in physical_processor:
            processor_brand = "intel"
        elif "amd" in physical_processor:
            processor_brand = "amd_cpu"
        elif "aws graviton" in physical_processor:
            processor_brand = "graviton"
        else:
            processor_brand = "other"
        # effectively, this skips Graviton (ARM-based) processors
        # TODO eventually support Graviton processors.
        if processor_brand not in ("intel", "amd_cpu"):
            # only log if we see non-Graviton processors
            if processor_brand != "graviton":
                print(
                    "Skipping non-Intel/AMD processor "
                    f"{attributes['physicalProcessor']} in {instance_type}"
                )
            continue
        non_consumable_resources[processor_brand] = 1.0

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
            consumable_resources[MEMORY_GB] = float(memory[: -len(" GiB")])
        except ValueError:
            print(
                f"Warning, skipping {instance_type} because memory isn't an float: "
                f"{memory}"
            )
            continue

        try:
            consumable_resources[LOGICAL_CPU] = float(attributes["vcpu"])
        except ValueError:
            print(
                f"Warning, skipping {instance_type} because vcpu isn't a number: "
                f"{attributes['vcpu']}"
            )
            continue

        if "gpu" in attributes:
            try:
                consumable_resources[GPU] = float(attributes["gpu"])
            except ValueError:
                print(
                    f"Warning, not including gpu resources for {instance_type} because "
                    f"gpu isn't a number: {attributes['gpu']}"
                )
            else:
                # If we have a GPU, we should label what kind of GPU we have. All EC2
                # instance types have nvidia GPUs except for the g4ad family
                if instance_type.lower().startswith("g4ad"):
                    non_consumable_resources["amd_gpu"] = 1.0
                else:
                    non_consumable_resources["nvidia"] = 1.0

                # also, we'll try to assign it GPU memory from our data
                if instance_type.lower() not in gpu_memory:
                    print(
                        f"Warning, skipping {instance_type.lower()} because we don't "
                        "know how much GPU memory it has"
                    )
                else:
                    consumable_resources[GPU_MEMORY] = gpu_memory[instance_type.lower()]

        avx_version = AvxVersion.NONE
        for feature in attributes.get("processorFeatures", "").split("; "):
            if feature in ("Intel AVX", "AVX"):
                avx_version = max(avx_version, AvxVersion.AVX)
            elif feature in ("Intel AVX2", "AVX2"):
                avx_version = max(avx_version, AvxVersion.AVX2)
            elif feature == "Intel AVX512":
                avx_version = max(avx_version, AvxVersion.AVX512)
            elif feature == "Intel Deep Learning Boost":
                non_consumable_resources[INTEL_DEEP_LEARNING_BOOST] = 1.0
            # ignore unknown features
        if avx_version != AvxVersion.NONE:
            non_consumable_resources[AVX] = float(avx_version)

        yield CloudInstanceType(
            instance_type,
            "on_demand",
            usd_price_float,
            ResourcesInternal(consumable_resources, non_consumable_resources),
        )


async def _get_ec2_spot_prices(
    region_name: str, on_demand_instance_types: Dict[str, CloudInstanceType]
) -> List[CloudInstanceType]:
    """Returns a list of CloudInstanceTypes for spot instances"""
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
            spot_instance_type = dataclasses.replace(
                on_demand_instance_type,
                price=price,
                on_demand_or_spot="spot",
                resources=on_demand_instance_type.resources.copy(),
            )
            # if interruption_probability is missing, just default to 80%
            spot_instance_type.resources.non_consumable[
                EVICTION_RATE_INVERSE
            ] = 100 - interruption_probabilities.get(instance_type, 80)
            results.append(spot_instance_type)

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
        response.raise_for_status()
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


def clear_prices_cache() -> None:
    clear_cache(_CACHED_EC2_PRICES_FILENAME)
