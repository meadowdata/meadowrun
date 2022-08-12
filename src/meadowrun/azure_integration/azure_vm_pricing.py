from typing import Dict, Tuple, Optional, Iterable, Any, List, cast

import aiohttp

from meadowrun.azure_integration.azure_meadowrun_core import get_subscription_id
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api_paged,
)
from meadowrun.config import (
    GPU,
    EVICTION_RATE_INVERSE,
    LOGICAL_CPU,
    MEMORY_GB,
)
from meadowrun.instance_selection import (
    CloudInstanceType,
    ON_DEMAND_OR_SPOT_VALUES,
    OnDemandOrSpotType,
    ResourcesInternal,
)

_RELEVANT_CAPABILITIES = {
    "GPUs",
    "HyperVGenerations",
    "MemoryGB",
    "vCPUs",
    "vCPUsAvailable",
}


async def _get_vm_skus(location: str) -> Iterable[Tuple[str, Dict[str, float]]]:
    """Returns [(name, consumable_resources), ...]"""
    seen_names = set()
    result = []

    # Unfortunately the slightly friendlier client.virtual_machine_sizes.list function
    # is deprecated as per
    # https://docs.microsoft.com/en-us/rest/api/compute/virtual-machine-sizes/list
    #
    # The new API is https://docs.microsoft.com/en-us/rest/api/compute/resource-skus The
    # underlying REST API does not support any filters other than location
    async for page in azure_rest_api_paged(
        "GET",
        f"subscriptions/{await get_subscription_id()}/providers/Microsoft.Compute/skus",
        "2021-07-01",
        query_parameters={"$filter": f"location eq '{location}'"},
    ):
        for r in page["value"]:
            if r["resourceType"] == "virtualMachines":
                capabilities = {
                    c["name"]: c["value"]
                    for c in r["capabilities"]
                    if c["name"] in _RELEVANT_CAPABILITIES
                }
                if "V2" in capabilities.get("HyperVGenerations", ""):
                    consumable_resources: Dict[str, float] = {}

                    # can't find documentation on the difference between vCPUsAvailable
                    # and vCPUs. When they are different, vCPUsAvailable seems to match
                    # what appears on the Azure Portal. vCPUsAvailable is not always
                    # available, though, in that case vCPUs seems to be accurate, but
                    # have not checked this exhaustively
                    logical_cpu_str = capabilities.get("vCPUsAvailable")
                    if logical_cpu_str is None:
                        logical_cpu_str = capabilities.get("vCPUs")
                    try:
                        consumable_resources[LOGICAL_CPU] = float(
                            cast(str, logical_cpu_str)
                        )
                    except ValueError:
                        print(
                            f"Warning {r['name']} could not be processed because vCPUs "
                            "is missing or cannot be parsed to a float "
                            f"{logical_cpu_str}"
                        )
                        break

                    memory_gb_str = capabilities.get("MemoryGB")
                    try:
                        consumable_resources[MEMORY_GB] = float(
                            cast(str, memory_gb_str)
                        )
                    except ValueError:
                        print(
                            f"Warning {r['name']} could not be processed because "
                            "MemoryGB is missing or cannot be parsed to a float "
                            f"{memory_gb_str}"
                        )
                        break

                    if "GPUs" in capabilities:
                        try:
                            consumable_resources[GPU] = float(capabilities["GPUs"])
                        except ValueError:
                            print(
                                f"Warning ignoring GPUs available on {r['name']} "
                                "because the field could not be parsed to a float "
                                f"{capabilities['GPUs']}"
                            )

                    if r["name"] in seen_names:
                        print(f"Warning skipping duplicate entry for {r['name']}")
                        break

                    seen_names.add(r["name"])
                    result.append((r["name"], consumable_resources))

    return result


async def _get_vm_prices(
    location: str,
) -> Dict[Tuple[str, OnDemandOrSpotType], Tuple[float, Optional[float]]]:
    """
    Returns (VM SKU, on_demand_or_spot) -> (price per hour, None). The None is there so
    that this can be used as a drop-in replacement for get_cached_vm_prices_and_eviction
    if the cached data is not available (in that function, the None would be the
    eviction rate)
    """
    all_prices: Dict[Tuple[str, OnDemandOrSpotType], Tuple[float, Optional[float]]] = {}

    # https://docs.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices
    _initial_prices_url = (
        "https://prices.azure.com/api/retail/prices?$filter="
        f"serviceName eq 'Virtual Machines' and armRegionName eq '{location}' "
        # other options here are DevTestConsumption for
        # https://azure.microsoft.com/en-us/pricing/dev-test/#overview and Reservation
        # for reserved VMs
        "and priceType eq 'Consumption'"
    )

    async with aiohttp.request("GET", _initial_prices_url) as response:
        response.raise_for_status()
        response_json = await response.json()
        _add_new_price_records(all_prices, response_json["Items"])
        next_page_link = response_json.get("NextPageLink")

    i = 1
    while next_page_link:
        if i % 5 == 0:
            print(f"Getting page {i} of Azure VM prices")
        i += 1
        async with aiohttp.request("GET", next_page_link) as response:
            response.raise_for_status()
            response_json = await response.json()
            _add_new_price_records(all_prices, response_json["Items"])
            next_page_link = response_json.get("NextPageLink")

    return all_prices


def _add_new_price_records(
    all_prices: Dict[Tuple[str, OnDemandOrSpotType], Tuple[float, Optional[float]]],
    new_records: Iterable[Dict[str, Any]],
) -> None:
    """Helper function for _get_vm_prices"""
    for r in new_records:
        # dedicated hosts are not VMs
        if r["productName"].endswith(" Dedicated Host") or r["productName"].endswith(
            " DedicatedHost"
        ):
            continue

        if r["productName"].endswith(" Windows"):
            # TODO eventually we want to support Windows as well
            continue

        if r["currencyCode"] != "USD":
            print(
                f"Warning, skipping price record for {r['armSkuName']} because the "
                f"currencyCode is not USD as expected: {r['currencyCode']}"
            )
            continue
        if r["unitOfMeasure"] != "1 Hour":
            print(
                f"Warning, skipping price record for {r['armSkuName']} because the "
                f"unitOfMeasure is not 1 Hour as expected: {r['unitOfMeasure']}"
            )
            continue

        if r["skuName"].endswith(" Low Priority"):
            # low priority VMs are only available through Azure Batch, basically not
            # relevant
            continue
        elif r["skuName"].endswith(" Spot"):
            on_demand_or_spot: OnDemandOrSpotType = "spot"
        else:
            on_demand_or_spot = "on_demand"

        key = cast(str, r["armSkuName"]), on_demand_or_spot

        if key in all_prices:
            print(f"Warning, got multiple records for {key}")
            continue

        if not isinstance(r["retailPrice"], float) and not isinstance(
            r["retailPrice"], int
        ):
            print(
                f"Warning, skipping price record for {r['armSkuName']} because the "
                f"retailPrice is not a float: {r['retailPrice']}"
            )

        # TODO potentially look at unitPrice as well? According to the docs (link
        # above), retailPrice is "without discount", so presumably unitPrice includes
        # discounts?
        all_prices[key] = r["retailPrice"], None


async def _get_cached_vm_prices_and_eviction(
    location: str,
) -> Dict[Tuple[str, OnDemandOrSpotType], Tuple[float, Optional[float]]]:
    """
    Returns (VM SKU, on_demand_or_spot) -> (price per hour, eviction rate). For eviction
    rate, None means that we do not know the eviction rate, NOT that it is 0. If it is
    0, then eviction rate will be set to 0.

    Tries to first get this data via a pre-cached JSON file stored in an Azure blob (see
    azure_pricing_eviction_cache.py). If that's not available, we'll use the Azure
    pricing APIs which are much slower. We won't have any eviction data unfortunately as
    there is no API.
    """
    async with aiohttp.request(
        "GET",
        (
            "https://meadowrunprod.blob.core.windows.net/azure-pricing/"
            f"{location}-prices-eviction.json"
        ),
    ) as response:
        if response.ok:
            pricing_eviction_data = await response.json()
            return {
                cast(Tuple[str, OnDemandOrSpotType], tuple(key)): value
                for key, value in pricing_eviction_data
            }

    print(
        f"Warning precompiled pricing and eviction rate data for {location} is not "
        "available. Please contact support@meadowdata.io to request an update. We "
        "will proceed with live pricing data (which is slow), and no eviction rate "
        "data (which is unavailable via API at this time)."
    )
    return await _get_vm_prices(location)


_DEFAULT_EVICTION_RATE = 60.0

# A bit of a hack to just hard-code these
_EXPECTED_MISSING_PRICES = {
    # These are burst instances, so they don't have spot versions
    ("Standard_B12ms", "spot"),
    ("Standard_B16ms", "spot"),
    ("Standard_B1ls", "spot"),
    ("Standard_B1ms", "spot"),
    ("Standard_B1s", "spot"),
    ("Standard_B20ms", "spot"),
    ("Standard_B2ms", "spot"),
    ("Standard_B2s", "spot"),
    ("Standard_B4ms", "spot"),
    ("Standard_B8ms", "spot"),
    # Not sure why these "Promo" SKUs don't have spot pricing
    ("Standard_DS11_v2_Promo", "spot"),
    ("Standard_DS12_v2_Promo", "spot"),
    ("Standard_DS13_v2_Promo", "spot"),
    ("Standard_DS14_v2_Promo", "spot"),
    ("Standard_DS2_v2_Promo", "spot"),
    ("Standard_DS3_v2_Promo", "spot"),
    ("Standard_DS4_v2_Promo", "spot"),
    ("Standard_DS5_v2_Promo", "spot"),
    # These SKUs just don't have any pricing, not sure why
    ("Standard_E96ias_v4", "on_demand"),
    ("Standard_E96ias_v4", "spot"),
    ("Standard_ND40s_v3", "on_demand"),
    ("Standard_ND40s_v3", "spot"),
    ("Standard_NV12s_v2", "on_demand"),
    ("Standard_NV12s_v2", "spot"),
    ("Standard_NV24s_v2", "on_demand"),
    ("Standard_NV24s_v2", "spot"),
    ("Standard_NV6s_v2", "on_demand"),
    ("Standard_NV6s_v2", "spot"),
}


async def get_vm_types(location: str) -> List[CloudInstanceType]:
    # TODO we should also take quotas into account, the default quotas will stop us from
    # creating a lot of VM sizes

    vm_skus = await _get_vm_skus(location)
    pricing_eviction_data = await _get_cached_vm_prices_and_eviction(location)

    results = []

    for name, consumable_resources in vm_skus:
        for on_demand_or_spot in ON_DEMAND_OR_SPOT_VALUES:
            price_eviction = pricing_eviction_data.get((name, on_demand_or_spot))
            if price_eviction is None:
                if (name, on_demand_or_spot) not in _EXPECTED_MISSING_PRICES:
                    print(
                        f"Warning skipping {name}, {on_demand_or_spot} because no "
                        "pricing/eviction rate data is available"
                    )
            else:
                price, eviction_rate = price_eviction
                if eviction_rate is None:
                    eviction_rate = _DEFAULT_EVICTION_RATE
                results.append(
                    CloudInstanceType(
                        name,
                        on_demand_or_spot,
                        price,
                        ResourcesInternal(
                            consumable_resources,
                            {EVICTION_RATE_INVERSE: 100 - eviction_rate},
                        ),
                    )
                )

    return results
