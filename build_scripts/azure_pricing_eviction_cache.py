# Unfortunately there is currently no API for getting eviction rate data for Azure VMs:
# https://docs.microsoft.com/en-us/answers/questions/564280/how-to-get-vm-eviction-rate-through-rest-api-1.html

# This script helps manually collect this data and upload it to publicly available Blob
# Storage that meadowrun will read from.

# Go to portal.azure.com > Virtual Machines > Create > Azure virtual machine > Change
# Region to the desired region > Check Azure Spot Instance > Open the browser
# inspector's network tab > Back in the webpage, click see all sizes > Back in the
# inspector's network tab, search for "getSpotEvictionRatesMemoized" in one of the
# calls to https://management.azure.com/batch?api-version=2020-06-01 > Copy the
# response for that request into <location>-eviction.json
import asyncio
import json
from typing import Tuple, Optional, List, Dict

from meadowrun.azure_integration.azure_vm_pricing import _get_vm_prices
from meadowrun.instance_selection import OnDemandOrSpotType


def _try_parse_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except ValueError:
        return None


def _interpret_eviction_rate(rate: str) -> Optional[float]:
    # first try to interpret strings like 5-10 by taking the average
    a, sep, b = rate.partition("-")
    if sep == "-":
        a_int = _try_parse_int(a)
        b_int = _try_parse_int(b)

        if a_int is not None and b_int is not None:
            return (a_int + b_int) / 2

    # if that didn't work, try to interpret strings like 20+ by interpreting it as
    # 20-100 and taking the average
    if rate.endswith("+"):
        a_optional_int = _try_parse_int(rate[:-1])
        if a_optional_int is not None:
            return (a_optional_int + 100) / 2

    # we don't know how to interpret
    return None


def _parse_json_for_eviction_rates(location: str) -> Dict[str, Optional[float]]:
    with open(f"{location}-eviction.json", "r", encoding="utf-8") as f:
        responses = json.load(f)

    result: Dict[str, Optional[float]] = {}

    for response in responses["responses"]:
        if "data" in response["content"]:
            for record in response["content"]["data"]:
                if record["skuName"] not in result:
                    eviction_rate_number = _interpret_eviction_rate(
                        record["evictionRate"]
                    )
                    if eviction_rate_number is None:
                        print(
                            "Warning ignoring eviction rate string that we don't know "
                            f"how to interpret {record['evictionRate']}"
                        )
                    else:
                        result[record["skuName"].lower()] = eviction_rate_number
                else:
                    print(f"Warning ignoring duplicate record for {record['skuName']}")

    return result


async def create_prices_eviction_json(location: str) -> None:
    eviction_rates = _parse_json_for_eviction_rates(location)
    result: List[
        Tuple[Tuple[str, OnDemandOrSpotType], Tuple[float, Optional[float]]]
    ] = []
    num_spot_rates_found = 0
    total_instance_types = 0
    for (name, on_demand_or_spot), price in (await _get_vm_prices(location)).items():
        if on_demand_or_spot == "on_demand":
            result.append(((name, on_demand_or_spot), (price[0], 0)))
        elif on_demand_or_spot == "spot":
            if name.lower() in eviction_rates:
                num_spot_rates_found += 1
            result.append(
                (
                    (name, on_demand_or_spot),
                    (price[0], eviction_rates.get(name.lower())),
                )
            )
        else:
            raise ValueError(
                f"Unexpected value for on_demand_or_spot {on_demand_or_spot}"
            )

        total_instance_types += 1

    with open(f"{location}-prices-eviction.json", "w", encoding="utf-8") as f:
        json.dump(result, f)

    print(
        f"Wrote {location}-prices.eviction.json. "
        f"{num_spot_rates_found}/{total_instance_types} spot rates found"
    )

    # TODO do this via API
    print(
        "Please update the resulting file to Azure: with `az storage blob upload "
        "--account-name meadowrunprod --container-name azure-pricing --name "
        f"{location}-prices-eviction.json --file {location}-prices-eviction.json "
        "--auth-mode login --overwrite --content-type application/json`"
    )


def main() -> None:
    asyncio.run(create_prices_eviction_json("eastus"))


if __name__ == "__main__":
    main()
