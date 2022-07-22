"""
It's a pain to call this API manually and there's no GUI for it, so we have a script
here to aid with manual testing of Azure spot evictions.
"""

import argparse
import asyncio

from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_resource_group,
    get_default_location,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("vm_name")
    args = parser.parse_args()

    vm_name = args.vm_name

    resource_group_path = await ensure_meadowrun_resource_group(get_default_location())

    await azure_rest_api(
        "POST",
        f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines/{vm_name}/"
        "simulateEviction",
        "2022-03-01",
    )


if __name__ == "__main__":
    asyncio.run(main())
