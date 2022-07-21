from __future__ import annotations

import argparse
import asyncio
import asyncio.subprocess
import logging
from typing import TYPE_CHECKING

from meadowrun.aws_integration.aws_core import (
    _get_default_region_name,
    _get_ec2_metadata,
)
from meadowrun.aws_integration.ec2_instance_allocation import EC2InstanceRegistrar

if TYPE_CHECKING:
    from meadowrun.instance_allocation import InstanceRegistrar
    from meadowrun.run_job_core import CloudProviderType
from meadowrun.run_job_core import CloudProvider


async def async_main(cloud: CloudProviderType, cloud_region_name: str) -> None:
    """
    Checks to see if we're running on a spot instance that's scheduled for
    interruption. If so, updates the InstanceRegistrar to prevent new jobs from being
    scheduled on this instance
    """
    if cloud == "EC2":
        if (await _get_ec2_metadata("spot/instance-action")) is None:
            # this means we're not a spot instance that's being interrupted right now
            return

        public_address = await _get_ec2_metadata("public-hostname")
        if cloud_region_name == "default":
            cloud_region_name = await _get_default_region_name()
        instance_registrar: InstanceRegistrar = EC2InstanceRegistrar(
            cloud_region_name, "raise"
        )
    elif cloud == "AzureVM":
        # TODO implement for Azure
        return
    else:
        raise ValueError(f"Unexpected value for cloud_provider: {cloud}")

    async with instance_registrar:
        if not public_address:
            raise ValueError(
                "Cannot register spot instance interruption because we can't get the "
                f"public address of the current {cloud} instance (maybe we're not "
                f"running on a {cloud} instance?)"
            )

        print(
            "We are on a spot instance that is being terminated, we will stop "
            "allocating new jobs to this instance"
        )
        while not await instance_registrar.set_prevent_further_allocation(
            public_address, True
        ):
            # Just keep trying to set set_prevent_further_allocation if it fails. Our
            # instance will get shutdown soon anyways
            await asyncio.sleep(1)


def main(cloud: CloudProviderType, cloud_region_name: str) -> None:
    asyncio.run(async_main(cloud, cloud_region_name))


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--cloud", required=True, choices=CloudProvider)
    parser.add_argument("--cloud-region-name", required=True)
    args = parser.parse_args()

    main(args.cloud, args.cloud_region_name)


if __name__ == "__main__":
    command_line_main()
