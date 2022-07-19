# flake8: noqa

import asyncio
import os.path
import subprocess

from meadowrun.azure_integration.azure_meadowrun_core import get_default_location
from meadowrun.azure_integration.azure_ssh_keys import ensure_meadowrun_key_pair
from meadowrun.azure_integration.azure_vms import _provision_vm
from meadowrun.ssh import connect

from build_image_shared import upload_and_configure_meadowrun

_BASE_IMAGE = (
    "/subscriptions/d740513f-4172-4792-bd29-5194e79d5881/resourceGroups/meadowrun-dev/"
    "providers/Microsoft.Compute/galleries/meadowrun.dev.gallery/images/"
    "ubuntu-20.04.4-docker-20.10.15-python-3.9.5"
)


async def build_meadowrun_azure_image():
    # get version

    # this only works if we're running in the directory with pyproject.toml
    package_root_dir = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.run(
        ["poetry", "version", "--short"], capture_output=True, cwd=package_root_dir
    )
    version = result.stdout.strip().decode("utf-8")

    # build a package locally
    subprocess.run(["poetry", "build"], cwd=package_root_dir)

    # launch a VM instance that we'll use to create the AMI
    print("Launching VM:")

    location = get_default_location()
    private_key, public_key = await ensure_meadowrun_key_pair(location)
    ip_address, vm_name = await _provision_vm(
        location, "Standard_DS4_v2", "spot", public_key, _BASE_IMAGE, "Regular"
    )
    print(f"Launched VM {ip_address}")

    async with connect(
        ip_address,
        username="meadowrunuser",
        private_key=private_key,
    ) as connection:
        await upload_and_configure_meadowrun(
            connection, version, package_root_dir, "AzureVM"
        )

    # TODO make this automatic, also replicate the image and delete the VM when we're
    # done. Current gallery is meadowrunprodeastus
    print(f"VM {vm_name} is ready to be captured into an image")


if __name__ == "__main__":
    asyncio.run(build_meadowrun_azure_image())
