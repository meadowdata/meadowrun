import argparse
import asyncio
import functools
import os.path
import subprocess
from typing import List, Optional, Any

import asyncssh

from ami_listings import BASE_AMIS, AMI_SIZES_GB
from build_ami_helper import (
    BEHAVIOR_OPTIONS,
    BEHAVIOR_OPTIONS_TYPE,
    _check_for_existing_amis,
    build_amis,
    get_name_from_ami,
)
from build_image_shared import upload_and_configure_meadowrun
import meadowrun.aws_integration.machine_agent
from meadowrun.ssh import write_text_to_file, run_and_print, run_and_capture_str


async def _configure_meadowrun_ec2(
    connection: asyncssh.SSHClientConnection, **kwargs: Any
) -> str:
    # TODO move into the base image!
    await run_and_print(connection, "sudo apt remove -y unattended-upgrades")
    await run_and_print(connection, "sudo apt update && sudo apt install -y nginx nscd")
    await run_and_print(connection, "sudo rm -rf /var/cache/apt")

    image_name = await upload_and_configure_meadowrun(connection, **kwargs)

    home_dir = await run_and_capture_str(connection, "echo $HOME")
    systemd_config_dir = "/etc/systemd/system/"

    # set up the nginx proxy for IMDS
    await write_text_to_file(
        connection,
        "events {}\n"
        "http {\n"
        "   proxy_cache_path /data/nginx/cache keys_zone=mycache:10m;\n"
        "   server {\n"
        "       listen 81;\n"
        "       proxy_cache mycache;\n"
        "       location / {\n"
        "           allow 127.0.0.1;\n"
        "           proxy_pass http://169.254.169.254;\n"
        "           proxy_cache_valid any 10s;\n"
        "       }\n"
        "   }\n"
        "}\n",
        f"{home_dir}/nginx.conf",
    )
    await run_and_print(
        connection,
        f"sudo mv {home_dir}/nginx.conf /etc/nginx/nginx.conf",
    )
    await run_and_print(
        connection,
        "sudo mkdir -p /data/nginx/cache",
    )

    # set up the machine agent
    machine_agent_module = meadowrun.aws_integration.machine_agent.__name__

    user = await run_and_capture_str(connection, "whoami")
    await write_text_to_file(
        connection,
        "[Unit]\n"
        "Description=The machine agent for Meadowrun\n"
        "StartLimitIntervalSec=0\n"
        "[Service]\n"
        f"User={user}\n"
        f"Group={user}\n"
        f"ExecStart=/var/meadowrun/env/bin/python -m {machine_agent_module}\n"
        "StandardOutput=append:/var/meadowrun/machine_agent.log\n"
        "StandardError=append:/var/meadowrun/machine_agent.log\n"
        f"Environment=HOME={home_dir}\n"
        "Environment=PYTHONUNBUFFERED=1\n"
        "Environment=AWS_EC2_METADATA_SERVICE_ENDPOINT=http://localhost:81\n"
        "Restart=always\n"
        "RestartSec=2\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n",
        f"{home_dir}/meadowrun-machine-agent.service",
    )
    await run_and_print(
        connection,
        f"sudo mv {home_dir}/meadowrun-machine-agent.service {systemd_config_dir}",
    )
    await run_and_print(
        connection,
        "sudo systemctl enable --now meadowrun-machine-agent.service",
    )

    return image_name


async def build_meadowrun_ami(
    regions: Optional[List[str]],
    ami_type: str,
    behavior: BEHAVIOR_OPTIONS_TYPE,
    prefix: str,
) -> None:
    all_region_base_amis = BASE_AMIS[ami_type]

    # get version

    # this only works if we're running in the directory with pyproject.toml
    package_root_dir = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.run(
        ["poetry", "version", "--short"], capture_output=True, cwd=package_root_dir
    )
    version = result.stdout.strip().decode("utf-8")
    # we assume all of the base amis in the different regions have the same name, so we
    # just take the first one
    base_ami_name = get_name_from_ami(*list(all_region_base_amis.items())[0])
    new_ami_name = f"{prefix}meadowrun{version}-{base_ami_name}"
    print(f"New AMI ({ami_type}) name is: {new_ami_name}")

    if regions is None:
        regions = list(all_region_base_amis.keys())
    ignore_regions, existing_images = _check_for_existing_amis(
        regions, new_ami_name, behavior
    )
    regions = [region for region in regions if region not in ignore_regions]

    if existing_images:
        print("Existing images:\n" + existing_images)

    if not regions:
        return

    # build a package locally
    subprocess.run(["poetry", "build"], cwd=package_root_dir)

    await build_amis(
        regions,
        all_region_base_amis,
        AMI_SIZES_GB[ami_type],
        functools.partial(
            _configure_meadowrun_ec2,
            version=version,
            package_root_dir=package_root_dir,
            cloud_provider="EC2",
            image_name=new_ami_name,
        ),
    )

    if existing_images:
        print("Existing images:\n" + existing_images)


def main() -> None:
    r"""
    Creates meadowrun AMIs. Usage:

    Build the plain image in one region:
        python build_scripts\build_ami.py plain us-east-2

    Test the version that's created in one region by copying just the one AMI id into
    _AMIS in ec2_instance_allocation.py, and then re-run in all regions
        python build_scripts\build_ami.py plain all

    This will leave existing images alone. To regenerate images (i.e. you've made
    changes):
        python build_scripts\build_ami.py plain all --on-existing-image delete

    Alternatively, you could use manage_amis.py to explicitly delete all AMIs across all
    regions where the name starts with a particular prefix:
        python build_scripts\manage_amis.py delete
        meadowrun0.1.15a1-ubuntu20.04.4-docker20.10.17-python3.9.5

    Replace "plain" with "cuda" to build the cuda images. You can specify more than one
    region at a time if you want, e.g. us-east-1,us-east-2.

    Copy the output of this script to the appropriate part of _AMIS in
    ec2_instance_allocation.py. You can always run
        python build_scripts\build_ami.py plain all
    and if the images have already been created, this will just print out what you need
    to copy into _AMIS.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("type", choices=["plain", "cuda"])
    parser.add_argument("regions", type=str)
    parser.add_argument("--prefix", type=str, default="")
    parser.add_argument(
        "--on-existing-image", type=str, choices=BEHAVIOR_OPTIONS, default="leave"
    )
    args = parser.parse_args()

    if args.regions == "all":
        regions = None
    else:
        regions = args.regions.split(",")

    print(f"Creating {args.type} AMIs")
    asyncio.run(
        build_meadowrun_ami(regions, args.type, args.on_existing_image, args.prefix)
    )
    print(f"Created {args.type} AMIs")


if __name__ == "__main__":
    main()
