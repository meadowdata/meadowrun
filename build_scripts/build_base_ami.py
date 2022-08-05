import argparse
import asyncio
import re

import asyncssh

from build_ami_helper import (
    parse_ubuntu_version,
    parse_docker_version,
    parse_python_version,
    build_amis,
    _check_for_existing_amis,
    BEHAVIOR_OPTIONS,
    _assert_str,
)
from meadowrun.ssh import run_and_print, run_and_capture

BASE_AMIS = {
    # generated via get_amis_for_regions. Got the initial AMI id in a single region by
    # using the console and looking for "Ubuntu Server 20.04 LTS (HVM), SSD Volume Type"
    # under quickstart AMIs
    "plain": {
        "us-east-2": "ami-0960ab670c8bb45f3",
        "us-east-1": "ami-08d4ac5b634553e16",
        "us-west-1": "ami-01154c8b2e9a14885",
        "us-west-2": "ami-0ddf424f81ddb0720",
        "eu-central-1": "ami-0c9354388bb36c088",
        "eu-west-1": "ami-0d2a4a5d69e46ea0b",
        "eu-west-2": "ami-0bd2099338bc55e6d",
        "eu-west-3": "ami-0f7559f51d3a22167",
        "eu-north-1": "ami-012ae45a4a2d92750",
    },
    # generated via get_amis_for_regions. AMI name is AWS Deep Learning Base AMI GPU
    # CUDA 11 (Ubuntu 20.04) 20220627
    "cuda": {
        "us-east-2": "ami-01e2c6319392b1b40",
        "us-east-1": "ami-0dbb3b4ca1bc58863",
        "us-west-1": "ami-0ee8a8c2d207b8dd4",
        "us-west-2": "ami-0acebbaf658eb64c1",
        "eu-central-1": "ami-051729bad190218f0",
        "eu-west-1": "ami-0152318739453cfc2",
        "eu-west-2": "ami-0396e3201ddcd75ee",
        "eu-west-3": "ami-0ec876e2bab7ac024",
        "eu-north-1": "ami-00fd15de5ac9fe409",
    },
}


async def prepare_meadowrun_virtual_env(
    connection: asyncssh.SSHClientConnection, python: str
) -> None:
    await run_and_print(
        connection,
        "sudo mkdir /var/meadowrun "
        "&& sudo chown ubuntu:ubuntu /var/meadowrun "
        "&& mkdir /var/meadowrun/env "
        f"&& {python} -m venv /var/meadowrun/env "
        "&& source /var/meadowrun/env/bin/activate "
        "&& pip install wheel",
    )


async def plain_base_image_actions_on_vm(
    connection: asyncssh.SSHClientConnection,
) -> str:

    # https://docs.docker.com/engine/install/ubuntu/)
    print("install Docker")
    await run_and_print(
        connection,
        "sudo apt-get update "
        "&& sudo apt-get install -y ca-certificates curl gnupg lsb-release",
    )
    await run_and_print(
        connection,
        "curl -fsSL https://download.docker.com/linux/ubuntu/gpg "
        "| sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg",
    )
    await run_and_print(
        connection,
        'echo "deb [arch=$(dpkg --print-architecture) '
        "signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] "
        'https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" '
        "| sudo tee /etc/apt/sources.list.d/docker.list > /dev/null",
    )
    await run_and_print(
        connection,
        "sudo apt-get update "
        "&& sudo apt-get install -y docker-ce docker-ce-cli containerd.io",
    )

    # https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket)
    print("Add the current user (ubuntu) to the docker group")
    await run_and_print(connection, "sudo usermod -aG docker ${USER}")

    # TODO docker docs recommend
    # https://docs.docker.com/config/containers/logging/configure/ setting "log-driver":
    # "local" in docker config daemon.json

    print("Install python")
    await run_and_print(connection, "sudo apt install -y python3.9 python3.9-venv")

    print("Delete apt cache")
    await run_and_print(connection, "sudo rm -rf /var/cache/apt")

    print("Prepare a virtualenv for Meadowrun")
    await prepare_meadowrun_virtual_env(connection, "python3.9")

    return (
        f"ubuntu{await parse_ubuntu_version(connection)}"
        f"-docker{await parse_docker_version(connection)}"
        f"-python{await parse_python_version(connection, 'python3.9')}"
    )


async def cuda_base_image_actions_on_vm(
    connection: asyncssh.SSHClientConnection,
) -> str:
    # https://developer.nvidia.com/cuda-downloads?target_os=Linux&target_arch=x86_64&Distribution=Ubuntu&target_version=20.04&target_type=deb_network
    print("Upgrade cuda")
    await run_and_print(
        connection,
        "sudo apt-get update "
        "&& wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin "  # noqa: E501
        "&& sudo mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600 "
        "&& sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/3bf863cc.pub "  # noqa: E501
        '&& sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ /" '  # noqa: E501
        "&& sudo apt-get update "
        "&& sudo apt-get -y install cuda "
        "&& sudo apt-get -y install libcudnn8 libcudnn8-dev ",
    )

    print("Uninstall older versions of cuda")
    # (just leaving cuda-11.7). These take up a ton of disk space
    await run_and_print(
        connection,
        "sudo rm -rf /usr/local/cuda-11.0 "
        "&& sudo rm -rf /usr/local/cuda-11.1 "
        "&& sudo rm -rf /usr/local/cuda-11.2 "
        "&& sudo rm -rf /usr/local/cuda-11.3 "
        "&& sudo rm -rf /usr/local/cuda-11.4 "
        "&& sudo rm -rf /usr/local/cuda-11.5 "
        "&& sudo rm -rf /usr/local/cuda-11.6 ",
    )

    print("Install venv")
    await run_and_print(connection, "sudo apt-get install -y python3.8-venv")

    print("Delete apt cache")
    await run_and_print(connection, "sudo rm -rf /var/cache/apt")

    print("Prepare a virtualenv for Meadowrun")
    await prepare_meadowrun_virtual_env(connection, "python3.8")

    return (
        f"cuda{await parse_cuda_version(connection)}"
        f"-ubuntu{await parse_ubuntu_version(connection)}"
        f"-python{await parse_python_version(connection, 'python3.8')}"
    )


async def parse_cuda_version(connection: asyncssh.SSHClientConnection) -> str:
    nvcc_version = _assert_str(
        (await run_and_capture(connection, "nvcc --version")).stdout
    ).strip()

    match = re.match(
        r"Cuda compilation tools, release (?P<version_string>[\d.]+),",
        nvcc_version.split("\n")[3],
    )
    if match is None:
        raise ValueError(f"Could not parse nvcc version string: {nvcc_version}")
    return match.group("version_string")


def main():
    r"""
    A script for creating a meadowrun EC2 base AMIs. The only reason to want to change
    the base AMI is because you want to change the version or flavour of
    Linux/Docker/Python we're using, or some other deep baked-into-the-OS thing. To
    update meadowrun, you can just run build_ami.py

    plain images are the "regular" base images. cuda images have cuda installed.

    Usage:

    Build the plain base image in one region, and get the name from the output. The name
    is based on the versions of software that get installed, so it will change as e.g.
    new versions of python get released:
        python build_scripts\build_base_ami.py plain us-east-2

    Test the version that's created in one region, and then re-run in all regions
        python build_scripts\build_base_ami.py plain all --expected-name
        ubuntu20.04.4-docker20.10.17-python3.9.5

    This will leave existing images alone. To regenerate images (i.e. you've made
    changes):
        python build_scripts\build_base_ami.py plain all --expected-name
        ubuntu20.04.4-docker20.10.17-python3.9.5 --on-existing-image delete

    Alternatively, you could use manage_amis.py to explicitly delete all AMIs across all
    regions where the name starts with a particular prefix:
        python build_scripts\manage_amis.py delete
        ubuntu20.04.4-docker20.10.17-python3.9.5

    Replace "plain" with "cuda" to build the cuda images. You can specify more than one
    region at a time if you want, e.g. us-east-1,us-east-2.

    Warning, if --expected-name does not match the actual name produced by the script,
    you will get unexpected behavior.

    Copy the output of this script to the appropriate part of BASE_AMIS in build_ami.py.
    You can always run
        python build_scripts\build_base_ami.py plain all --expected-name
        ubuntu20.04.4-docker20.10.17-python3.9.5
    and if the images have already been created, this will just print out what you need
    to copy into BASE_AMIS.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("type", choices=["plain", "cuda"])
    parser.add_argument("regions", type=str)
    parser.add_argument("--expected-name", type=str)
    parser.add_argument(
        "--on-existing-image", type=str, choices=BEHAVIOR_OPTIONS, default="leave"
    )
    args = parser.parse_args()

    all_region_base_amis = BASE_AMIS[args.type]

    if args.type == "plain":
        volume_size_gb = 16
        actions_on_vm = plain_base_image_actions_on_vm
    elif args.type == "cuda":
        volume_size_gb = 100
        actions_on_vm = cuda_base_image_actions_on_vm
    else:
        raise ValueError(f"Unexpected type {args.type}")

    if args.regions == "all":
        regions = list(all_region_base_amis.keys())
    else:
        regions = args.regions.split(",")

    existing_images = None
    if args.expected_name:
        ignore_regions, existing_images = _check_for_existing_amis(
            regions, args.expected_name, args.on_existing_image
        )
        regions = [region for region in regions if region not in ignore_regions]

        if existing_images:
            print("Existing images:\n" + existing_images)

        if not regions:
            return

    print(f"Creating {args.type} base AMIs")
    asyncio.run(
        build_amis(regions, all_region_base_amis, volume_size_gb, actions_on_vm)
    )
    print(f"Created {args.type} base AMIs")

    if existing_images:
        print("Existing images:\n" + existing_images)


if __name__ == "__main__":
    main()
