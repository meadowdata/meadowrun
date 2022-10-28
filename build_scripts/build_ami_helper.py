from __future__ import annotations

import asyncio
import re
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
)
from typing_extensions import Literal, get_args

import asyncssh
import boto3

from meadowrun.aws_integration.ec2 import (
    LaunchEC2InstanceSettings,
    LaunchEC2InstanceSuccess,
    _MEADOWRUN_SSH_SECURITY_GROUP,
    authorize_current_ip_for_meadowrun_ssh,
    ensure_security_group,
    launch_ec2_instance,
)
from meadowrun.aws_integration.ec2_instance_allocation import SSH_USER
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
    ensure_meadowrun_key_pair,
)
from meadowrun.run_job_core import _retry
from meadowrun.ssh import connect, run_and_capture

if TYPE_CHECKING:
    from mypy_boto3_ec2 import EC2Client
    from mypy_boto3_ec2.type_defs import ImageTypeDef

REGION_TO_INSTANCE_TYPE = {
    "us-east-2": "t2.micro",
    "us-east-1": "t2.micro",
    "us-west-1": "t2.micro",
    "us-west-2": "t2.micro",
    "eu-central-1": "t2.micro",
    "eu-west-1": "t2.micro",
    "eu-west-2": "t2.micro",
    "eu-west-3": "t2.micro",
    "eu-north-1": "t3.micro",
}


def get_name_from_ami(region: str, image_id: str) -> str:
    """
    E.g. get_name_from_ami("us-east-1", "ami-08d4ac5b634553e16") ->
    "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20220610"
    """
    return boto3.client("ec2", region_name=region).describe_images(
        Filters=[{"Name": "image-id", "Values": [image_id]}]
    )["Images"][0]["Name"]


def get_amis_for_regions(image_name: str) -> Dict[str, str]:
    """Gets the AMI ids for the given image name in every region"""
    bad_regions = []
    results = {}

    for region in REGION_TO_INSTANCE_TYPE.keys():
        images = boto3.client("ec2", region_name=region).describe_images(
            Filters=[{"Name": "name", "Values": [image_name]}]
        )["Images"]
        if len(images) == 0:
            print(f"Image with name {image_name} not found in region {region}")
            bad_regions.append(region)
        else:
            results[region] = images[0]["ImageId"]
            if len(images) > 1:
                print(
                    f"Warning: more than one image with name {image_name} found in "
                    f"region {region}"
                )
                print(images)

    if bad_regions:
        raise ValueError(
            f"Error finding image {image_name} in region(s): " + ", ".join(bad_regions)
        )

    return results


async def _build_ami(
    base_ami_id: str,
    region_name: str,
    volume_size_gb: int,
    actions_on_vm: Callable[[asyncssh.SSHClientConnection], Awaitable[str]],
) -> Tuple[bool, str, str, Optional[str]]:
    """
    Builds an AMI

    plain_base_image_actions_on_vm must return the ami name

    You must call _wait_for_image_and_make_public(region_name, image_id) after this
    function, and then terminate the instance_id

    Returns success, region_name (same as function argument), image id if success,
    otherwise exception traceback, instance_id
    """
    instance_id = None

    try:
        client = boto3.client("ec2", region_name=region_name)

        # launch an EC2 instance that we'll use to create the AMI
        print("Launching EC2 instance:")
        # we use the "default region name" as the SSH key will only exist in a region
        # where we've installed Meadowrun
        ensure_meadowrun_key_pair(region_name)
        pkey = get_meadowrun_ssh_key(region_name)
        launch_result = await launch_ec2_instance(
            region_name,
            REGION_TO_INSTANCE_TYPE[region_name],
            "on_demand",
            LaunchEC2InstanceSettings(
                ami_id=base_ami_id,
                security_group_ids=[
                    ensure_security_group(_MEADOWRUN_SSH_SECURITY_GROUP, region_name)
                ],
            ),
            key_name=MEADOWRUN_KEY_PAIR_NAME,
            volume_size_gb=volume_size_gb,
        )
        if not isinstance(launch_result, LaunchEC2InstanceSuccess):
            raise ValueError(f"Failed to launch EC2 instance: {launch_result}")
        public_address = await launch_result.public_address_continuation
        assert public_address is not None
        print(f"Launched EC2 instance {public_address}")

        # get the instance id of our EC2 instance
        instances = client.describe_instances(
            Filters=[{"Name": "dns-name", "Values": [public_address]}]
        )
        instance_id = instances["Reservations"][0]["Instances"][0]["InstanceId"]

        await authorize_current_ip_for_meadowrun_ssh(region_name)

        connection = await _retry(
            lambda: connect(
                public_address,
                username=SSH_USER,
                private_key=pkey,
            ),
            (TimeoutError, ConnectionRefusedError, OSError),
            max_num_attempts=20,
        )

        async with connection:
            new_ami_name = await actions_on_vm(connection)

        print(f"Using AMI name: {new_ami_name}")

        # create an image, and wait for it to become available
        result = client.create_image(InstanceId=instance_id, Name=new_ami_name)

        return True, region_name, result["ImageId"], instance_id
    except Exception as e:
        return (
            False,
            region_name,
            "".join(traceback.format_exception(type(e), e, e.__traceback__)),
            instance_id,
        )


async def _wait_for_image_and_make_public(region_name: str, image_id: str) -> None:
    client = boto3.client("ec2", region_name=region_name)
    while True:
        images = client.describe_images(ImageIds=[image_id])
        if images["Images"][0]["State"] != "pending":
            break
        else:
            print(f"Image is pending in {region_name}, sleeping for 30s...")
            await asyncio.sleep(30)
    # make it public
    client.modify_image_attribute(
        ImageId=image_id, LaunchPermission={"Add": [{"Group": "all"}]}
    )


async def build_amis(
    regions: List[str],
    all_region_base_amis: Dict[str, str],
    volume_size_gb: int,
    actions_on_vm: Callable[[asyncssh.SSHClientConnection], Awaitable[str]],
) -> None:
    """Builds the same AMI in every region specified by regions"""
    region_to_base_amis = [(region, all_region_base_amis[region]) for region in regions]

    print(
        "Will build image in " + ", ".join(region for region, _ in region_to_base_amis)
    )

    results = await asyncio.gather(
        *(
            _build_ami(base_ami, region, volume_size_gb, actions_on_vm)
            for region, base_ami in region_to_base_amis
        )
    )

    try:
        exceptions = [
            (region, image_id_or_exception)
            for success, region, image_id_or_exception, _ in results
            if not success
        ]
        if exceptions:
            print("There were exceptions in " + ", ".join(r for r, _ in exceptions))
            for region, exception in exceptions:
                print(f"{region}: {exception}")

        wait_tasks = []

        print("Initial image creation done:")
        for success, region, image_id, _ in results:
            if success:
                print(f'"{region}": "{image_id}",')
                wait_tasks.append(_wait_for_image_and_make_public(region, image_id))

        print(
            "Now will wait for image creation to complete, and then make images public"
        )
        await asyncio.gather(*wait_tasks)

        print("Done!")
        for success, region, image_id, _ in results:
            if success:
                print(f'"{region}": "{image_id}",')
    finally:
        for _, region, _, instance_id in results:
            if instance_id is not None:
                # now terminate the instance as we don't need it anymore
                print(f"Terminating {instance_id} in {region}")
                boto3.client("ec2", region_name=region).terminate_instances(
                    InstanceIds=[instance_id]
                )


def _assert_str(s: Any) -> str:
    """Helper function just for mypy"""
    if not isinstance(s, str):
        raise ValueError(f"Expected a string but got {type(s)}")
    return s


async def parse_ubuntu_version(connection: asyncssh.SSHClientConnection) -> str:
    ubuntu_version = _assert_str(
        (await run_and_capture(connection, "lsb_release -a")).stdout
    ).strip()

    match = re.match(
        r"Description:\s+Ubuntu (?P<version_string>[\d.]+)( LTS)?",
        ubuntu_version.split("\n")[1],
    )
    if match is None:
        raise ValueError(f"Could not parse Ubuntu version string: {ubuntu_version}")
    return match.group("version_string")


async def parse_docker_version(connection: asyncssh.SSHClientConnection) -> str:
    docker_version = _assert_str(
        (await run_and_capture(connection, "docker --version")).stdout
    ).strip()

    match = re.match(r"Docker version (?P<version_string>[\d.]+),", docker_version)
    if match is None:
        raise ValueError(f"Could not parse Docker version string: {docker_version}")
    return match.group("version_string")


async def parse_python_version(
    connection: asyncssh.SSHClientConnection, python: str
) -> str:
    python_version = _assert_str(
        (await run_and_capture(connection, f"{python} --version")).stdout
    ).strip()

    match = re.match(r"Python (?P<version_string>[\d.]+)", python_version)
    if match is None:
        raise ValueError(f"Could not parse Python version string: {python_version}")
    return match.group("version_string")


BEHAVIOR_OPTIONS_TYPE = Literal["raise", "delete", "leave"]
BEHAVIOR_OPTIONS = get_args(BEHAVIOR_OPTIONS_TYPE)


def _check_for_existing_amis(
    regions: Iterable[str], ami_name: str, behavior: BEHAVIOR_OPTIONS_TYPE
) -> Tuple[Set[str], str]:
    """
    Checks for existing AMIs with the specified ami_name in the specified regions.

    If behavior is raise, raises an exception. If behavior is delete, deletes the
    existing AMIs. If behavior is leave, returns (regions where AMI already exists, a
    nicely formatted string of the regions/AMI ids)
    """
    # contains (region, client, existing_image)
    regions_with_existing_images: List[Tuple[str, EC2Client, ImageTypeDef]] = []
    for region in regions:
        client = boto3.client("ec2", region_name=region)
        existing_images = client.describe_images(
            Filters=[{"Name": "name", "Values": [ami_name]}]
        )["Images"]
        if existing_images:
            regions_with_existing_images.extend(
                (region, client, existing_image) for existing_image in existing_images
            )

    if regions_with_existing_images:
        print(
            f"There are already images with the name {ami_name} in "
            + ", ".join(r for r, _, _ in regions_with_existing_images)
        )

        if behavior == "raise":
            raise ValueError(
                "Exiting because there are existing images with the expected name"
            )
        elif behavior == "delete":
            print("Deleting these images")
            for region, client, existing_image in regions_with_existing_images:
                image_id = existing_image["ImageId"]
                print(f"Deleted {image_id} in {region}")
                client.deregister_image(ImageId=image_id)

                for block_device_mapping in existing_image["BlockDeviceMappings"]:
                    if "Ebs" in block_device_mapping:
                        snapshot_id = block_device_mapping["Ebs"].get(
                            "SnapshotId", None
                        )
                        if snapshot_id is None:
                            print("Warning snapshot id is None")
                        else:
                            print(f"Deleting related snapshot: {snapshot_id}")
                            client.delete_snapshot(SnapshotId=snapshot_id)
            return set(), ""
        elif behavior == "leave":
            print("Will not create new images in these regions")
            return {region for region, _, _ in regions_with_existing_images}, "\n".join(
                f"\"{region}\": \"{existing_image['ImageId']}\","
                for region, _, existing_image in regions_with_existing_images
            )
        else:
            raise ValueError(f"Unexpected value for behavior: {behavior}")

    return set(), ""
