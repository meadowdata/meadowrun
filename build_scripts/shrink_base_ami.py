import argparse
import asyncio
import json
import math
import re
import time
from typing import Dict, Any, List, Callable, Awaitable, Union, Type, Tuple, TypeVar

import asyncssh
import boto3

from ami_listings import BASE_AMIS, VANILLA_UBUNTU_AMIS
from meadowrun import ssh
from meadowrun.aws_integration.ec2 import get_ssh_security_group_id
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
)


_T = TypeVar("_T")


def _retry(
    loop: asyncio.AbstractEventLoop,
    function: Callable[[], Awaitable[_T]],
    exception_types: Union[Type, Tuple[Type, ...]],
    max_num_attempts: int = 5,
    delay_seconds: float = 1,
    message: str = "Retrying on error",
) -> _T:
    i = 0
    while True:
        try:
            return loop.run_until_complete(function())
        except exception_types as e:
            i += 1
            if i >= max_num_attempts:
                raise
            else:
                print(f"{message}: {e}")
                time.sleep(delay_seconds)


def _launch_ec2_instance(
    block_device_mappings: List[Dict[str, Any]],
    ec2_resource: Any,
    ami_id: str,
    instance_type: str,
    region_name: str,
) -> Any:
    """Returns an Instance object"""
    optional_args: Dict[str, Any] = {
        # TODO allow users to specify the size of the EBS they need
        "BlockDeviceMappings": block_device_mappings
    }
    optional_args["SecurityGroupIds"] = [get_ssh_security_group_id(region_name)]
    optional_args["KeyName"] = MEADOWRUN_KEY_PAIR_NAME

    optional_args["InstanceMarketOptions"] = {"MarketType": "spot"}

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.run_instances
    instances = ec2_resource.create_instances(
        ImageId=ami_id,
        MinCount=1,
        MaxCount=1,
        InstanceType=instance_type,
        **optional_args,
    )
    instance = instances[0]

    time.sleep(2)
    while not instance.public_dns_name:
        instance.load()
        time.sleep(10)
        print("Waiting for public DNS name to become available")

    print(f"Started instance at {instance.public_dns_name}")
    return instance


def _launch_copier_instance(
    ec2_resource: Any,
    base_ami_id: str,
    instance_type: str,
    region_name: str,
    snapshot_id: str,
    snapshot_size_gb: int,
) -> Any:
    """Returns an instance object"""
    return _launch_ec2_instance(
        [
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": 12,
                    "VolumeType": "gp2",
                },
            },
            {
                "DeviceName": "/dev/sdf",
                "Ebs": {
                    "VolumeSize": snapshot_size_gb,
                    "VolumeType": "gp2",
                    "SnapshotId": snapshot_id,
                },
            },
        ],
        ec2_resource,
        base_ami_id,
        instance_type,
        region_name,
    )


def _resize_disk(
    loop: asyncio.AbstractEventLoop,
    connection: asyncssh.SSHClientConnection,
    path_to_disk: str,
) -> int:
    """Returns number of blocks"""
    lsblk_output = loop.run_until_complete(
        ssh.run_and_capture(connection, f"lsblk -f {path_to_disk}")
    )
    assert isinstance(lsblk_output.stdout, str)  # just for mypy
    if "ext4" not in lsblk_output.stdout:
        raise ValueError("Need to add handling for non-ext4 volumes")

    loop.run_until_complete(
        ssh.run_and_print(connection, f"sudo e2fsck -y -f {path_to_disk}", check=False)
    )
    resizing_completed = False
    for i in range(100):
        resize_output = loop.run_until_complete(
            ssh.run_and_capture(connection, f"sudo resize2fs -M -p {path_to_disk}")
        )
        print(resize_output.stdout)
        print(resize_output.stderr)
        assert isinstance(resize_output.stderr, str)  # just for mypy
        if "Nothing to do" in resize_output.stderr:
            resizing_completed = True
            break

    if not resizing_completed:
        print("Warning, resizing never completed")

    assert isinstance(resize_output.stderr, str)
    blocks_match = re.search(
        r"\b(?P<num_blocks>\d+) \(4k\) blocks long", resize_output.stderr
    )
    if blocks_match is None:
        print(resize_output.stderr)
        raise ValueError("Could not parse resize output")

    return int(blocks_match.group("num_blocks"))


def shrink_ami(
    pre_base_plain_ami_id: str,
    ami_to_shrink_id: str,
    copier_instance_type: str,
    region_name: str,
    sdf_maps_to: str,
    sdh_maps_to: str,
    copy_mbr: bool,
) -> str:
    new_volume = None
    instance = None

    try:
        ec2_resource = boto3.resource("ec2", region_name=region_name)

        ami_to_shrink = ec2_resource.Image(ami_to_shrink_id)
        new_ami_name = f"{ami_to_shrink.name}-shrunk"

        if list(
            ec2_resource.images.filter(
                Filters=[{"Name": "name", "Values": [new_ami_name]}]
            )
        ):
            print(
                f"Not shrinking {ami_to_shrink_id} because the -shrink version already "
                f"exists"
            )
            return ami_to_shrink_id

        ebs_disks = [
            disk for disk in ami_to_shrink.block_device_mappings if "Ebs" in disk
        ]
        if len(ebs_disks) != 1:
            raise ValueError(
                f"Don't know how to shrink {ami_to_shrink_id}, it has {len(ebs_disks)} "
                "Ebs disks"
            )
        ebs_disk = ebs_disks[0]["Ebs"]

        instance = _launch_copier_instance(
            ec2_resource,
            pre_base_plain_ami_id,
            copier_instance_type,
            region_name,
            ebs_disk["SnapshotId"],
            ebs_disk["VolumeSize"],
        )
        assert instance is not None  # just for mypy
        public_dns_name = instance.public_dns_name

        loop = asyncio.new_event_loop()

        connection = _retry(
            loop,
            lambda: ssh.connect(
                public_dns_name,
                username="ubuntu",
                private_key=get_meadowrun_ssh_key(region_name),
            ),
            (TimeoutError, ConnectionRefusedError, OSError),
            max_num_attempts=20,
        )

        # We're not following either of these instructions exactly, but they're helpful
        # references
        # https://serverfault.com/questions/332648/decreasing-root-disk-size-of-an-ebs-boot-ami-on-ec2
        # https://ehikioya.com/shrink-amazon-ebs-volumes/

        # A couple sanity checks
        block_size = loop.run_until_complete(
            ssh.run_and_capture(
                connection, f"sudo blockdev --getbsz /dev/{sdf_maps_to}"
            )
        )
        assert isinstance(block_size.stdout, str)
        if block_size.stdout.strip() != "4096":
            raise ValueError(f"Blocksize was not 4K as expected: {block_size.stdout}")
        sector_size = loop.run_until_complete(
            ssh.run_and_capture(connection, f"sudo blockdev --getss /dev/{sdf_maps_to}")
        )
        assert isinstance(sector_size.stdout, str)
        if sector_size.stdout.strip() != "512":
            raise ValueError(
                f"Sector size was not 512 as expected: {sector_size.stdout}"
            )

        # move files around in filesystem in main partition so that we can resize the
        # partition and get the number of actual bytes in the partition
        main_partition_num_blocks = _resize_disk(
            loop, connection, f"/dev/{sdf_maps_to}1"
        )
        main_partition_bytes = main_partition_num_blocks * 4 * 1024
        # add 512MB of buffer, then round up to the nearest MB
        main_partition_mb = math.ceil(
            (main_partition_bytes + 512 * 1024 * 1024) / (1024**2)
        )
        main_partition_num_16mb_chunks = math.ceil(
            main_partition_bytes / (16 * 1024 * 1024)
        )

        # now actually resize the main partition to the smaller size
        loop.run_until_complete(
            ssh.run_and_print(
                connection,
                f'echo ",{main_partition_mb}MiB" | sudo sfdisk -N 1 '
                f"/dev/{sdf_maps_to}",
            )
        )

        # now get the resulting size of the overall disk after the resize
        disk_partitions_json = loop.run_until_complete(
            ssh.run_and_capture(connection, f"sudo sfdisk --json /dev/{sdf_maps_to}")
        )
        assert isinstance(disk_partitions_json.stdout, str)
        disk_partitions = json.loads(disk_partitions_json.stdout)
        disk_size_sectors = max(
            partition["size"] + partition["start"]
            for partition in disk_partitions["partitiontable"]["partitions"]
        )
        disk_size_gb = math.ceil(disk_size_sectors * 512 / (1024**3))

        print(
            f"We shrank the main partition to {main_partition_mb}MiB "
            f"({main_partition_num_16mb_chunks} 16MB chunks to copy). This means we can"
            f" copy to a disk that is {disk_size_gb}GB. "
            f"main_partition_bytes={main_partition_bytes} bytes"
        )

        # now we can create a new volume that is the size we want:
        new_volume = ec2_resource.create_volume(
            AvailabilityZone=instance.meta.data["Placement"]["AvailabilityZone"],
            Size=disk_size_gb,
        )
        while new_volume.state != "available":
            print(f"Waiting for volume to be created: {new_volume.state}")
            time.sleep(2)
            new_volume.load()
        new_volume.attach_to_instance(
            InstanceId=instance.instance_id, Device="/dev/sdh"
        )
        while new_volume.state != "in-use":
            print(f"Waiting for volume to attach: {new_volume.state}")
            time.sleep(2)
            new_volume.load()

        # blockdev --getsz sometimes returns 500MB for an AWS Volume even though we
        # clearly created a larger volume. We're not sure why this happens. Waiting
        # between attaching the volume and doing something with it seems to help.
        time.sleep(10)
        new_disk_actual_size_output = loop.run_until_complete(
            ssh.run_and_capture(connection, f"sudo blockdev --getsz /dev/{sdh_maps_to}")
        )
        assert isinstance(new_disk_actual_size_output.stdout, str)
        new_disk_actual_size = (
            int(new_disk_actual_size_output.stdout.strip()) * 512 / (1024**3)
        )
        if new_disk_actual_size != disk_size_gb:
            raise ValueError(
                f"The volume got mounted and it should be {disk_size_gb}GB but it is "
                f"actually {new_disk_actual_size}GB. We don't know why this happens, "
                f"usually detaching and re-attaching helps helps"
            )

        print(f"Created a new volume {new_volume.id}")

        # now copy the partition metadata over
        partition_metadata = loop.run_until_complete(
            ssh.run_and_capture(connection, f"sudo sfdisk --dump /dev/{sdf_maps_to}")
        )
        assert isinstance(partition_metadata.stdout, str)
        modified_partition_metadata = "".join(
            line.replace(sdf_maps_to, sdh_maps_to)
            for line in partition_metadata.stdout.splitlines(True)
            # first-lba and last-lba are optional so it's not worth recalculating them
            if not line.startswith("first-lba") and not line.startswith("last-lba")
        )
        loop.run_until_complete(
            ssh.write_to_file(
                connection,
                modified_partition_metadata.encode("utf-8"),
                "/home/ubuntu/part_table",
            )
        )
        loop.run_until_complete(
            ssh.run_and_print(
                connection,
                f"sudo sfdisk /dev/{sdh_maps_to} < /home/ubuntu/part_table",
            )
        )

        # now copy the main partition's data over
        loop.run_until_complete(
            ssh.run_and_print(
                connection,
                (
                    f"sudo dd bs=16M if=/dev/{sdf_maps_to}1 of=/dev/{sdh_maps_to}1 "
                    f"count={main_partition_num_16mb_chunks} status=progress"
                ),
            )
        )

        # now copy the remaining partitions' data over
        remaining_partitions = [
            partition["node"]
            for partition in disk_partitions["partitiontable"]["partitions"]
            if partition["node"] != f"/dev/{sdf_maps_to}1"
        ]
        for old_partition in remaining_partitions:
            print(f"Copying {old_partition}")
            loop.run_until_complete(
                ssh.run_and_print(
                    connection,
                    f"sudo dd bs=16M if={old_partition} "
                    f"of={old_partition.replace(sdf_maps_to, sdh_maps_to)}",
                )
            )

        # copy the MBR if requested. We haven't been able to get an MBR-based disk to
        # work with this method, it seems like sfdisk is not doing the right thing
        if copy_mbr:
            loop.run_until_complete(
                ssh.run_and_print(
                    connection, "sudo dd if=/dev/sdf of=/dev/sdk bs=512 count=1"
                )
            )

        # a sanity check
        new_num_blocks = _resize_disk(loop, connection, f"/dev/{sdh_maps_to}1")
        print(
            f"Orig num blocks should match new num blocks {main_partition_num_blocks} "
            f"{new_num_blocks}"
        )

        new_volume.detach_from_instance()
        while new_volume.state != "available":
            print(f"Waiting for volume to detach: {new_volume.state}")
            time.sleep(2)
            new_volume.load()

        new_snapshot = new_volume.create_snapshot()
        while new_snapshot.state != "completed":
            print(f"Waiting until snapshot completes: {new_snapshot.state}")
            time.sleep(20)
            new_snapshot.load()

        client = boto3.client("ec2", region_name=region_name)
        new_ami_id = client.register_image(
            Name=new_ami_name,
            RootDeviceName="/dev/sda1",
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        "DeleteOnTermination": True,
                        "SnapshotId": new_snapshot.id,
                        "VolumeSize": disk_size_gb,
                        "VolumeType": "gp2",
                    },
                }
            ],
        )["ImageId"]
        print(f"Image {new_ami_id} created!")

        return new_ami_id
    finally:
        if new_volume is not None:
            new_volume.delete()
        if instance is not None:
            instance.terminate()


def main() -> None:
    """
    This script resizes a base image to be as small as possible. There is some
    preliminary support for MBR-based disks, but this doesn't work at this point, so we
    only support GPT-based disks.

    Another warning--you should only run this script on the first AMI in your "chain" of
    AMIs. AMIs built on other AMIs get charged based on the blocks that are changed, but
    if you run this script on them, all of the blocks will change.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("regions", type=str)
    args = parser.parse_args()

    if args.regions == "all":
        regions = BASE_AMIS[args.type].keys()
    else:
        regions = args.regions.split(",")

    result = []
    for region in regions:
        print(f"About to process {region}")

        new_ami_id = shrink_ami(
            VANILLA_UBUNTU_AMIS[region],
            BASE_AMIS["plain"][region],
            "t2.micro",
            region,
            # for other types of instances, this will be nvme1n1p
            "xvdf",
            "xvdh",
            False,
        )

        result_str = f'"{region}": "{new_ami_id}",'
        result.append(result_str)
        print(f"Done processing region {region}:\n" + "\n".join(result))


if __name__ == "__main__":
    main()
