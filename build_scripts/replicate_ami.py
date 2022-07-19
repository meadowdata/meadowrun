import argparse
import time

import boto3

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)

_SOURCE_REGION = "us-east-2"  # the region that you ran build_ami.py under
_SUPPORTED_REGIONS = [
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-central-1",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "eu-north-1",
]


def replicate_images(source_ami: str) -> None:
    """
    After building an AMI (using build_ami.py), we need to make it available in all the
    regions we want to support. This function copies the AMI specified by _SOURCE_AMI to
    all of the _SUPPORTED_REGIONS.
    """
    source_client = boto3.client("ec2", region_name=_SOURCE_REGION)
    image_name = source_client.describe_images(ImageIds=[source_ami])["Images"][0][
        "Name"
    ]
    print(
        f"Will replicate {image_name} from {_SOURCE_REGION} to "
        + ", ".join(_SUPPORTED_REGIONS)
    )

    # first, make sure the source image is public
    source_client.modify_image_attribute(
        ImageId=source_ami, LaunchPermission={"Add": [{"Group": "all"}]}
    )

    # then copy the image
    created_images = []
    for destination_region in _SUPPORTED_REGIONS:
        if destination_region != _SOURCE_REGION:
            client = boto3.client("ec2", region_name=destination_region)
            result = client.copy_image(
                Name=image_name, SourceImageId=source_ami, SourceRegion=_SOURCE_REGION
            )
            destination_image_id = result["ImageId"]
            created_images.append((destination_region, destination_image_id))

    print("Copy this into ec2_instance_allocation.py:_EC2_ALLOC_AMIS")
    print(f'"{_SOURCE_REGION}": "{source_ami}",')
    for destination_region, destination_image_id in created_images:
        print(f'"{destination_region}": "{destination_image_id}",')

    # now make those images public (this API requires waiting until the AMI has been
    # fully created)
    for destination_region, destination_image_id in created_images:
        client = boto3.client("ec2", region_name=destination_region)
        while True:
            success, result = ignore_boto3_error_code(
                lambda: client.modify_image_attribute(
                    ImageId=destination_image_id,
                    LaunchPermission={"Add": [{"Group": "all"}]},
                ),
                {"InvalidAMIID.Unavailable", "InvalidAMIID.NotFound"},
            )
            if not success:
                print(
                    f"Waiting for AMI {destination_image_id} in "
                    f"{destination_region} to be created"
                )
                time.sleep(2)
            else:
                print(f"Made AMI {destination_image_id} in {destination_region} public")
                break

    # repeating this from before, as it's sometimes hard to find the first one if there
    # are a lot of "waiting" messages in between
    print("Copy this into ec2_instance_allocation.py:_EC2_ALLOC_AMIS")
    print(f'"{_SOURCE_REGION}": "{source_ami}",')
    for destination_region, destination_image_id in created_images:
        print(f'"{destination_region}": "{destination_image_id}",')


def list_all_images() -> None:
    for region in _SUPPORTED_REGIONS:
        print(f"Checking {region}")
        client = boto3.client("ec2", region_name=region)
        response = client.describe_images(Owners=["self"])
        for image in response.get("Images", ()):
            print(f"{region}, {image['ImageId']}, {image['Name']}")


def delete_replicated_images(
    image_starts_with: str, include_source_region: bool, dry_run: bool
) -> None:
    """
    Deletes all images in _SUPPORTED_REGIONS (excluding _SOURCE_REGION unless
    include_source_region is set) that start with "meadowrun"
    """
    for region in _SUPPORTED_REGIONS:
        if region == _SOURCE_REGION and not include_source_region:
            print(f"Skipping {region} because it's the source region")
        else:
            print(f"Checking {region}")
            client = boto3.client("ec2", region_name=region)
            response = client.describe_images(Owners=["self"])
            for image in response.get("Images", ()):
                if "Name" in image and image["Name"].startswith(image_starts_with):
                    print(f"Will delete: {region}, {image['ImageId']}, {image['Name']}")
                    if not dry_run:
                        client.deregister_image(ImageId=image["ImageId"])
                    for block_device_mapping in image["BlockDeviceMappings"]:
                        if "Ebs" in block_device_mapping:
                            snapshot_id = block_device_mapping["Ebs"]["SnapshotId"]
                            print(f"Deleting related snapshot: {snapshot_id}")
                            if not dry_run:
                                client.delete_snapshot(SnapshotId=snapshot_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    parser_replicate = subparsers.add_parser(
        "replicate",
        help="Replicates the specified AMI to the regions configured in this file",
    )
    parser_replicate.add_argument("source_ami_id")

    subparsers.add_parser("list", help="List all of your AMIs in all regions")

    parser_delete = subparsers.add_parser(
        "delete",
        help="Delete AMIs across all regions that start with the specified string",
    )
    parser_delete.add_argument("starts_with")
    parser_delete.add_argument("--include-source-region", action="store_true")
    parser_delete.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command == "replicate":
        replicate_images(args.source_ami_id)
    elif args.command == "list":
        list_all_images()
    elif args.command == "delete":
        delete_replicated_images(
            args.starts_with, args.include_source_region, args.dry_run
        )
    else:
        ValueError(f"Unrecognized command: {args.command}")


if __name__ == "__main__":
    main()
