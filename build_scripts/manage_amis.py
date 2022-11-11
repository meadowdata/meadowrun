import argparse
import time

import boto3
from build_ami_helper import REGION_TO_INSTANCE_TYPE

from meadowrun.aws_integration.boto_utils import ignore_boto3_error_code

_SOURCE_REGION = "us-east-2"  # your "main" region
_SUPPORTED_REGIONS = list(REGION_TO_INSTANCE_TYPE.keys())


def replicate_images(source_ami: str) -> None:
    """
    This function is deprecated--replicating AMIs across regions is much more expensive
    than recreating them in each region.
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
            image = client.copy_image(
                Name=image_name, SourceImageId=source_ami, SourceRegion=_SOURCE_REGION
            )
            destination_image_id = image["ImageId"]
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
            success, _ = ignore_boto3_error_code(
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
        for image in sorted(
            response.get("Images", ()), key=lambda image: image["Name"]
        ):
            print(f"{region}, {image['ImageId']}, {image['Name']}")


def delete_replicated_images(
    image_name_to_delete: str, include_source_region: bool, dry_run: bool
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
                if "Name" in image and image["Name"] == image_name_to_delete:
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
        help=(
            "Replicates the specified AMI to the regions configured in this file. This "
            "command is deprecated, as it is significantly cheaper to just re-build "
            "AMIs in each region rather than replicating them across regions."
        ),
    )
    parser_replicate.add_argument("source_ami_id")

    subparsers.add_parser("list", help="List all of your AMIs in all regions")

    parser_delete = subparsers.add_parser(
        "delete",
        help="Delete AMIs across all regions that start with the specified string",
    )
    parser_delete.add_argument("image_name_to_delete")
    parser_delete.add_argument("--include-source-region", action="store_true")
    parser_delete.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.command == "replicate":
        replicate_images(args.source_ami_id)
    elif args.command == "list":
        list_all_images()
    elif args.command == "delete":
        delete_replicated_images(
            args.image_name_to_delete, args.include_source_region, args.dry_run
        )
    else:
        ValueError(f"Unrecognized command: {args.command}")


if __name__ == "__main__":
    main()
