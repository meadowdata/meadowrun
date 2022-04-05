import time

import boto3

from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)

_SOURCE_REGION = "us-east-2"  # the region that you ran build_ami.py under
_SOURCE_AMI = "ami-074049d9b7ca1605a"  # the original AMI id in that region
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


def replicate_images() -> None:
    """
    After building an AMI (using build_ami.py), we need to make it available in all the
    regions we want to support. This function copies the AMI specified by _SOURCE_AMI to
    all of the _SUPPORTED_REGIONS.
    """
    source_client = boto3.client("ec2", region_name=_SOURCE_REGION)
    image_name = source_client.describe_images(ImageIds=[_SOURCE_AMI])["Images"][0][
        "Name"
    ]
    # first, make sure the source image is public
    source_client.modify_image_attribute(
        ImageId=_SOURCE_AMI, LaunchPermission={"Add": [{"Group": "all"}]}
    )

    # then copy the image
    created_images = []
    for destination_region in _SUPPORTED_REGIONS:
        if destination_region != _SOURCE_REGION:
            client = boto3.client("ec2", region_name=destination_region)
            result = client.copy_image(
                Name=image_name, SourceImageId=_SOURCE_AMI, SourceRegion=_SOURCE_REGION
            )
            destination_image_id = result["ImageId"]
            created_images.append((destination_region, destination_image_id))

    print("Copy this into ec2_alloc.py:_EC2_ALLOC_AMIS")
    print(f'"{_SOURCE_REGION}": "{_SOURCE_AMI}",')
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
    print("Copy this into ec2_alloc.py:_EC2_ALLOC_AMIS")
    print(f'"{_SOURCE_REGION}": "{_SOURCE_AMI}",')
    for destination_region, destination_image_id in created_images:
        print(f'"{destination_region}": "{destination_image_id}",')


def list_all_images() -> None:
    for region in _SUPPORTED_REGIONS:
        print(f"Checking {region}")
        client = boto3.client("ec2", region_name=region)
        response = client.describe_images(Owners=["self"])
        for image in response.get("Images", ()):
            print(f"{region}, {image['ImageId']}, {image['Name']}")


def delete_replicated_images(image_starts_with: str, dry_run: bool) -> None:
    """
    Deletes all images in _SUPPORTED_REGIONS (excluding _SOURCE_REGION) that start with
    "meadowrun"
    """
    for region in _SUPPORTED_REGIONS:
        if region == _SOURCE_REGION:
            print(f"Skipping {region} because it's the source region")
        else:
            print(f"Checking {region}")
            client = boto3.client("ec2", region_name=region)
            response = client.describe_images(Owners=["self"])
            for image in response.get("Images", ()):
                if image["Name"].startswith(image_starts_with):
                    print(f"Will delete: {region}, {image['ImageId']}, {image['Name']}")
                    if len(image["BlockDeviceMappings"]) != 1:
                        print("Warning skipping, because len(BlockDeviceMappings) != 1")
                    else:
                        snapshot_id = image["BlockDeviceMappings"][0]["Ebs"][
                            "SnapshotId"
                        ]
                        if not dry_run:
                            client.deregister_image(ImageId=image["ImageId"])
                        print(f"Deleting related snapshot: {snapshot_id}")
                        if not dry_run:
                            client.delete_snapshot(SnapshotId=snapshot_id)
