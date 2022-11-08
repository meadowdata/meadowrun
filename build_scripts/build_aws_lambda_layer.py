"""Run this to upload a new version of the lambda layer, which contains 3rd party
dependencies, to all regions.

The script create the lambda from a local dev meadowrun install, create a zip file in
the right format, upload it as a layer, and make the layer public so others can use it.

This is kind of untested, but this script should only upload new versions, so is
non-destructive. There's currently no script to delete old layer versions.

The script, when succesful, prins lines like:
> Published layer for region us-east-2, layer version Arn is
  arn:aws:lambda:us-east-2:344606234287:layer:meadowrun-dependencies:1.

The last part of that (after the region) should be updated in the constant in
LAMBDA_LAYER_POSTFIX in aws_mgmt_lambda_install.py. See also the comment there.
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
from typing import TYPE_CHECKING

import boto3

if TYPE_CHECKING:
    from typing_extensions import Literal

from build_ami_helper import REGION_TO_INSTANCE_TYPE

LAYER_NAME = "meadowrun-dependencies"
PYTHON_VERSION: Literal["python3.9"] = "python3.9"
REGIONS = tuple(REGION_TO_INSTANCE_TYPE.keys())
# uncomment this to only upload one region
# REGIONS = ("us-east-2",)


def main() -> None:
    if platform.system() != "Linux":
        raise Exception("This must be run on a Linux platform (WSL works).")
    # clean
    shutil.rmtree(f"dist/{LAYER_NAME}", ignore_errors=True)
    if os.path.exists(f"dist/{LAYER_NAME}.zip"):
        os.remove(f"dist/{LAYER_NAME}.zip")

    # build site-packages with dependencies
    result = subprocess.run(
        [
            "python",
            "-m",
            "pip",
            "install",
            "-t",
            f"dist/{LAYER_NAME}/python/lib/{PYTHON_VERSION}/site-packages",
            "-r",
            "src/meadowrun/aws_integration/management_lambdas/requirements.txt",
        ]
    )
    result.check_returncode()

    # no point taking pyc files, they'll likely be recompiled anyway due to python
    # version mismatch.
    result = subprocess.run(
        ["zip", "-x", "*.pyc", "-r", f"../{LAYER_NAME}.zip", "."],
        cwd=f"dist/{LAYER_NAME}",
    )
    result.check_returncode()

    zip_path = os.path.abspath(f"dist/{LAYER_NAME}.zip")
    print(f"Uploading {zip_path}")

    with open(zip_path, "rb") as zip_file:
        zip_bytes = zip_file.read()

    for region in REGIONS:
        lambda_client = boto3.client("lambda", region_name=region)
        response = lambda_client.publish_layer_version(
            LayerName=LAYER_NAME,
            Content={"ZipFile": zip_bytes},
            Description="Meadowrun dependencies layer",
            CompatibleRuntimes=[PYTHON_VERSION],
            CompatibleArchitectures=["x86_64"],
        )
        version = response["Version"]
        arn = response["LayerVersionArn"]
        print(f"Published layer for region {region}, layer version Arn is {arn}.")
        perm_result = lambda_client.add_layer_version_permission(
            LayerName=LAYER_NAME,
            VersionNumber=version,
            Action="lambda:GetLayerVersion",
            StatementId="make-meadowrun-lambda-layer-public",
            Principal="*",
        )
        rev_id = perm_result["RevisionId"]
        print(f"Updated permissions for region {region}, revision id is {rev_id}")


if __name__ == "__main__":
    main()
