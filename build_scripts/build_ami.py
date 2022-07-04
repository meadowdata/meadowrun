# flake8: noqa

"""
A script to create a meadowrun EC2 BASE AMI. The only reason to want to change the base
AMI is because you want to change the version or flavour of Linux/Docker/Python we're
using, or some other deep baked-into-the-OS thing. To update meadowrun, you just need to
run the script below without going through these manual steps.

The main prerequisite for this script is an AMI that has that has Linux, Docker, and
Python installed, referenced as _BASE_AMI. To build this base AMI (which should also be
automated):


Clear Linux 
-----------
Clear Linux has the fastest boot times, see https://www.daemonology.net/blog/2021-08-12-EC2-boot-time-benchmarking.html)

To create a new base image, launch an EC2 instance with 2Gb of storage using the Clear Linux AMI ami-04ce26dd385676bc2 in us-east-2.

(That AMI - the base base ami - was created by following the instructions at
https://docs.01.org/clearlinux/latest/get-started/cloud-install/import-clr-aws.html and
with a little help from
https://stackoverflow.com/questions/69581704/an-error-occurred-invalidparameter-when-calling-the-importimage-operation-the
There is a Clear Linu AMI on AWS marketplace, but AMIs created via the marketplace can't
be changed and then made public.)

From a bash shell, run:
cat build_scripts/clear-linux-prepare-base.sh | ssh -i <key> clear@<public address>

Now, create an AMI from this image, naming it based on the versions that were actually
installed, e.g. `clear-36520-docker-20.10.11-python-3.10.5`. The srcipt should print these out.
This AMI won't actually be used for anything other than to build meadowrun AMIs.


Ubuntu
------
- Launch an EC2 instance using the "Ubuntu Server 20.04 LTS (HVM), SSD Volume Type"
quickstart AMI.
- ssh into the instance: `ssh -i path/to/key.pem ubuntu@public-dns-of-instance`. The following instructions should be run on the EC2 instance.
- [Install Docker](https://docs.docker.com/engine/install/ubuntu/):
```shell
sudo apt-get update

sudo apt-get install -y ca-certificates curl gnupg lsb-release

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io
```
- [Add the current user (ubuntu) to the docker group](https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket): `sudo usermod -aG docker ${USER}`
- TODO [docker docs recommend](https://docs.docker.com/config/containers/logging/configure/) setting "log-driver": "local" in docker config daemon.json
- Install python: `sudo apt install -y python3.9 python3.9-venv`
- Prepare a virtualenv for meadowrun
```shell
sudo mkdir /var/meadowrun
sudo chown ubuntu:ubuntu /var/meadowrun
mkdir /var/meadowrun/env
python3.9 -m venv /var/meadowrun/env

source /var/meadowrun/env/bin/activate
pip install wheel  # not sure why this is necessary...
```
- Get the versions of everything we've installed:
```shell
lsb_release -a  # ubuntu version
docker --version
python3.9 --version
```

Now, create an AMI from this image, naming it based on the versions that were actually
installed, e.g. `ubuntu-20.04.3-docker-20.10.12-python-3.9.5`. This AMI won't actually
be used for anything other than to build meadowrun AMIs.
"""

import asyncio
import os.path
import subprocess
import time

import boto3
import fabric

from build_image_shared import upload_and_configure_meadowrun
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import SSH_USER
from meadowrun.aws_integration.ec2 import (
    authorize_current_ip_helper,
    get_ssh_security_group_id,
    launch_ec2_instance,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
)

_BASE_AMI = "ami-02d4450c98101f83a"
_NEW_AMI_NAME = "meadowrun-ec2alloc-{}-clear-36600-docker-20.10.11-python-3.10.5"


async def build_meadowrun_ami():
    client = boto3.client("ec2")

    # get version

    # this only works if we're running in the directory with pyproject.toml
    package_root_dir = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.run(
        ["poetry", "version", "--short"], capture_output=True, cwd=package_root_dir
    )
    version = result.stdout.strip().decode("utf-8")
    new_ami_name = _NEW_AMI_NAME.format(version)
    print(f"New AMI name is: {new_ami_name}")

    # deregister existing image with the same name:
    existing_images = client.describe_images(
        Filters=[{"Name": "name", "Values": [new_ami_name]}]
    )["Images"]
    if existing_images:
        input(
            f"There's already an image with the name {new_ami_name}, press enter to "
            "delete it"
        )
        client.deregister_image(ImageId=existing_images[0]["ImageId"])

    # build a package locally
    subprocess.run(["poetry", "build"], cwd=package_root_dir)

    # launch an EC2 instance that we'll use to create the AMI
    print("Launching EC2 instance:")
    region_name = await _get_default_region_name()
    pkey = get_meadowrun_ssh_key(region_name)
    public_address = await (
        await launch_ec2_instance(
            region_name,
            "t2.micro",
            "on_demand",
            _BASE_AMI,
            [get_ssh_security_group_id(region_name)],
            key_name=MEADOWRUN_KEY_PAIR_NAME,
        )
    )
    print(f"Launched EC2 instance {public_address}")

    await authorize_current_ip_helper(region_name)

    with fabric.Connection(
        public_address,
        user=SSH_USER,
        connect_kwargs={"pkey": pkey},
    ) as connection:
        await upload_and_configure_meadowrun(
            connection,
            version,
            package_root_dir,
            # see comments in clear-linux-prepare-base.sh for explanation of shreds
            pre_command=(
                "sudo swupd update; "
                "sudo shred -u /etc/ssh/*_key /etc/ssh/*_key.pub; "
                "shred -u ~/.ssh/authorized_keys; "
                "sudo shred -u /var/lib/cloud/aws-user-data; "
                "sudo shred -u /etc/machine-id"
            ),
        )

    # get the instance id of our EC2 instance
    instances = client.describe_instances(
        Filters=[{"Name": "dns-name", "Values": [public_address]}]
    )
    instance_id = instances["Reservations"][0]["Instances"][0]["InstanceId"]

    # create an image, and wait for it to become available
    result = client.create_image(InstanceId=instance_id, Name=new_ami_name)
    image_id = result["ImageId"]
    print(f"New image id: {image_id}")
    while True:
        images = client.describe_images(ImageIds=[image_id])
        if images["Images"][0]["State"] != "pending":
            break
        else:
            print("Image is pending, sleeping for 5s...")
            time.sleep(5)
    # make it public
    client.modify_image_attribute(
        ImageId=image_id, LaunchPermission={"Add": [{"Group": "all"}]}
    )
    print("Done! Terminating instance")

    # now terminate the instance as we don't need it anymore
    client.terminate_instances(InstanceIds=[instance_id])

    print(f"New image id: {image_id}")
    print(
        "After testing, you will need to replicate to other regions via "
        f"`python build_scripts\\replicate_ami.py replicate {image_id}`"
    )
    print("Remember to delete old AMIs and snapshots!")


if __name__ == "__main__":
    asyncio.run(build_meadowrun_ami())
