# flake8: noqa

"""
A script to create a meadowrun EC2 AMI.

The main prerequisite for this script is an AMI that has that has ubuntu, docker, and
python installed, referenced as _BASE_AMI. To build this AMI (which should also be
automated):

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
import io
import os.path
import subprocess
import time
from typing import cast

import boto3
import fabric
import paramiko.ssh_exception

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2 import (
    ensure_meadowrun_ssh_security_group,
    launch_ec2_instance,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    ensure_meadowrun_key_pair,
)
from meadowrun.run_job import _retry

_BASE_AMI = "ami-01344892e448f48c2"
_NEW_AMI_NAME = "meadowrun-ec2alloc-{}-ubuntu-20.04.3-docker-20.10.12-python-3.9.5"


async def build_meadowrun_ami():
    client = boto3.client("ec2")

    # get version

    # this only works if we're running in the directory with pyproject.toml
    package_root_dir = os.path.dirname(os.path.dirname(__file__))
    result = subprocess.run(
        "poetry version --short", capture_output=True, cwd=package_root_dir
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
    pkey = ensure_meadowrun_key_pair(await _get_default_region_name())
    public_address = await launch_ec2_instance(
        await _get_default_region_name(),
        "t2.micro",
        "on_demand",
        _BASE_AMI,
        [await ensure_meadowrun_ssh_security_group()],
        key_name=MEADOWRUN_KEY_PAIR_NAME,
    )
    print(f"Launched EC2 instance {public_address}")

    with fabric.Connection(
        public_address,
        user="ubuntu",
        connect_kwargs={"pkey": pkey},
    ) as connection:
        # retry with a no-op until we've established a connection
        await _retry(
            lambda: connection.run("echo $HOME"),
            (
                cast(Exception, paramiko.ssh_exception.NoValidConnectionsError),
                cast(Exception, TimeoutError),
            ),
        )

        # install the meadowrun package
        connection.put(
            os.path.join(
                package_root_dir, "dist", f"meadowrun-{version}-py3-none-any.whl"
            ),
            "/var/meadowrun/",
        )
        connection.run(
            "source /var/meadowrun/env/bin/activate "
            f"&& pip install /var/meadowrun/meadowrun-{version}-py3-none-any.whl"
        )
        connection.run(f"rm /var/meadowrun/meadowrun-{version}-py3-none-any.whl")

        # set deallocate_jobs to run from crontab
        crontab_line = (
            "* * * * * /var/meadowrun/env/bin/python "
            "/var/meadowrun/env/lib/python3.9/site-packages/meadowrun/aws_integration/deallocate_jobs.py "  # noqa E501
            ">> /var/meadowrun/deallocate_jobs.log 2>&1\n"
        )
        with io.StringIO(crontab_line) as sio:
            connection.put(sio, "/var/meadowrun/meadowrun_crontab")
        connection.run("crontab < /var/meadowrun/meadowrun_crontab")
        connection.run("rm /var/meadowrun/meadowrun_crontab")

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
    print("Done! Terminating instance")

    # now terminate the instance as we don't need it anymore
    client.terminate_instances(InstanceIds=[instance_id])

    print(f"New image id: {image_id}")
    print("Remember to delete old AMIs and snapshots!")


if __name__ == "__main__":
    asyncio.run(build_meadowrun_ami())
