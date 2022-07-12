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

New option: use Clear Linux - Launch an EC2 instance using the Clear Linux AMI
(https://clearlinux.org/download). Use a 4Gb EBS volume.

- ssh into the instance: `ssh -i key_pair.pem clear@public-dns-of-instance`
- Run `sudo swupd update`
- Disable automatic updates (otherwise swupd will automatically update at at startup, using resources): sudo swupd autoupdate --disable
- Enable SFTP: https://docs.01.org/clearlinux/latest/guides/network/openssh-server.html?highlight=ssh#id4
    ```echo 'subsystem sftp /usr/libexec/sftp-server' | sudo tee -a /etc/ssh/sshd_config```
- Install cron & allow clear user to use it
    `sudo swupd bundle-add cronie`
    `sudo systemctl enable cronie`
    `echo clear | sudo tee -a /etc/cron.allow`
- Install Docker: `sudo swupd bundle-add containers-basic`
- [Add the current user (clear) to the docker group](https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket): `sudo usermod -aG docker ${USER}`
- Install Python: `sudo swupd bundle-add python-basic`

-
Prepare a virtualenv for meadowrun
```shell
sudo mkdir /var/meadowrun 
sudo chown clear:clear /var/meadowrun
mkdir /var/meadowrun/env
python -m venv /var/meadowrun/env
source /var/meadowrun/env/bin/activate
python -m pip install --upgrade pip
pip install wheel  # not sure why this is necessary...
```

- Get the versions of everything we've installed:
```shell
swupd info  # Clear Linux version
docker --version
python --version
```
Now, since the Clean Linux image is "sold" for 0 dollars on AWS Marketplace, we can't make any AMIs public.
This is not a problem for Clear Linux, as it's free anyway, but some technical AWS limitation to protect actually
for-pay marketplace AMIs.
To work around this, we'll remove the AWS marketplace code: https://www.caseylabs.com/remove-the-aws-marketplace-code-from-a-centos-ami/
- Create a 4Gb EBS volume in the same availability region as your EC2 instance.
- Attach the volume to your EC2 instance.
- Prepare the volume:
    - `sudo lsblk -f` should show the volumne id. It might be different from what AWS tells you. Assuming in what follows it's /dev/xvdf.
    - format it: `sudo mkfs -t ext4 /dev/xvdf`
- Copy the root volume to the new volume - assuming root volume is /dev/xvda: `sudo dd bs=1M if=/dev/xvda of=/dev/xvdf` 
- Shutdown (do not terminate yet!) the instance.
- 


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
from meadowrun.ssh import connect
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2 import (
    authorize_current_ip_helper,
    get_ssh_security_group_id,
    launch_ec2_instance,
)
from meadowrun.aws_integration.ec2_instance_allocation import SSH_USER
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
)
from meadowrun.run_job_core import _retry

from build_image_shared import upload_and_configure_meadowrun

_BASE_AMI = "ami-01344892e448f48c2"
_NEW_AMI_NAME = "meadowrun-ec2alloc-{}-ubuntu-20.04.3-docker-20.10.12-python-3.9.5"


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

    connection = await _retry(
        lambda: connect(
            public_address,
            username=SSH_USER,
            private_key=pkey,
        ),
        (TimeoutError, ConnectionRefusedError),
        max_num_attempts=20,
    )

    async with connection:
        await upload_and_configure_meadowrun(connection, version, package_root_dir)

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
