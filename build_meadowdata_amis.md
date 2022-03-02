# Building meadowdata AMIs

This documents how to build the meadowdata AMIs in AWS. Ideally this would be replaced by some sort of automation.

## Install prerequisites

First, we'll build an AMI that has ubuntu, docker, and python installed.

- Launch an EC2 instance using the "Ubuntu Server 20.04 LTS (HVM), SSD Volume Type" quickstart AMI.
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
- Prepare a virtualenv for meadowdata
```shell
sudo mkdir /meadowgrid
sudo chown ubuntu:ubuntu /meadowgrid
mkdir /meadowgrid/env
python3.9 -m venv /meadowgrid/env

source /meadowgrid/env/bin/activate
pip install wheel  # not sure why this is necessary...
```
- Get the versions of everything we've installed:
```shell
lsb_release -a  # ubuntu version
docker --version
python3.9 --version
```

Now, create an AMI from this image, naming it based on the versions that were actually installed, e.g. `ubuntu-20.04.3-docker-20.10.12-python-3.9.5`. This AMI won't actually be used, but is helpful to have so that you don't need to redo these steps every time you want to deploy a new version of meadowdata.

## Install meadowdata

Continuing from the `ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance, we'll install meadowdata.

- On your development machine, build the meadowdata package: `poetry build`
- Copy the wheel file to the EC2 instance: `scp -i path/to/key.pem dist/meadowdata-0.1.0-py3-none-any.whl ubuntu@public-dns-of-instance:/meadowgrid` (replace `0.1.0` with the appropriate version number)
- On the EC2 instance, activate the meadowdata virtualenv and install the meadowdata package:
```shell
source /meadowgrid/env/bin/activate
pip install /meadowgrid/meadowdata-0.1.0-py3-none-any.whl
```

Now, create another AMI from this image, naming it based on the version that were actually installed, e.g. `meadowdata-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5`. This AMI won't actually be used, but we'll create the next two images with this image as a base.

## Set up EC2 alloc

Continuing from the `meadowdata-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance, we'll set up the deallocate_jobs.py script which needs to run on every EC2 instance.

- On the EC2 instance, add a new cronjob by running `crontab -e` and then adding
```
* * * * * /meadowgrid/env/bin/python /meadowgrid/env/lib/python3.9/site-packages/meadowgrid/deallocate_jobs.py >> /meadowgrid/deallocate_jobs.log 2>&1
```

Now, create the `meadowgrid-ec2alloc-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance. Copy the AMI ID for this into `aws_integration.py:_EC2ALLOC_AWS_AMI` and make the AMI public.
