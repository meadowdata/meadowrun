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

## Set up the agent

Continuing from the `meadowdata-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance, we'll set up the meadowgrid agent.

- On the EC2 instance, write a file using e.g. `sudo vi /etc/systemd/system/meadowgrid_agent.service` with the contents:
```
[Unit]
Description="meadowgrid agent"
# per https://serverfault.com/questions/871328/start-service-after-aws-user-data-has-run
After=cloud-final.service
StartLimitIntervalSec=0

[Service]
User=ubuntu
Group=ubuntu
# we're expecting this file to contain e.g. COORDINATOR_HOST=127.0.0.1\nAGENT_ID=ae094361-3b47-4861-9c46-34e990fb16f5\nJOB_ID
EnvironmentFile=/meadowgrid/agent.conf
Environment=PYTHONUNBUFFERED=1
ExecStart=/meadowgrid/env/bin/meadowgrid_agent --working-folder /meadowgrid/working_folder --coordinator-host $COORDINATOR_HOST --agent-id $AGENT_ID --job-id $JOB_ID
Restart=always
RestartSec=1

[Install]
WantedBy=cloud-init.target
```
- Then enable the systemd service: `sudo systemctl enable meadowgrid_agent`

Now, create the `meadowgrid-agent-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance. This AMI will be used to run the meadowgrid agent. Copy the AMI ID for this into `aws_integration.py:_AGENT_AWS_AMI` and make the AMI public.

## Set up the coordinator

Now create a fresh instance from the `meadowdata-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image (i.e. "undo" everything we did to set up the agent).

- On the EC2 instance, write a file using e.g. `sudo vi /etc/systemd/system/meadowgrid_coordinator.service` with the contents:
```
[Unit]
Description="meadowgrid coordinator"
# per https://serverfault.com/questions/871328/start-service-after-aws-user-data-has-run
After=cloud-final.service
StartLimitIntervalSec=0

[Service]
User=ubuntu
Group=ubuntu
Environment=PYTHONUNBUFFERED=1
ExecStart=/meadowgrid/env/bin/meadowgrid_coordinator --host 0.0.0.0 --agent-creator aws
Restart=always
RestartSec=1

[Install]
WantedBy=cloud-init.target
```
- Then enable the systemd service: `sudo systemctl enable meadowgrid_coordinator`
- This is quite hacky, but to make things a bit quicker, we're going to patch in the agent AMI ID. So `vi /meadowgrid/env/lib/python3.9/site-packages/meadowgrid/aws_integration.py` and edit `_AGENT_AWS_AMI` to be the AMI ID for the agent that we created in the last step.

Now, create the `meadowgrid-coordinator-0.1.0-ubuntu-20.04.3-docker-20.10.12-python-3.9.5` image/instance. This AMI will be used to run the meadowgrid coordinator. Copy the AMI ID for this into `aws_integration.py:_COORDINATOR_AWS_AMI` and make the AMI public.
