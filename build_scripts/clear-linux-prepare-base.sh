#!bin/bash

# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

sudo swupd update --quiet --wait-for-scripts --no-progress
# Disable automatic updates (otherwise swupd will automatically update at at startup, using resources)
sudo swupd autoupdate --disable
# Enable SFTP: https://docs.01.org/clearlinux/latest/guides/network/openssh-server.html?highlight=ssh#id4
echo 'subsystem sftp /usr/libexec/sftp-server' | sudo tee -a /etc/ssh/sshd_config
# Install cron & allow clear user to use it
sudo swupd bundle-add cronie
sudo systemctl enable cronie
echo clear | sudo tee -a /etc/cron.allow
# Install Docker
sudo swupd bundle-add containers-basic 
# Start Docker on boot
sudo systemctl enable docker
# Add the current user (clear) to the docker group https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket
sudo usermod -aG docker ${USER}
# Install Python
sudo swupd bundle-add python-basic
# Prepare a virtualenv for meadowrun
sudo mkdir /var/meadowrun 
sudo chown ${USER}:${USER} /var/meadowrun
mkdir /var/meadowrun/env
python -m venv /var/meadowrun/env
source /var/meadowrun/env/bin/activate
python -m pip install --upgrade pip
pip install wheel  # not sure why this is necessary...

# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/building-shared-amis.html
sudo shred -u /etc/ssh/*_key /etc/ssh/*_key.pub
shred -u ~/.ssh/authorized_keys
# Clear Linux only fetches new user data, which includes the configured ssh keys,
# if this file is not present at boot. In other words this file needs to be deleted
# every time we make an image.
sudo shred -u /var/lib/cloud/aws-user-data

# also delete the machine id, otherwise all machines will have the same 
# uname -n
sudo shred -u /etc/machine-id 

# Get the versions of everything we've installed
swupd info  # Clear Linux version
docker --version
python --version