# flake8: noqa

"""

This contains the notes from a failed attempt to replace Ubuntu with Clear Linux. The
biggest sticking point was the difficulty of getting Nvidia drivers installed in Clear
Linux:
https://community.clearlinux.org/t/nvidia-drivers-on-aws-clear-linux-key-was-rejected-by-service/7709

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

"""
