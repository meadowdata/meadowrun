# Installing Meadowrun

## Prerequisites

Meadowrun requires python 3.7 or above.

## Install

First, install meadowrun with conda:

```shell
conda install -c defaults -c conda-forge -c meadowdata meadowrun
```

## Configure the AWS and/or Azure CLI

If you want to use AWS, [install the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
then [configure it](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).

If you want to use Azure, [install the Azure
CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and then
[login](https://docs.microsoft.com/en-us/cli/azure/get-started-with-azure-cli#how-to-sign-into-the-azure-cli).

Meadowrun makes it easy to run on both AWS and Azure--no need to stick to just one.


## Install meadowrun in your AWS/Azure account

Finally, set up the permanent resources that Meadowrun needs to function:

```shell
> meadowrun-manage-[ec2|azure-vm] install --allow-authorize-ips
Creating resources for running meadowrun
Created/updated user group meadowrun_user_group. Add users to this group to allow them
to use Meadowrun with `aws iam add-user-to-group --group-name meadowrun_user_group
--user-name <username>`
Created resources in 27.18 seconds
```

You will need to run this as a root/Administrator account.

In this example, we set --allow-authorize-ips which reduces the configuration we need to
do but is slightly less secure. You can easily rerun this command later without this
option to switch to the more secure configuration.

If you decide to stop using meadowrun, you can remove all meadowrun-related resources
easily:

```shell
> meadowrun-manage-[ec2|azure-vm] uninstall
Deleting all meadowrun resources
Deleted all meadowrun resources in 6.61 seconds
```


## Add users to the meadowrun user group

### AWS

If you want to use Meadowrun from a non-root/Administrator account, you'll need to add
users to the Meadowrun User Group. As the command output also shows, you can run:
```shell
aws iam add-user-to-group --group-name meadowrun_user_group --user-name <username>
```

### Azure

If you want to use Meadowrun from a non-root/Administrator account, you'll need to make
users Contributors to the Meadowrun-rg Resource Group, which is created by the `install`
command.

```shell
az role assignment create --assignee <user email or id> --role Contributor --resource-group Meadowrun-rg
```
