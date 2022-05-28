# Installing Meadowrun

## Prerequisites

Meadowrun requires python 3.7 or above.

## Install

First, install meadowrun with conda:

```shell
conda install -c defaults -c conda-forge -c hrichardlee meadowrun
```

## Configure the AWS and/or Azure CLI

If you want to use AWS, [install the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
then configure it [configured the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).

If you want to use Azure, [install the Azure
CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and then
[login](https://docs.microsoft.com/en-us/cli/azure/get-started-with-azure-cli#how-to-sign-into-the-azure-cli).

Meadowrun makes it easy to run on both AWS and Azure--no need to stick to just one.


## Install meadowrun in your AWS/Azure account

Finally, set up AWS Lambdas/Azure Functions that clean up resources (e.g. idle EC2
instances/VMs) when they're no longer needed:

```shell
> meadowrun-manage-[ec2|azure-vm] install
Creating lambdas for cleaning up meadowrun resources
Waiting for newly created AWS IAM role to become available...
Created lambdas in 17.63 seconds
```

Skipping this step won't prevent you from using meadowrun, but it does mean that once
you start using meadowrun your EC2 instances/VMs will run forever.

If you decide to stop using meadowrun, you can remove all meadowrun-related resources
easily:

```shell
> meadowrun-manage-[ec2|azure-vm] uninstall
Deleting all meadowrun resources
Deleted all meadowrun resources in 6.61 seconds
```
