# Installing Meadowrun

## Prerequisites

Meadowrun requires python 3.7 or above.

## Install

First, install Meadowrun using Pip, Conda, or Poetry:

=== "Pip"
    ```shell
    pip install meadowrun
    ```
=== "Conda"
    ```shell
    conda install -c defaults -c conda-forge -c meadowdata meadowrun
    ```
=== "Poetry"
    ```shell
    poetry add meadowrun
    ```

## Configure the AWS and/or Azure CLI

If you're using AWS and you haven't configured your AWS CLI, [install the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
then [configure it](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).

If you're using Azure and you haven't configured your Azure CLI, [install the Azure
CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and then
[login](https://docs.microsoft.com/en-us/cli/azure/get-started-with-azure-cli#how-to-sign-into-the-azure-cli).

Meadowrun makes it easy to run on both AWS and Azure--no need to stick to just one.


## Install Meadowrun in your AWS/Azure account

Finally, set up the permanent resources that Meadowrun needs to function (this step is
not needed if you're running on Kubernetes):

=== "AWS"
    ```
    > meadowrun-manage-ec2 install --allow-authorize-ips
    Creating resources for running meadowrun
    Created/updated user group meadowrun_user_group. Add users to this group to allow them
    to use Meadowrun with `aws iam add-user-to-group --group-name meadowrun_user_group
    --user-name <username>`
    Created resources in 27.18 seconds
    ```
=== "Azure"
    ```
    > meadowrun-manage-azure-vm install --allow-authorize-ips
    Creating resources for running meadowrun
    Created resources in 47.86 seconds
    ```

You will need to run this as a root/Administrator account.

In this example, we set `--allow-authorize-ips` which reduces the configuration we need
to do but is slightly less secure. You can rerun this command later without this option
to switch to the more secure configuration.

If you decide to stop using meadowrun, you can remove all meadowrun-related resources
easily:

=== "AWS"
    ```
    > meadowrun-manage-ec2 uninstall
    Deleting all meadowrun resources
    Deleted all meadowrun resources in 6.61 seconds
    ```
=== "Azure"
    ```
    > meadowrun-manage-azure-vm uninstall
    Deleting all meadowrun resources
    Deleted all meadowrun resources in 6.61 seconds
    ```


## Setting up non-administrator users

If you want non-administrator users to use Meadowrun, you'll need to give them enough
permissions. See [Grant permissions to use Meadowrun](/how_to/1_user_permissions). 


## Next steps

Once you've installed Meadowrun, try [running a function](/tutorial/run_function).
