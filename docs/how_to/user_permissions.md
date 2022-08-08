# Grant permissions to use Meadowrun

This page describes how to grant users permissions to use Meadowrun. If you want to
grant permissions to Meadowrun jobs to access resources, go to [Access resources from
Meadowrun jobs](../access_resources).

## AWS

When you run `meadowrun-manage-ec2 install`, Meadowrun creates a user group called
`meadowrun_user_group` (see [Installing Meadowrun](../../tutorial/install)). Any account
that has a superset of the permissions granted by `meadowrun_user_group` will be able to
use Meadowrun. For example, a root/administrator account will be able to use Meadowrun
without any further configuration. For non-administrators, you can add them to the
Meadowrun user group to give them permissions to use Meadowrun:

```shell
aws iam add-user-to-group --group-name meadowrun_user_group --user-name <username>
```


## Azure

When you run `meadowrun-manage-azure-vm install`, Meadowrun creates a resource group
called `Meadowrun-rg`. We recommend that you assign users the Contributor role in the
`Meadowrun-rg` resource group scope, which will give them permissions to use Meadowrun
(see [Azure's documentation on assigning
roles](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal)).

```
az role assignment create --assignee <user email or id> --role Contributor --resource-group Meadowrun-rg
```
