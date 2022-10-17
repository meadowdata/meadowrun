# Grant permissions to use Meadowrun on AWS or Azure

This page describes how to grant users permissions to use Meadowrun. If you want to
grant permissions to Meadowrun jobs to access resources, go to [Access resources from
Meadowrun jobs](../access_resources).

## AWS

### For users

Meadowrun will usually be used from a developer machine where a developer or data
scientist has logged into the AWS CLI via `aws configure` (see [the AWS EC2
tutorial](../../tutorial/aws_ec2)).

In this case, the logged in user must have at least the permissions granted by the
`meadowrun_user_group`. This group is created when you run `meadowrun-manage-ec2
install`. For example, a root/administrator account will be able to use Meadowrun
without any further configuration. For non-administrators, you can add them to the
Meadowrun user group to give them permissions to use Meadowrun:

```shell
aws iam add-user-to-group --group-name meadowrun_user_group --user-name <username>
```

### For automated users

If you're running an automated job or service that is calling Meadowrun, you probably
won't have logged into the AWS CLI. In that case, you'll need to grant sufficient
permissions to the IAM role that your automated job/service is running as. The easiest
way to do this is to attach the `meadowrun_user_policy` policy to the IAM role you want
to use.

### Security groups

Finally, the user will need to be able to SSH into the EC2 instances that Meadowrun
creates. Meadowrun-managed EC2 instances will always be in the
`meadowrun_ssh_security_group`.

- If you run `meadowrun-manage-ec2 install` with the `--allow-authorize-ips` parameter,
  Meadowrun users will be allowed to modify the `meadowrun_ssh_security_group`, and
  Meadowrun will automatically add a rule to the security group to allow SSH (port 22)
  access from their current IP address.
- If `--allow-authorize-ips` is not set, which is the default, you (the administrator)
  will need to manually modify the `meadowrun_ssh_security_group` to allow traffic from
  the IPs that your users will be using.


## Azure

When you run `meadowrun-manage-azure-vm install`, Meadowrun creates a resource group
called `Meadowrun-rg`. We recommend that you assign users the Contributor role in the
`Meadowrun-rg` resource group scope, which will give them permissions to use Meadowrun
(see [Azure's documentation on assigning
roles](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal)).

```
az role assignment create --assignee <user email or id> --role Contributor --resource-group Meadowrun-rg
```
