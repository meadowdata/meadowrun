# Access resources from Meadowrun jobs

For most non-trivial applications, you'll need to access resources in your cloud account
from code that is running on Meadowrun-managed machines, like an S3 bucket, a DynamoDB
table, etc. In order to do that, you'll usually need to explicitly grant access.

(If you're looking for how to grant users permissions to use Meadowrun, see [Grant
permissions to use Meadowrun](../user_permissions).)


## AWS

The Meadowrun-managed EC2 instances will be started with a Meadowrun-configured IAM role
called `meadowrun_ec2_role`. You should attach policies to this role to grant access to
whatever resources you want to access (see [AWS' documentation on adding permissions to
an
identity](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_manage-attach-detach.html)).
Your policies will not get detached if you rerun `meadowrun-manage-ec2 install`--only
the Meadowrun-managed policies will be updated. Your policies will be detached but not
deleted if you run `meadowrun-manage-ec2 uninstall`.

In addition, there are some command line helpers for easily adding access to commonly
used resources:

- `meadowrun-manage-ec2 grant-permission-to-secret <secret_name>`
- `meadowrun-manage-ec2 grant-permission-to-s3-bucket <bucket_name>`
- `meadowrun-manage-ec2 grant-permission-to-ecr-repo <repository_name>`


## Azure

The Meadowrun-managed VMs will be started with the Meadowrun-configured managed identity
called `meadowrun-managed-identity`. You should assign roles to this managed identity to
grant access to whatever resources you want to access (see [Azure's documentation on
assigning
roles](https://docs.microsoft.com/en-us/azure/role-based-access-control/role-assignments-portal)).

`meadowrun-managed-identity` is configured by default to have the `Contributor` role in
the `Meadowrun-rg` resource group, so anything created in that resource group will be
accessible without any further configuration.
