# Use a private git repo on AWS

To use a private git repo, you'll need to give Meadowrun the name of an AWS secret that
contains the private SSH key for the repo you want to use.

## Prerequisites

If you don't already have an SSH key for accessing your repo, you'll need to set one up
with your git hosting provider. For example, see [GitHub's instructions for adding a
deploy key](https://docs.github.com/en/developers/overview/managing-deploy-keys#setup-2)

## Create an AWS Secret

First, [create an AWS
secret](<https://docs.aws.amazon.com/secretsmanager/latest/userguide/tutorials_basic.html#tutorial-basic-step1>)
called `my_ssh_key` that contains a key-value pair where the key is `private_key` and
the value is the contents of a private SSH key that has permission to read your private
git repo. If you're using the AWS Console, you'll need to use the "Plaintext" view and
use explicit newline characters to create the appropriate line breaks. The "plaintext"
will look something like:

```json
{
  "private_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nLINE1\nLINE2\n-----END OPENSSH PRIVATE KEY-----\n"
}
```

Note that the trailing newline is very important!

## Grant permission to the secret

Next, the EC2 instances that are running your code (i.e. the role that they run as) will
need to access this secret:

```shell
meadowrun-manage-ec2 grant-permission-to-secret my_ssh_key
```

## Use your secret

Now you can use the following [Deployment][meadowrun.Deployment] with
[run_function][meadowrun.run_function] or [run_map][meadowrun.run_map]:

```python
import meadowrun

meadowrun.Deployment.git_repo(
    "git@github.com:my_organization/my_private_repo",
    interpreter=meadowrun.CondaEnvironmentYmlFile("myenv.yml"),
    ssh_key_secret=meadowrun.AwsSecret("my_ssh_key")
)
```

Note that we're using the SSH URL. The equivalent https URL (e.g.
`https://github.com/my_organization/my_private_repo`) will NOT work because we're using
an SSH key.
