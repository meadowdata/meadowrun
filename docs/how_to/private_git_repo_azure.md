# Use a private git repo on Azure

To use a private git repo, you'll need to give Meadowrun the name of an Azure secret
that contains the private SSH key for the repo you want to use.

## Prerequisites

If you don't already have an SSH key for accessing your repo, you'll need to set one up
with your git hosting provider. For example, see [GitHub's instructions for adding a
deploy key](https://docs.github.com/en/developers/overview/managing-deploy-keys#setup-2)

## Get the name of your Meadowrun Key Vault

The Meadowrun Key Vault will be created the first time you use Meadowrun, so if you
haven't done so, [run a function with Meadowrun](/tutorial/run_function.md). Next, go to
the Azure portal, and go to "Key Vaults". You should see a Key Vault named something
like "mr724792bd295194e79d5881" in the Meadowrun-rg Resource Group. The actual name will
be "mr" followed by the last 22 letters/numbers of your subscription id (Azure Key Vault
names need to be globally unique).

## Create a secret

Next, create a secret in this Key Vault called `my_ssh_key` where the contents are the
private SSH key for the repo you want to use. This will be a multi-line secret, so
you'll need to [use the CLI or
Powershell](https://docs.microsoft.com/en-in/azure/key-vault/secrets/multiline-secrets)
to create it.

```shell
az keyvault secret set --vault-name "mr724792bd295194e79d5881" --name "my_ssh_key" --file "path/to/secret_key"
```

## Using your secret

Now you can use the following [Deployment][meadowrun.Deployment] with
[run_function][meadowrun.run_function] or [run_map][meadowrun.run_map]:

```python
import meadowrun

meadowrun.Deployment.git_repo(
    "git@github.com:my_organization/my_private_repo",
    interpreter=meadowrun.CondaEnvironmentYmlFile("myenv.yml"),
    ssh_key_secret=meadowrun.AzureSecret("my_ssh_key")
)
```

Note that we're using the SSH URL. The equivalent https URL (e.g.
`https://github.com/my_organization/my_private_repo`) will NOT work because we're using
an SSH key.

