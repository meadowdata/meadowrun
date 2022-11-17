# Create your first configuration for managing AWS EC2 instances

This page shows how to set configuration settings that determine when Meadowrun shuts down idle instances, and allows pre-provisioning instances.

## Prerequisites

Please make sure you have installed Meadowrun, as outlined in [the prerequisites here](../aws_ec2).

## Generate a default configuration template

In an environment with meadowrun installed, run:

```bash
> meadowrun-manage-ec2 config --get default
Creating default configuration as custom_config.py
Edit custom_config.py to your liking, then upload it with --set custom_config.py
```

You should now have a file `custom_config.py` in your working directory.

## Edit the generated custom_config.py file

For the tutorial, we'll simply modify how long Meadowrun allows a machine to be idle, i.e. not running any jobs, before it terminates the machine.

Change `get_config` to look as follows:

```python
def get_config() -> ManagementConfig:
    defaults = ManagementConfig()
    defaults.terminate_instances_if_idle_for = dt.timedelta(hours=1)
    return defaults
```

After we apply this configuration in the next step, Meadowrun will keep idle machines around for an hour, instead of the default 5 minutes.

## Apply the configuration

Run the following:

```bash
> meadowrun-manage-ec2 config --set custom_config.py
Changing management lambda config
Changed management lambda config in 9.00 seconds
```

That's it! You can edit and re-run `config --set custom_config.py` as many times as you want to change the configuration.

## Next steps

- [How to pre-provision AWS EC2 machines](../../how_to/instance_management)
- [How to manage configuration](../../how_to/manage_configuration)