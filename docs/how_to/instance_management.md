# Pre-provision AWS EC2 machines

By default, Meadowrun only starts new EC2 machines when a job is started. To reduce job start times, you can configure Meadowrun to pre-provision machines.

Configuring Meadowrun uses a flexible, Python-only configuration mechanism. See [Create your first configuration for managing AWS EC2 instances](../../tutorial/aws_configuration) to learn how to create and apply configuration.

## The configuration options

The template configuration file looks like this:

```python
def get_config() -> ManagementConfig:
    defaults = ManagementConfig()
    return defaults
```

The idea is for `get_config` to return a `ManagementConfig` instance. To configure how many and what kind of resources to pre-provision, you can set `instance_thresholds` to a list of [Thresholds][meadowrun.Threshold]. Each threshold defines some amount of resources that you'd like to be available at all times. Meadowrun will then take care of keeping machines running (if they are already), or start new machines if thresholds are not met.

## Defining thresholds

How to use thresholds is easiest to explain with an example.

```python
defaults.instance_thresholds = [
    Threshold(Resources(logical_cpu=1, memory_gb=4), num_resources=4)
]
```

This threshold keeps 4 "chunks" (because `num_resources=4`) of resources available, each chunk consisting of 1 CPU + 4GB. The idea is that you'll have resources to run a `run_map` with 4 workers and 1 CPU/4GB per worker, and Meadowrun figures out how to best create instances and instance type to make sure that these resources are available. For the example, it could be 4 small machines with 1 CPU/4GB, or a single big machine with 4 CPUs/16GB, or some combination.

Note that a threshold specifies the total amount of resources available, regardless of whether they're being used.

You can also define multiple thresholds, and Meadowrun tries to meet them all:

```python
defaults.instance_thresholds = [
    Threshold(Resources(logical_cpu=1, memory_gb=4), num_resources=4),
    Threshold(Resources(gpus=1, gpu_memory=16), num_resources=2),
]
```

This example starts the same machines as before, and additionally makes sure two chunks of 1 GPU/16GB are available.

## Apply the configuration

Once you're done with defining thresholds, apply the configuration using:

```bash
> meadowrun-manage-ec2 config --set custom_config.py
Changing management lambda config
Changed management lambda config in 9.00 seconds
```

This assumes you've been editing a file called `custom_config.py`.

## More information

- [More detail](../../explanation/thresholds) on how Meadowrun decides when to terminate existing instances, and start new ones.
- [How to manage configuration](../../how_to/manage_configuration)