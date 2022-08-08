# Run a distributed map

Run a distributed `map`, calling a Python function on different inputs/tasks on
different cores/instances.

## Prerequisites

This assumes that you've successfully run a function by following [Run a
function](../run_function) or [Run a function from a git repo using
Conda](../run_function_git_conda)

## Write a Python script to run the distributed map

Here is an example template—add it to a file in a local checkout of the test repo, or
where you can easily run it from an environment with meadowrun installed.

```python
import asyncio
import meadowrun

async def main():
    await meadowrun.run_map(
        lambda n: n ** n,
        [1, 2, 3, 4],
        host=meadowrun.AllocCloudInstance(cloud_provider="EC2"),
        resources_per_task=meadowrun.Resources(
            logical_cpu=4, memory_gb=32, max_eviction_rate=15
        ),
        num_concurrent_tasks=3,
        deployment=await meadowrun.Deployment.mirror_local()
    )

if __name__ == "__main__":
    print(asyncio.run(main()))
```

This has roughly the same semantics as the python built-in function `map(lambda n: n **
n, [1, 2, 3, 4])` but it runs in parallel and distributed.

## Run the script

Assuming you saved the file above as mdr.py:

```shell
> python -m mdr
1/3 workers allocated to existing EC2 instances: ec2-18-188-55-74.us-east-2.compute.amazonaws.com
Launched 1 new EC2 instances (total $0.0898/hr) for the remaining 2 workers:
    ec2-3-16-123-166.us-east-2.compute.amazonaws.com: r5n.2xlarge (8 CPU/64.0 GB), spot ($0.0898/hr, 2.5% chance of interruption), will run 2 job/worker
```

The output will walk you through what Meadowrun's [run_map][meadowrun.run_map] is doing:

Based on the options specified in [Resources][meadowrun.Resources] and
[AllocCloudInstance][meadowrun.AllocCloudInstance], `run_map` launches the cheapest
combination of EC2 instances/Azure VMs such that we can run 3 workers that each are
allocated at least 4 CPU and 32GB of memory. (In this case, we already have one instance
that can run a worker, so we'll use that in addition to launching another instance for
the remaining 2 workers.) The instances will have <15% chance of being interrupted. You
can set this to 0 to exclude spot instances and only use on-demand instances. The exact
instance types chosen depends on current EC2/Azure VM prices.

The workers will execute tasks until there are none left, eventually returning a list of
results:

```shell
[1, 4, 27, 256]
```
