# Run a function

This walks you through running a function on an EC2 instance using your local code and
libraries.

This tutorial won't work for you if you're using Conda on Windows or Mac, as you won't
be able to recreate your local Conda environment on a remote Linux machine, because
Conda doesn't work across platforms. You can either give
[Poetry](https://python-poetry.org/) a try, or follow a [Git repo-based
tutorial](../run_function_git_conda) instead. See [Cross-platform
compatibility](../../explanation/deployment/#cross-platform-compatibility) for more
information as well.


## Prerequisites

You'll need to have [installed meadowrun](../install). For this tutorial, we'll
use a poetry environment, but it should be straightforward to translate the steps to a
pip or Conda environment.


## Write a Python script to run a function remotely

Here is an example template, create a file like this locally:

```python
import asyncio
import meadowrun

if __name__ == "__main__":
    result = asyncio.run(
       meadowrun.run_function(
           # this is where the function to run goes
           lambda: sum(range(1000)) / 1000,
            # run on a dynamically allocated AWS EC2 instance
           meadowrun.AllocEC2Instance(),
           # requirements to choose an appropriate EC2 instance
           meadowrun.Resources(logical_cpu=1, memory_gb=4, max_eviction_rate=15),
           # mirror the local code and python environment
           meadowrun.Deployment.mirror_local()
       )
    )
    print(f"Meadowrun worked! Got {result}")
```

## Run the script

Assuming you saved the file above as mdr.py:

```
> python -m mdr 
Size of pickled function is 40
Job was not allocated to any existing EC2 instances, will launch a new EC2 instance
Launched a new EC2 instance for the job: ec2-18-216-7-235.us-east-2.compute.amazonaws.com:
t2.medium (2 CPU/4.0 GB), spot ($0.0139/hr, 2.5% eviction rate), will run 1 job/worker
Meadowrun worked! Got 499.5
```

The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:

1. Based on the options specified in [Resources][meadowrun.Resources] and
   [AllocEC2Instance][meadowrun.AllocEC2Instance], `run_function` will launch the
   cheapest EC2 instance type that has at least 1 CPU and 4GB of memory, and a <15%
   chance of being evicted. You can set this to 0 to exclude spot instances and only
   use on-demand instances. The exact instance type chosen depends on current EC2
   prices, but in this case we can see that it's a spot t2.medium, and we're paying
   $0.0139/hr for it.
   
3. [Deployment.mirror_local][meadowrun.Deployment.mirror_local] tells Meadowrun to copy
   your local environment and code to the provisioned EC2 instance. Meadowrun detects what
   kind of environment (conda, pip, or poetry) you're currently in, and for a poetry
   environment, sends the pyproject.toml and poetry.lock files to the EC2 instance. The EC2
   instance will create a new docker container based on that poetry environment and cache
   it for reuse in a Meadowrun-managed AWS ECR container registry. Meadowrun will also zip
   up your local code and send it to the EC2 instance.

4. Meadowrun runs the specified function in that environment on the remote machine and
   returns the result.


You can also log or print to stdout in your remote function--meadowrun will copy the
remote output to the local output.


## Next steps

Try running a [distributed map](../run_map)!
