# Run a command in a container

This walks you through running a command in an already-built container on an EC2 instance.

## Prerequisites

You'll need to have [installed meadowrun](../install).

Also, you'll need a working container registry (e.g. Amazon Elastic Container Registry)
if you want to pull images you've built yourself. Private images require some additional
setup to give Meadowrun access to a secret. More info for
[AWS](../how_to/private_container_aws.md), [Azure](../how_to/private_container_azure.md)
or [Kubernetes](../how_to/private_container_kubernetes.md).

## Write a Python script to run a command in a container on EC2

Here is an example template, create a file like this locally:

```python
import asyncio

import meadowrun

async def main(): 
    await meadowrun.run_command(
        # the command to run
        ("python", "--version"), 
        # run on an AWS EC2 instance
        meadowrun.AllocEC2Instance(),
        # requirements for the EC2 instance
        meadowrun.Resources(logical_cpu=1, memory_gb=1, max_eviction_rate=80),
        # what to deploy on the VM - here an image in a container registry
        meadowrun.Deployment.container_image(
            repository="python", 
            tag="slim-bullseye"
        ),
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## Run the script

Assuming you saved the file above as mdr.py:

```
> python -m mdr 
Job was not allocated to any existing instances, will launch a new instance
Launched a new instance for the job: ec2-3-143-216-161.us-east-2.compute.amazonaws.com: t3.small (2.0 CPU, 2.0 GB), spot ($0.0062/hr, 2.5% eviction rate), will run 1 workers
Running job on ec2-3-143-216-161.us-east-2.compute.amazonaws.com /var/meadowrun/job_logs/python---version.f409064a-abcb-43b9-8003-822edc5a546c.log
Pulling docker image python:slim-bullseye
Successfully pulled docker image python:slim-bullseye
Running container (py_command): python --version; container image=python:slim-bullseye; PYTHONPATH=None log_file_name=/var/meadowrun/job_logs/python---version.f409064a-abcb-43b9-8003-822edc5a546c.log; code paths= ports=
Task worker: Python 3.10.7
```

The output walks you through what Meadowrun's [run_command][meadowrun.run_command]
is doing:

1. Based on the options specified in [Resources][meadowrun.Resources] and
   [AllocEC2Instance][meadowrun.AllocEC2Instance], `run_command` launches the
   cheapest EC2 instance type that has at least 1 CPU and 1GB of memory, and a <80%
   chance of being evicted. You can set this to 0 to exclude spot instances and only
   use on-demand instances. The exact instance type chosen depends on current EC2
   prices, but in this case we can see that it's a spot t3.small, and we're paying
   $0.0062/hr for it.
   
2. [Deployment.container_image][meadowrun.Deployment.container_image] tells Meadowrun to
   pull a container image from the given repository with the given tag. You can
   optionally pass in a secret to access a private container registry, see the
   prerequisites above for more information.

3. Meadowrun runs the specified command in the container on the remote machine and
   prints the output.

## Next steps

Try running a [distributed map](../run_map)!
