# Run a function remotely

Run a Python function on a single AWS EC2 instance.

## Prerequisites

Choose a GitHub repo you'd like to run a function from. Third party dependencies, like
pandas and numpy, are supported and should be installed using Conda. If you don't have
any repo to hand, try our test repo https://github.com/meadowdata/test_repo.

## Create a Conda environment export file

Meadowrun needs to know what the third-party dependencies are to execute the function.
With Conda, the easiest way to do that is:

```shell
conda list --export > myenv.yml
```

Check this file into the repository, and push the change.

If you already have such a file in the repository, you can skip this step.

Meadowrun can be used from Windows or Linux, but only Linux is supported for the remote
environment, so `myenv.yml` must describe a Linux-compatible conda environment.

Unlike for `run_map`, the Conda environment (`myenv.yml`) for `run_function` does not
need to have meadowrun installed.

## Write a Python script to run the function remotely

Here is an example template—add it to a file in a local checkout of the test repo, or
where you can easily run it from an environment with meadowrun installed.

```python
import asyncio
from meadowrun import run_function, AllocCloudInstance, Deployment

async def main():
    result = await run_function(
        # this is where the function to run goes
        lambda: sum(range(1000)) / 1000,
        # requirements to choose an appropriate EC2 instance
        AllocCloudInstance(
            logical_cpu_required=1,
            memory_gb_required=4,
            interruption_probability_threshold=15,
            cloud_provider="EC2"
        ),
        Deployment.git_repo(
            # URL to the repo
            "https://github.com/meadowdata/test_repo",
            # name of the environment file we created in step 1
            conda_yml_file="myenv.yml"
        )
    )
    print(f"Meadowrun worked! Got {result}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Run the script

Assuming you saved the file above as mdr.py:

```python
> python -m mdr
Size of pickled function is 40
Job was not allocated to any existing EC2 instances, will launch a new EC2 instance
Launched a new EC2 instance for the job: ec2-18-216-7-235.us-east-2.compute.amazonaws.com: t2.medium (2 CPU/4.0 GB), spot ($0.0139/hr, 2.5% chance of interruption), will run 1 job/worker
Meadowrun worked! Got 499.5
```

The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:

1. Based on the options specified in [AllocCloudInstance][meadowrun.AllocCloudInstance],
   `run_function` will launch the cheapest EC2 instance type that has at least 1 CPU and
   4GB of memory, and a <15% chance of being interrupted. You can set this to 0 to
   exclude spot instances and only use on-demand instances. The exact instance type chosen
   depends on current EC2 prices, but in this case we can see that it's a spot t2.medium,
   and we're paying $0.0139/hr for it.

2. Based on the options specified in
   [Deployment.git_repo][meadowrun.Deployment.git_repo], `run_function` grabs code from the
   `main` branch of the `test_repo` git repo, and then creates a conda environment (in a
   container) using the `myenv.yml` file in the git repo as the environment specification.
   Creating the conda environment takes some time, but once it has been created, it gets
   cached and reused using AWS ECR. Creating the container happens on the EC2 instance, so
   make sure to size your `AllocCloudInstance` appropriately.

3. Meadowrun runs the specified function in that environment on the remote machine and
   returns the result.


You can also log or print to stdout, meadowrun shows that in the local output.
