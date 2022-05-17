Run a distributed map
=====================

Run a distributed :code:`map`, calling a Python function on different
inputs/tasks on different cores/instances.

Prerequisites
-------------

1. Choose a GitHub repo you'd like to run a function from. Third party dependencies, like pandas and numpy, are supported and  should be installed using Conda. If you don't have any repo to hand, try our our test repo https://github.com/meadowdata/test_repo.
2. Install meadowrun in the target repo, see :ref:`Installing Meadowrun <tutorial/install:install>`

Create a Conda environment export file
--------------------------------------

Meadowrun needs to know what the third-party dependencies are to execute the function. With Conda, the easiest way to do that is:

.. code-block:: shell

    conda list --export > myenv.yml 

Check this file into the repository, and push the change.

If you already have such a file in the repository, you can skip this step.

Meadowrun can be used from Windows or Linux, but only Linux is
supported for the remote environment, so :code:`myenv.yml` must describe a
Linux-compatible conda environment. 

Unlike for :code:`run_function`, the conda environment (:code:`myenv.yml`) for :code:`run_map` must have meadowrun installed.

Write a Python script to run the distributed map
------------------------------------------------

Here is an example template - add it to a file in a local checkout of the test repo, or where you can easily run it from an environment with meadowrun installed.

.. code-block:: python

    import asyncio
    from meadowrun import run_map, AllocCloudInstances, Deployment

    async def main():
        await run_map(
            lambda n: n ** n,
            [1, 2, 3, 4],
            AllocCloudInstances(
                logical_cpu_required_per_task=4,
                memory_gb_required_per_task=32,
                interruption_probability_threshold=15
                num_concurrent_tasks=3),
            Deployment.git_repo(
                "https://github.com/meadowdata/test_repo",
                conda_yml_file="myenv.yml"
            )
        )

    if __name__ == "__main__":
        asyncio.run(main())

This has roughly the same semantics as the python built-in function :code:`map(lambda n:
n ** n, [1, 2, 3, 4])` but it runs in parallel and distributed.

Run the script
--------------

Assuming you saved the file above as mdr.py:

.. code-block:: shell

    > python -m mdr
    1/3 workers allocated to existing EC2 instances: ec2-18-188-55-74.us-east-2.compute.amazonaws.com
    Launched 1 new EC2 instances (total $0.0898/hr) for the remaining 2 workers:
        ec2-3-16-123-166.us-east-2.compute.amazonaws.com: r5n.2xlarge (8 CPU/64.0 GB), spot ($0.0898/hr, 2.5% chance of interruption), will run 2 job/worker

The output will walk you through what Meadowrun's :code:`run_map` is doing:

1. Based on the options specified in :code:`AllocCloudInstances`, :code:`run_map`
launches the cheapest combination of EC2 instances such that we can run 3 workers that
each are allocated at least 4 CPU and 32GB of memory. The instances will have <15%
chance of being interrupted. You can set this to 0 to exclude spot instances and only
use on-demand instances.. The exact instance types chosen depends on current EC2 prices.

2. Based on the options specified in :code:`Deployment.git_repo`,
:code:`run_map` grabs code from the :code:`main` branch of the
:code:`test_repo` git repo, and creates a Conda environment (in a container) using
the :code:`myenv.yml` file in the git repo as the environment specification. Creating
the Conda environment takes some time, but once it has been created, it gets cached and
reused using AWS ECR. 

3. Finally, the workers will execute tasks until there are none left, returning a list of
results:

.. code-block::

    > [1, 4, 27, 256]
