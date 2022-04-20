Run a distributed map
=====================

Here we'll run a distributed :code:`map`, calling a single function on different
inputs/tasks on different cores/instances.

.. code-block:: python

    from meadowrun import run_map, EC2AllocHosts, Deployment
    await run_map(
        lambda n: n ** n,
        [1, 2, 3, 4],
        EC2AllocHosts(4, 32, 15, 3),
        Deployment.git_repo(
            "https://github.com/meadowdata/test_repo",
            conda_yml_file="myenv.yml"
        )
    )

This has roughly the same semantics as the python built-in function :code:`map(lambda n:
n ** n, [1, 2, 3, 4])` but it runs in parallel and distributed.

Based on the options specified in :code:`EC2AllocHost`, :code:`run_map` will launch the
cheapest combination of EC2 instances such that we can run 3 workers that each are
allocated at least 4 CPU and 32GB of memory. The instances will have <15% chance of
being interrupted (you can set this to 0 to exclude spot instances and only use
on-demand instances). The exact instance types chosen depends on current EC2 prices,
but an example output is:

.. code-block::

    1/3 workers allocated to existing EC2 instances: ec2-18-188-55-74.us-east-2.compute.amazonaws.com
    Launched 1 new EC2 instances (total $0.0898/hr) for the remaining 2 workers:
        ec2-3-16-123-166.us-east-2.compute.amazonaws.com: r5n.2xlarge (8 CPU/64.0 GB), spot ($0.0898/hr, 2.5% chance of interruption), will run 2 job/worker

Then, based on the options specified in :code:`Deployment.git_repo`,
:code:`run_function` will grab code from the :code:`main` branch of the
:code:`test_repo` git repo, and then create a conda environment (in a container) using
the :code:`myenv.yml` file in the git repo as the environment specification (assumes
that :code:`myenv.yml` was created using :code:`conda env export > myenv.yml`). Creating
the conda environment takes some time, but once it has been created, it gets cached and
reused using AWS ECR. Meadowrun can be used from Windows or Linux, but only Linux is
supported for the remote environment, so :code:`myenv.yml` must describe a
Linux-compatible conda environment. Unlike for :code:`run_function`, the conda
environment (:code:`myenv.yml`) for :code:`run_map` must have meadowrun installed.

Finally, the workers will execute tasks until there are none left, returning a list of
results:

.. code-block::

    > [1, 4, 27, 256]
