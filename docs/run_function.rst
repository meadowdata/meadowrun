Run a function remotely
=======================

Here we'll run a single function in AWS with meadowrun

.. code-block:: python

    from meadowrun import run_function, EC2AllocHost, Deployment
    await run_function(
       lambda: sum(range(1000)) / 1000,
       EC2AllocHost(
           logical_cpu_required=4,
           memory_gb_required=32,
           interruption_probability_threshold=15),
       Deployment.git_repo(
           "https://github.com/meadowdata/test_repo",
           conda_yml_file="myenv.yml"
       )
    )

Based on the options specified in :code:`EC2AllocHost`, :code:`run_function` will launch
the cheapest EC2 instance type that has at least 4 CPU and 32GB of memory, and a <15%
chance of being interrupted (you can set this to 0 to exclude spot instances and only
use on-demand instances). The exact instance type chosen depends on current EC2 prices,
but an example output is:

.. code-block::

    Launched a new EC2 instance for the job: ec2-52-15-119-88.us-east-2.compute.amazonaws.com: r5a.xlarge (4 CPU/32.0 GB), spot ($0.0424/hr, 2.5% chance of interruption), will run 1 job/worker

Then, based on the options specified in :code:`Deployment.git_repo`,
:code:`run_function` will grab code from the :code:`main` branch of the
:code:`test_repo` git repo, and then create a conda environment (in a container) using
the :code:`myenv.yml` file in the git repo as the environment specification (assumes
that :code:`myenv.yml` was created using :code:`conda env export > myenv.yml`). Creating
the conda environment takes some time, but once it has been created, it gets cached and
reused using AWS ECR. Meadowrun can be used from Windows or Linux, but only Linux is
supported for the remote environment, so :code:`myenv.yml` must describe a
Linux-compatible conda environment. Creating the container happens on the EC2 instance,
so make sure to size your :code:`EC2AllocHost` appropriately.

Finally, meadowrun will run the specified lambda in that environment on the remote
machine and return the result:

.. code-block::

    > 499.5
