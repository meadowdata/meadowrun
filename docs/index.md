# Overview

Meadowrun automates the tedious details of running your python code on AWS or Azure.
Meadowrun will

- choose the optimal set of spot or on-demand EC2 instances/Azure VMs and turn them off
  when they're no longer needed
- deploy your code and libraries to the instances/VMs, so you don't have to worry about
  creating packages and building Docker images
- scale from a single function to thousands of parallel tasks

For more context, see the [project homepage](https://meadowrun.io), or our [case
studies](case_studies)
  
## Quickstart with AWS

First, install Meadowrun using Pip, Conda, or Poetry:

=== "Pip"
    ```shell
    pip install meadowrun
    ```
=== "Conda"
    ```shell
    conda install -c defaults -c conda-forge -c meadowdata meadowrun
    ```
=== "Poetry"
    ```shell
    poetry add meadowrun
    ```

Now install Meadowrun resources in your AWS account:

```shell
meadowrun-manage-ec2 install
```

Now as long as you have the AWS CLI configured and you have enough permissions, you can
run:

```python
import meadowrun
import asyncio

print(
    asyncio.run(
        meadowrun.run_function(
            lambda: sum(range(1000)) / 1000,
            meadowrun.AllocEC2Instance(),
            meadowrun.Resources(logical_cpu=4, memory_gb=32, max_eviction_rate=15),
            meadowrun.Deployment.mirror_local()
        )
    )
)
```

One caveat is that if you're using conda on a Windows or Mac, this won't work
because conda environments aren't cross-platform. If you're in this situation, when you
get to [Run a function](tutorial/run_function), you'll want to follow the link to [Run
a function from a git repo using Conda](tutorial/run_function_git_conda).

## Next steps

For a more in-depth tutorial on running your first job, start with [Installing
Meadowrun](tutorial/install).

Or for more background, read about [How Meadowrun works](explanation/how_it_works).
