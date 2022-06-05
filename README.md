# ![Meadowrun](meadowrun-logo-full.svg)

Meadowrun automates the tedious details of running your python code on AWS. Meadowrun
will
- choose the optimal set of EC2 on-demand or spot instances and turn them off when
  they're no longer needed
- deploy your code and libraries to the EC2 instances, so you don't have to worry about
  creating packages and building Docker images
- scale from a single function to thousands of parallel tasks

For more information see Meadowrun's
[documentation](https://docs.meadowrun.io/en/latest/), or the [project
homepage](https://meadowrun.io).

## When would I use Meadowrun?

- You want to run distributed computations in python in AWS with as little fuss as
  possible. You don't want to build docker images as part of your iteration cycle, and
  you want to be able to take advantage of spot pricing without having to manage a
  cluster.
- You can't/don't want to run tests or analysis on your laptop and you want a better
  experience than SSHing into an EC2 machine.

## Quick demo

First, install meadowrun:

```
> conda install -c defaults -c conda-forge -c meadowdata meadowrun
```

Next, make sure you've [configured the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).

Now you can run:

```python
from meadowrun import run_function, AllocCloudInstance, Deployment
await run_function(
    lambda: sum(range(1000)) / 1000,
    AllocCloudInstance(
        logical_cpu_required=4,
        memory_gb_required=32,
        interruption_probability_threshold=15,
        cloud_provider="EC2"
    ),
    Deployment.git_repo(
        "https://github.com/meadowdata/test_repo",
        conda_yml_file="myenv.yml"
    )
)
```
