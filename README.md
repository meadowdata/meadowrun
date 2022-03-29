# Meadowrun 

Meadowrun makes developing and experimenting with your python code on the cloud as easy
and frictionless as working locally.


## Getting started (`run_function`)

First, install meadowrun, e.g.

```
> conda install meadowrun
```

And make sure you've [configured the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).


Now you can run:

```python
from meadowrun import run_function, EC2AllocHost, Deployment
await run_function(
    lambda: analyze_stuff(a_big_file),
    EC2AllocHost(
        logical_cpu_required=4,
        memory_gb_required=32,
        interruption_probability_threshold=15),
    Deployment.git_repo(
        "https://github.com/meadowdata/test_repo",
        conda_yml_file="myenv.yml"
    )
)
```

Based on the options specified in `EC2AllocHost`, `run_function` will launch the
cheapest EC2 instance type that has at least 4 CPU and 32GB of memory, and a <15% chance
of being interrupted (you can set this to 0 to exclude spot instances and only use
on-demand instances).

Then, based on the options specified in `Deployment.git_repo`, `run_function` will grab
code from the `main` branch of the `test_repo` git repo, and then create a conda
environment (in a container) using the `myenv.yml` file in the git repo as the
environment specification (assumes that `myenv.yml` was created using
`conda env export > myenv.yml`). Creating the conda environment takes a while, but once
it has been created, it gets cached and reused using AWS ECR.

Finally, meadowrun will run `analyze_stuff(a_big_file)` (or whatever code you specify)
in that environment on the remote machine.

### Cleaning up EC2 instances

Meadowrun uses regularly scheduled lambdas to automatically manage the EC2 instances it
creates, reusing them for subsequent jobs and terminating them after a configurable idle
timeout. In order for Meadowrun's automatic management to work, you need to run a
one-time command to set up the lambdas in your AWS account:

```> meadowrun-manage install```

### Distributed map

Meadowrun also provides a distributed `map`:

```python
from meadowrun import run_map, EC2AllocHosts, Deployment
await run_map(
    lambda file_name: analyze_stuff(file_name),
    [file1, file2, ...],
    EC2AllocHosts(4, 32, 15),
    Deployment.git_repo(
        "https://github.com/meadowdata/test_repo",
        conda_yml_file="myenv.yml"
    )
)
```

This has roughly the same semantics as the python built-in function `map(analyze_stuff,
[file1, file2, ...])` but it runs distributed.

### Private git repos

The examples above will only work with a publicly accessible git repo. To use a private
git repo, you'll need to give Meadowrun the name of an AWS secret that contains the
private SSH key for the repo you want to use:

```python
Deployment.git_repo(
    "https://github.com/meadowdata/test_repo",
    conda_yml_file="myenv.yml",
    ssh_key_aws_secret="my_ssh_key"
)
```

This pulls the AWS secret at runtime, so you also need to grant access to the secret to
the EC2 role that meadowrun runs as:

```
> meadowrun-manage grant-permission-to-secret my_ssh_key
```
