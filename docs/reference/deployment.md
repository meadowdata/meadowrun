# Deployment options overview

This page gives quick examples of how to use the main deployment options in Meadowrun.
Also see [How deployment works](../../explanation/deployment) for a look at what's
happening under the covers.

{%
include-markdown "entry_points.md"
start="<!--quickstarted-start-->"
end="<!--quickstarted-end-->"
%}


## [`mirror_local`][meadowrun.Deployment.mirror_local]

`mirror_local` mirrors your local code and libraries. We won't go into further depth
here as all of the tutorials use `mirror_local` by default (when no deployment is
specified, `mirror_local` is used as the default).


## [`git_repo`][meadowrun.Deployment.git_repo]

This option runs using code committed to a Git repo rather than the local code. For this
example, we'll use our test repo at
[https://github.com/meadowdata/test_repo](https://github.com/meadowdata/test_repo).

The Git repo must specify the environment/libraries to run in.

=== "pip"
    For pip, you can run `pip freeze > requirements.txt` in an existing environment to
    produce a
    [`requirements.txt`](https://github.com/meadowdata/test_repo/blob/main/requirements.txt)
    file that you commit to your git repo.

    Then, you can run

    ```python    
    meadowrun.run_function(
        lambda: sum(range(1000)) / 1000,
        host,
        resources,
        deployment=meadowrun.Deployment.git_repo(
            # URL to the repo
            https://github.com/meadowdata/test_repo
            # name of our environment file
            interpreter=meadowrun.PipRequirementsFile("requirements.txt")
    )
    ```
=== "conda"
    For conda, you can run `conda env export > myenv.yml` in an existing environment to
    produce a [`myenv.yml`](https://github.com/meadowdata/test_repo/blob/main/myenv.yml)
    file that you commit to your git repo.

    Conda environments are not cross-platform, so you'll need to make sure `myenv.yml`
    describes a conda environment that can be built on Linux.

    Then, you can run

    ```python    
    meadowrun.run_function(
        lambda: sum(range(1000)) / 1000,
        host,
        resources,
        deployment=meadowrun.Deployment.git_repo(
            # URL to the repo
            https://github.com/meadowdata/test_repo
            # name of our environment file
            interpreter=meadowrun.CondaEnvironmentYmlFile("myenv.yml")
    )
    ```
=== "poetry"
    For poetry, you will usually already have committed the
    [pyproject.toml](https://github.com/meadowdata/test_repo/blob/main/pyproject.toml) and
    [poetry.lock](https://github.com/meadowdata/test_repo/blob/main/poetry.lock) files to
    your git repo.

    Then, you can run

    ```python    
    meadowrun.run_function(
        lambda: sum(range(1000)) / 1000,
        host,
        resources,
        deployment=meadowrun.Deployment.git_repo(
            # URL to the repo
            https://github.com/meadowdata/test_repo
            # name of our environment file
            interpreter=meadowrun.PoetryProjectPath("")
    )
    ```

Meadowrun will create an environment based on the file we specify and run the specified
command in that environment.

In most cases, the environment must have meadowrun installed as a dependency.

<!--gitrepoauth-start-->
For git repos that require authentication, see [Use a private git repo on
AWS](../../how_to/private_git_repo_aws) or [Use a private git repo on
Azure](../../how_to/private_git_repo_azure).
<!--gitrepoauth-end-->


## [`container_image`][meadowrun.Deployment.container_image]

In some cases, you might have all of the code and libraries you want already built in a
container (for combining `mirror_local` or `git_repo` with a container image, see [How
deployment works](../../explanation/deployment)).

Here's an example:

```python    
meadowrun.run_command(
    "python --version",
    host,
    resources,
    deployment=meadowrun.Deployment.container_image(
        repository="python", tag="slim-bullseye"
    ),
)
```

The container must have `python` installed and on the path. In most cases, meadowrun
must be installed in the `python` that's on the path. (Although in this case, because
we're using a simple `run_command`, we don't need Meadowrun installed.)

<!--containerauth-start-->
For images that require authentication, [Use a private container on
AWS](../../how_to/private_container_aws), [Use a private container on
Azure](../../how_to/private_container_azure), or [Use a private container on
Kubernetes](../../how_to/private_container_kubernetes).
<!--containerauth-end-->