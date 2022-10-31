# How deployment works

This page gives a deep dive into how deployment in Meadowrun works. For a quick overview
and examples, see the [Meadowrun deployment overview](../../reference/deployment).

Meadowrun makes it easy to get your **code** and libraries, which we'll refer to as your
**interpreter**, into the cloud.

The main entry points ([`run_function`][meadowrun.run_function],
[`run_map`][meadowrun.run_map]) take a [`Deployment`][meadowrun.Deployment] object. The
recommended way to construct a `Deployment` object is to use
[`Deployment.mirror_local`][meadowrun.Deployment.mirror_local] or
[`Deployment.git_repo`][meadowrun.Deployment.git_repo].


## [`mirror_local`][meadowrun.Deployment.mirror_local]

Use `mirror_local` when you want to replicate your local machine's environment on the
cloud. By default, this function will zip up all of your code in `sys.path` (excluding
things that are part of the interpreter), and unzip on the remote machine. You can also
add paths explicitly. Meadowrun zips all python files in these paths and uploads them to
S3. The workers then unpack the zip file and add the paths back to `sys.path` in the
right order.

For the interpreter, by default, this function will first detect what kind of
interpreter you're using (conda, poetry, or pip), generate or pick up a "lock" file
(i.e. `conda env export`, `pip freeze` or `poetry.lock`), and send that to the remote
machine so that an identical environment can be recreated on the other end.

One alternative is you can tell `mirror_local` to mirror any interpreter on your machine
even if it's not the currently running one by specifying e.g.
`interpreter=LocalCondaEnv(name_of_conda_env)`.

Another alternative is to tell `mirror_local` to create an interpreter based on an
environment specification file (`environment.yml` for conda, `requirements.txt` for pip,
or `poetry.lock` for poetry), on your machine, e.g.
`interpreter=PipRequirementsFile("envs/requirements.txt")`

For these options, the Meadowrun worker will create a docker image to replicate your
environment, and this image will get cached in a Meadowrun-managed AWS/Azure container
registry. (One of the AWS Lambdas/Azure Functions that run periodically will clean up
images that haven't been used recently.)

Finally, you can use `interpreter=ContainerInterpreter(repository_name)` to tell
Meadowrun to use a pre-built container rather than any local environment on your
machine. In this case, the Meadowrun worker will pull the specified image from the
specified container registry.

{%
include-markdown "../reference/deployment.md"
start="<!--containerauth-start-->"
end="<!--containerauth-end-->"
%} With this option, your local
code will still be available on the remote machine as well.

If you're not running on a Linux machine, Meadowrun will only be able to replicate local
pip and poetry environments on the remote machine--conda environments are
platform-specific. If you're using conda on a Linux machine, you'll need to specify an
environment specification file or a container for your interpreter.


## [`Deployment.git_repo`][meadowrun.Deployment.git_repo]

Use `git_repo` when you want to run with code that's been deployed to a git repo. The
Meadowrun worker will pull the specified branch/commit.

{%
include-markdown "../reference/deployment.md"
start="<!--gitrepoauth-start-->"
end="<!--gitrepoauth-end-->"
%}


This function requires you to specify the type of interpreter you want to use with your
git repo.

One option is if your git repo contains an environment specification file
(`environment.yml` for conda, `requirements.txt` for pip, or `poetry.lock` for poetry),
you can point Meadowrun to that environment specification file and Meadowrun will create
an environment based on that file on the remote machine. E.g.
`interpreter=CondaEnvironmentFile("path/in/repo/environment.yml")`

In this case, the Meadowrun worker will create a docker image to replicate your
environment, and this image will get cached in a Meadowrun-managed AWS/Azure container
registry. (One of the AWS Lambdas/Azure Functions that run periodically will clean up
images that haven't been used recently.)

You can also use `interpreter=ContainerInterpreter(repository_name)` to use a pre-built
container. In this case, the Meadowrun worker will pull the specified image from the
specified container registry.

{%
include-markdown "../reference/deployment.md"
start="<!--containerauth-start-->"
end="<!--containerauth-end-->"
%}


## Cross-platform compatibility

If you're not running on Linux (i.e. Windows or Mac), you might run into issues with
cross-platform compatibility.

Pip and poetry environments will usually work as-is across platforms, but there are
cases where a package's wheels have only been built for one system but not the other. We
recommend using [pip environment
markers](https://stackoverflow.com/questions/16011379/operating-system-specific-requirements-with-pip)
or [poetry's platform
keyword](https://stackoverflow.com/questions/61052866/python-poetry-how-to-specify-platform-specific-dependency-alternatives)
to install packages only when they're available for the platform you're running on.

Conda environments generally need to be entirely re-built for different platforms. We
recommend maintaining multiple environment files, like `environment-windows.yml` and
`environment-linux.yml` that you keep in sync manually. Then you can specify
`interpreter=CondaEnvironmentFile("environment-linux.yml")` for Meadowrun, but still use
`environment-windows.yml` on your local machine.
