# Documentation fragments

Under construction

## Deployment

- `interpreter_deployment: Union[InterpreterDeployment, VersionedInterpreterDeployment]`
- `code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None]`

Because we are distributing our function to remote machines, we need to tell meadowrun what interpreter to run with on the remote machines, and we might also want to include additional code that's not part of the interpreter.

Examples of `interpreter_deployment` are:
- `ContainerAtTag(repository="meadowrun", tag="latest")`: a tag in a container repo (tag defaults to `latest`). The container should be configured so that `docker run meadowrun:latest python [additional arguments]` behaves as expected. Note that this is not deterministic (tags can resolve to different digests over time).
- `ContainerAtDigest(repository="gcr.io/my_org/my_image", digest="sha256:76eaa9e5bd357d6983a88ddc9c4545ef4ad64c50f84f081ba952c7ed08e3bdd6")`: a specific digest within a container repo
- `PoetryProject()` (not implemented): a pyproject.toml and poetry.lock file in the `code_deployment` that specifies versions of the interpreter and libraries
- `CondaEnv()` (not implemented): a condaenv.yml file in the `code_deployment` that specifies versions of the interpreter and libraries
- `PipRequirements()` (not implemented): a requirements.txt file in the `code_deployment` that specifies versions of the interpreter and libraries
- `ServerAvailableInterpreter(interpreter_path="/deployments/my_code/venv/bin/python")`: a python interpreter that already exists on the agent machines. You are responsible for installing/managing the referenced interpreter
- `ServerAvailableInterpreter(interpreter_path=meadowrun.config.MEADOWRUN_INTERPRETER)`: the python interpreter that the agent is running with. Should not be used except for tests.

Examples of `code_deployment` are:
- `GitRepoBranch(repo_url="https://github.com/meadowdata/test_repo", branch="main")`: a branch on a git repo (the branch defaults to `main`). Note that this is not deterministic (branches can resolve to different commits over time).
- `GitRepoCommit(repo_url="https://github.com/meadowdata/test_repo", commit="d44155a28cdcb171e6bad5090a787e9e15640663")`: a specific commit in a git repo
- `ServerAvailableFolder(code_paths=["/deployments/my_code"])`: typically a shared folder that the agent has access to via the machine's file system. You are responsible for populating/managing the referenced folders.
- `None`: no additional code is needed beyond the interpreter.

`function` can reference any library or code provided by `code_deployment` and `interpreter_deployment`.


## Credentials

Deployments like `ContainerAtTag` or `GitRepoBranch` may reference private repositories that require credentials to access. Credentials are added to a coordinator in a separate call, like:

Credentials must be set up in a way that meadowrun can understand--meadowrun will use to perform actions for you, like pulling the latest version of a private Docker container image from a Docker registry like DockerHub.

A credentials source is specified by:

- The service, which tells meadowrun how to use this secret. Currently supported: "DOCKER", "GIT"
- The service url
  - For Docker, this is the URL of the Docker registry. `registry-1.docker.io` is the "default" Docker registry used when there is no domain specified. E.g. `docker pull ubuntu` is equivalent to `docker pull registry-1.docker.io/ubuntu`.
  - For Git, this is the URL of the git server, e.g. "git@github.com"
- The credentials source:
  - `AwsSecret("name_of_aws_secret")`: A secret stored in AWS Secrets Manager that the coordinator can access that has a "username" key and a "password" key.
  - `ServerAvailableFile("/path/to/credentials.txt")`: A file that the coordinator can access that has a username on the first line and a password on the second line. 
