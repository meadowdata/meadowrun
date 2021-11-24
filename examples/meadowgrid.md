# meadowgrid

meadowgrid is a cluster manager with built-in support for running batch jobs (from meadowflow or other job schedulers) as well as running distributed compute jobs on the same pool of resources.

## Key concepts

There are two kinds of meadowgrid servers, the [coordinator](/src/meadowgrid/coordinator.py) and the [job workers](/src/meadowgrid/job_worker.py). These servers can be launched by running `meadowgrid_coordinator` or `meadowgrid_job_worker`, respectively, on the command line after the meadowdata package has been installed.

The job workers can run "simple jobs" which run a single function like `MeadowGridFunction.from_name("my_module", "my_function")` or a command line like `MeadowGridCommand(["jupyter", "nbconvert", "my_notebook.ipynb", "--to", "html"])`. Simple jobs are usually scheduled by a job scheduler like meadowflow. Job workers can also run "grid jobs" where a single function is run over many inputs (aka tasks) in parallel, like `grid_map(my_function, [task1, task2, ...], ...)`. Grid jobs are usually started by a simple job, or from an engineer/researcher's development environment. The compute environment should have one or more machines running a single job worker each. The job workers will run one or more jobs on their machines depending on the resources available.

The coordinator (usually there will be just one) accepts jobs from clients and then assigns jobs/tasks to job workers. Clients and job workers never communicate directly.

> Implementation detail: The coordinator has two sets of APIs. Clients use `add_job` and `get_simple_job_states`/`get_grid_task_states` to request that jobs get run and check on their status. Job workers use `get_next_job`, `update_job_states`, etc. to get jobs to run and report the results.

## Quickstart: How to run on AWS

Under construction

## Quickstart: How to run on-premise

1. Configure a coordinator server. Requires installing the meadowdata package and then scheduling `meadowgrid_coordinator` to run as a service. Example of how to install on Ubuntu (also will work on Windows):
   - Install python 3.9 (currently the only tested version), create a virtualenv, and install meadowdata in it
     ```shell
     sudo apt update
     sudo apt install python3.9
     python3.9 -m venv /home/<USER>/meadowgrid_env
     /home/<USER>/meadowgrid_env/bin/pip install --extra-index-url https://test.pypi.org/simple meadowdata
     ```
   - Set up meadowgrid_coordinator as a systemd service by creating a file at `/etc/systemd/system/meadowgrid_coordinator.service` with the contents:
     ```
     [Unit]
     Description="meadowgrid coordinator"
     After=network.target
    
     [Service]
     User=<USER>
     Group=<GROUP>
     ExecStart=/home/<USER>/meadowgrid_env/bin/meadowgrid_coordinator --host 0.0.0.0
     Restart=always
    
     [Install]
     WantedBy=multi-user.target
     ```
   - Clients and job workers must be able to reach port 15319 on this host.
2. Configure one or more job workers. These should ideally not run on the same machine as the coordinator. Each machine only needs one job worker service (a single job worker will launch many jobs depending on the resources of the machine). Similar to the coordinator, but schedule `meadowgrid_job_worker` to run instead. Example of how to install on Ubuntu (also will work on Windows)
   - Optionally, [install Docker](https://docs.docker.com/engine/install/ubuntu/) if you are planning on using containers to run your jobs/tasks.
   - Same instructions as above to install python3.9 and create a virtual environment with meadowdata installed
   - Set up meadowgrid_job_worker as a systemd service by creating a file at `/etc/systemd/system/meadowgrid_coordinator.service` with the contents:
     ```
     [Unit]
     Description="meadowgrid job worker"
     After=network.target
    
     [Service]
     User=<USER>
     Group=<GROUP>
     ExecStart=/home/<USER>/meadowgrid_env/bin/meadowgrid_job_worker --coordinator-host <COORDINATOR_HOST>
     Restart=always
    
     [Install]
     WantedBy=multi-user.target
     ```
   
3. Run a grid job. From any machine that can access the coordinator:
   ```python
   import meadowgrid
   
   meadowgrid.grid_map(
       # this function will run on the job workers
       lambda x: x * 2,
       # we will run the above function on these 3 inputs/tasks
       [1, 2, 3],
       # this is a publicly available image with just meadowdata installed in it
       meadowgrid.ContainerRepo("hrichardlee/meadowdata"),
       # the coordinator host
       coordinator_host="<COORDINATOR_HOST>"
   )
   ```

## Deep dive on `grid_map` and `grid_map_async`

### function and args

- `function: Callable[[T], U]`
- `args: Iterable[T]`

At its core `grid_map(function, args, ...)` is just a distributed version of the built-in function `map(function, args)`. E.g. `grid_map(lambda x: x * 2, [1, 2, 3], ...) == map(lambda x: x * 2, [1, 2, 3]) == [2, 4, 6]` but `grid_map` parallelizes execution of the function to many machines.

`function` must be picklable using [cloudpickle](https://github.com/cloudpipe/cloudpickle) and each of `args` must be picklable using the standard library pickle.

### Deployment

- `interpreter_deployment: Union[InterpreterDeployment, VersionedInterpreterDeployment]`
- `code_deployment: Union[CodeDeployment, VersionedCodeDeployment, None] = None`

Because we are distributing our function to remote machines, we need to tell meadowgrid what interpreter to run with on the remote machines, and we might also want to include additional code that's not part of the interpreter.

Examples of `interpreter_deployment` are:
- `ContainerRepo("meadowdata")`: a tag in a container repo (tag defaults to `latest`). The container should be configured so that `docker run meadowdata:latest python [additional arguments]` behaves as expected.
- `ContainerAtDigest("gcr.io/my_org/my_image", "sha256:76eaa9e5bd357d6983a88ddc9c4545ef4ad64c50f84f081ba952c7ed08e3bdd6")`: a specific digest within a container repo
- `PoetryProject()` (not implemented): a pyproject.toml and poetry.lock file in the `code_deployment` that specifies versions of the interpreter and libraries
- `CondaEnv()` (not implemented): a condaenv.yml file in the `code_deployment` that specifies versions of the interpreter and libraries
- `PipRequirements()` (not implemented): a requirements.txt file in the `code_deployment` that specifies versions of the interpreter and libraries
- `ServerAvailableInterpreter(interpreter_path="/deployments/my_code/venv/bin/python")`: a python interpreter that already exists on the job worker machines. You are responsible for installing/managing the referenced interpreter
- `ServerAvailableInterpreter(interpreter_path=meadowgrid.config.MEADOWGRID_INTERPRETER)`: the python interpreter that the job worker is running with. Should not be used except for tests.

Examples of `code_deployment` are:
- `GitRepo("https://github.com/meadowdata/test_repo")`: a branch on a git repo (the branch defaults to `main`)
- `GitRepoCommit(repo_url="https://github.com/meadowdata/test_repo", commit="d44155a28cdcb171e6bad5090a787e9e15640663")`: a specific commit in a git repo
- `ServerAvailableFolder(code_paths=["/deployments/my_code"])`: typically a shared folder that the job worker has access to via the machine's file system. You are responsible for populating/managing the referenced folders.
- `None`: no additional code is needed beyond the interpreter.

`function` can reference any library or code provided by `code_deployment` and `interpreter_deployment`.

For grid jobs (but not for simple jobs), meadowdata must be available as a library.

### Resources

- `resources_required_per_task: Optional[Dict[str, float]] = None`

By default, jobs require `LOGICAL_CPU` and `MEMORY_GB` per the defaults in `DEFAULT_LOGICAL_CPU_REQUIRED` and `DEFAULT_MEMORY_GB_REQUIRED` in `meadowgrid.config`. You can override the defaults, e.g. `{meadowgrid.config.MEMORY_GB: 8}` will override `MEMORY_GB`, but `LOGICAL_CPU` will still be the default value. If you want to meadowgrid to ignore `LOGICAL_CPU` or `MEMORY_GB` requirements, then you can do something like `{meadowgrid.config.MEMORY_GB: 8, meadowgrid.config.LOGICAL_CPU: 0}`. In that case, meadowgrid will completely ignore `LOGICAL_CPU` when scheduling the job.

You can also require custom resources, e.g. `{"TEMPERATURE_SENSOR": 1}`. For custom resources, you will also need to tell the job workers what custom resources they have available, e.g. on the command line `meadowgrid_job_worker --available_resource TEMPERATURE_SENSOR 4`.

meadowgrid will not schedule jobs on a job worker unless all of the required resources are available.

### priority

- `priority: float = DEFAULT_PRIORITY`

If the currently requested jobs require more resources than are available on the existing job workers, then the order that meadowgrid schedules jobs will be based on their priority. E.g. if jobs 1, 2, and 3 each have priority 100, and job 4 has priority 300, meadowgrid will choose job 1 with probability 100/600, and the same for 2 and 3. meadowgrid will choose job 4 with probability 300/600. In other words, `priority` is how many raffle tickets that job gets, and meadowgrid will randomly choose one of the raffle tickets to decide which job to run next.

### Coordinator

- `coordinator_host: str = DEFAULT_COORDINATOR_HOST`
- `coordinator_port: int = DEFAULT_COORDINATOR_PORT`

These specify where to find the coordinator.

### grid_map_async

`grid_map_async` is the asyncio version of `grid_map`. Instead of returning `Sequence[U]`, `grid_map_async` returns `Sequence[Awaitable[U]]`, so the simplest usage is

```python
tasks = await grid_map_async(...)
results = await asyncio.gather(*tasks)
```

But more complicated usages are possible using the usual asyncio functionality, e.g. creating continuations that will run as each task finish, or processing results in the order that tasks finish (rather than in the order they were submitted).
