# meadowgrid

meadowgrid is a cluster manager with built-in support for running batch jobs (from meadowflow or other job schedulers) as well as running distributed compute jobs on the same pool of resources.

## Key concepts

### Coordinator/job worker and simple jobs/grid jobs

There are two kinds of meadowgrid servers, the [coordinator](/src/meadowgrid/coordinator.py) and the [job workers](/src/meadowgrid/job_worker.py). These servers can be launched by running `meadowgrid_coordinator` or `meadowgrid_job_worker`, respectively, on the command line after the meadowdata package has been installed.

The job workers can run "simple jobs" which run a single function like `MeadowGridFunction.from_name("my_module", "my_function")` or a command line like `MeadowGridCommand(["jupyter", "nbconvert", "my_notebook.ipynb", "--to", "html"])`. Simple jobs are usually scheduled by a job scheduler like meadowflow. Job workers can also run "grid jobs" where a single function is run over many inputs (aka tasks) in parallel, like `grid_map(my_function, [task1, task2, ...], ...)`. Grid jobs are usually started by a simple job, or from an engineer/researcher's development environment. The compute environment should have one or more machines running a single job worker each. The job workers will run one or more jobs on their machines depending on the resources available.

The coordinator (usually there will be just one) accepts jobs from clients and then assigns jobs/tasks to job workers. Clients and job workers never communicate directly.

> Implementation details: The coordinator has two sets of APIs. Clients use `add_job` and `get_simple_job_states`/`get_grid_task_states` to request that jobs get run and check on their status. Job workers use `get_next_job`, `update_job_states`, etc. to get jobs to run and report the results.

### Code and interpreter deployments

Both "simple jobs" and "grid jobs" need to define the python interpreter (and associated libraries) and code they need to run on. A `CodeDeployment` can be:
- `GitRepo`: a branch on a git repo
- `GitRepoCommit`: a specific commit in a git repo
- `ServerAvailableCode`: typically a shared folder that the job worker has access to via the machine's file system
- `None` in the case where the interpreter already has all the code it needs

An `InterpreterDeployment` can be:
- `ContainerRepo`: a tag in a container repo
- `ContainerAtDigest`: a specific digest within a container repo
- `PoetryProject` (not implemented): a pyproject.toml and poetry.lock file in the `CodeDeployment` that specifies versions of the interpreter and libraries
- `CondaEnv` (not implemented): a condaenv.yml file in the `CodeDeployment` that specifies versions of the interpreter and libraries
- `PipRequirements` (not implemented): a requirements.txt file in the `CodeDeployment` that specifies versions of the interpreter and libraries
- `ServerAvailableInterpreter`: a python interpreter installed on the job worker's machine out-of-band

The job workers take care of downloading and setting up the `CodeDeployment` and `InterpreterDeloyment`. As an example:

```python
meadowgrid.grid_map(my_function, [task1, task2, ...], GitRepo("https://github.com/meadowdata/test_repo"), PipRequirements())
```

As long as there's a requirements.txt file in the repo, `my_function` can reference any code in the git repo, as well as any libraries referenced by the requirements.txt file.

## How to run on AWS

Under construction

## How to run on-premise

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
       # we don't need any additional code for this example
       meadowgrid.ServerAvailableFolder(),
       # this is a publicly available image with just meadowdata installed in it
       meadowgrid.ContainerRepo("hrichardlee/meadowdata"),
       # the coordinator host
       "<COORDINATOR_HOST>"
   )
   ```
