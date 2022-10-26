# Run a Python function on Azure VMs

This page walks you through running a function on an Azure VM using your local code and
libraries.


## Prerequisites

- If you haven't set up the Azure CLI, [install the Azure
  CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and then
  [login](https://docs.microsoft.com/en-us/cli/azure/get-started-with-azure-cli#how-to-sign-into-the-azure-cli).


- Install Meadowrun

    {%
    include-markdown "../index.md"
    start="<!--install-start-->"
    end="<!--install-end-->"
    %}

  
- Finally, set up [Meadowrun resources](../../reference/azure_resources) in your Azure account.

    ```
    > poetry run meadowrun-manage-azure-vm install
    Creating resources for running meadowrun
    Waiting for a long-running Azure operation (5s)
    Waiting for a long-running Azure operation (5s)
    Waiting for a long-running Azure operation (5s)
    Waiting for a long-running Azure operation (5s)
    Waiting for a long-running Azure operation (5s)
    Created resources in 49.69 seconds
    ```

    You will need to run this as a root/Administrator account.
    
    If you decide to stop using Meadowrun, it's easy to [uninstall Meadowrun](../../how_to/uninstall).


## Write a Python script to run a function remotely

Create a file called `mdr.py`:

```python
import asyncio
import meadowrun

if __name__ == "__main__":
    result = asyncio.run(
        meadowrun.run_function(
            # the function to run remotely
            lambda: sum(range(1000)) / 1000,
            # run on a dynamically allocated Azure VM
            host=meadowrun.AllocAzureVM(),
            # requirements to choose an appropriate Azure VM
            resources=meadowrun.Resources(
                logical_cpu=1, memory_gb=4, max_eviction_rate=80
            ),
        )
    )
    print(f"Meadowrun worked! Got {result}")
```

## Run the script

Assuming you saved the file above as mdr.py:

```
> python -m mdr 
Provisioned virtual machine 666ae778-5fe7-47e4-b4e4-f068865e082c-vm (20.85.227.52)
Launched a new instance for the job: 20.85.227.52: Standard_D2as_v4 (2.0 CPU, 8.0 GB),
spot ($0.0096/hr, 2.5% eviction rate), will run 1 workers
Running job on 20.85.227.52 /var/meadowrun/job_logs/lambda.d33ad94a-dffb-46bf-b24c-0ebd0517e263.log
Meadowrun worked! Got 499.5
```

The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:

- Based on the options specified in [`Resources`][meadowrun.Resources] and
  [`AllocAzureVM`][meadowrun.AllocAzureVM], `run_function` will launch the
  cheapest Azure VM type that has at least 1 CPU and 4 GB of memory, and a <80% chance of
  being evicted. You can set this last parameter to 0 to exclude spot instances and only
  use on-demand instances. The exact instance type chosen depends on current prices,
  but in this case Meadowrun has selected a Standard_D2as_v4 spot instance, and we're paying
  $0.0096/hr for it.

- We didn't provide the `deployment` parameter, which is equivalent to specifying
  `deployment=`[`meadowrun.Deployment.mirror_local`][meadowrun.Deployment.mirror_local].
  This tells Meadowrun to copy your local environment and code to the provisioned VM.
  Meadowrun detects what kind of environment (conda, pip, or poetry) you're currently in
  and calls the equivalent of `pip freeze` to capture the libraries installed in the
  current environment. The VM will then create a new Docker image based on the list of
  libraries and cache the image for reuse in a Meadowrun-managed Azure Container Registry
  (ACR) container registry. Meadowrun will also zip up your local code and send it to the
  Azure VM.

- Meadowrun runs the specified function in that environment on the remote machine and
  returns the result.

- When we ran `meadowrun-manage-azure-vm install` earlier on this page, that set up an Azure
  Function that runs periodically and turns off unused VMs. If the VM we started is not
  reused for a few minutes, it will be deleted automatically.


You can also log or print to stdout in your remote function--Meadowrun will copy the
remote output to the local output.



## Next steps

- In addition to `run_function`, Meadowrun provides [other entrypoints](../../reference/entry_points).
  The most important of these is [`run_map`][meadowrun.run_map], which allows you to use
  many VMs in parallel.

{%
include-markdown "aws_ec2.md"
start="<!--aws-azure-generic-next-steps-start-->"
end="<!--aws-azure-generic-next-steps-end-->"
%}
