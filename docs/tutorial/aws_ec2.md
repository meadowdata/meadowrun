# Run Meadowrun on AWS EC2

This page walks you through running a function on an EC2 instance using your local code
and libraries.


## Prerequisites

- If you haven't set up the AWS CLI,
  [install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
  and [configure
  it](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

- Install Meadowrun

    {%
    include-markdown "../index.md"
    start="<!--install-start-->"
    end="<!--install-end-->"
    %}

- Finally, set up [Meadowrun resources](../../reference/aws_resources) in your AWS account.

    ```
    > meadowrun-manage-ec2 install --allow-authorize-ips
    Creating resources for running meadowrun
    Created/updated user group meadowrun_user_group. Add users to this group to allow them
    to use Meadowrun with `aws iam add-user-to-group --group-name meadowrun_user_group
    --user-name <username>`
    Created resources in 27.18 seconds
    ```

    You will need to run this as a root/Administrator account.
    
    In this example, we set `--allow-authorize-ips` which reduces the configuration we need
    to do but is slightly less secure. You can rerun this command later without this option
    to switch to the more secure configuration.
    
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
            # run on a dynamically allocated AWS EC2 instance
            host=meadowrun.AllocEC2Instance(),
            # requirements to choose an appropriate EC2 instance
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
Size of pickled function is 40
Job was not allocated to any existing EC2 instances, will launch a new EC2 instance
Launched a new EC2 instance for the job: ec2-18-216-7-235.us-east-2.compute.amazonaws.com:
t2.medium (2 CPU/4.0 GB), spot ($0.0139/hr, 2.5% eviction rate), will run 1 job/worker
Meadowrun worked! Got 499.5
```

The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:

- Based on the options specified in [`Resources`][meadowrun.Resources] and
  [`AllocEC2Instance`][meadowrun.AllocEC2Instance], `run_function` will launch the
  cheapest EC2 instance type that has at least 1 CPU and 4 GB of memory, and a <80%
  chance of being evicted. You can set this last parameter to 0 to exclude spot instances
  and only use on-demand instances. The exact instance type chosen depends on current EC2
  prices, but in this case Meadowrun has selected a t2.medium spot instance, and we're
  paying $0.0139/hr for it.

- We didn't provide the `deployment` parameter, which is equivalent to specifying
  `deployment=`[`meadowrun.Deployment.mirror_local`][meadowrun.Deployment.mirror_local].
  This tells Meadowrun to copy your local environment and code to the provisioned EC2
  instance. Meadowrun detects what kind of environment (conda, pip, or poetry) you're
  currently in and calls the equivalent of `pip freeze` to capture the libraries installed
  in the current environment. The EC2 instance will then create a new Docker image based
  on the list of libraries and cache the image for reuse in a Meadowrun-managed AWS ECR
  container registry. Meadowrun will also zip up your local code and send it to the EC2
  instance.

- Meadowrun runs the specified function in that environment on the remote machine and
  returns the result.

- When we ran `meadowrun-manage-ec2 install` earlier on this page, that set up an AWS
  Lambda that runs periodically and turns off unused EC2 instances. If the EC2 instance we
  started is not reused for a few minutes, it will be terminated automatically by this
  Lambda.


You can also log or print to stdout in your remote function--Meadowrun will copy the
remote output to the local output.


## Next steps

- In addition to `run_function`, Meadowrun provides [other entrypoints](../entry_points).
  The most important of these is [`run_map`][meadowrun.run_map], which allows you to use
  many EC2 instances in parallel.
<!--aws-azure-generic-next-steps-start-->
- By default, Meadowrun will mirror your current local deployment, but there are [other
  ways to specify the code and libraries](../deployment) you want to use when running
  remotely.
- If you want non-administrator users to use Meadowrun, you'll need to [grant them
  permissions to use Meadowrun](../../how_to/user_permissions).
- For most tasks, you'll need to do a bit of configuration to be able to [access
  resources from Meadowrun jobs](../../how_to/access_resources)
<!--aws-azure-generic-next-steps-end--->