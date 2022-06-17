# How it works

## Terminology

The core functionality workflow of Meadowrun is to run a **job** from a **client** (e.g.
your laptop), via [run_function][meadowrun.run_function],
[run_command][meadowrun.run_command], or [run_map][meadowrun.run_map]. The job will run
on a **worker** which is usually on an **instance** (i.e. a remote machine). A single
instance can have one or more workers on it.


## Account
Meadowrun operates entirely in your AWS/Azure account and does not depend on any
external services. The Meadowrun client uses the credentials stored by the AWS/Azure
command line tools and so does not require any additional configuration to manage/access
your AWS/Azure account.


## Allocating jobs to instances

When you run a job, Meadowrun first looks for any existing instances that can
accommodate the CPU/memory requirements you specify. In order to do this, Meadowrun
keeps track of all Meadowrun-managed instances in a DynamoDB/Azure Table, which we'll
call the **instance allocation** table. This table also keeps track of what jobs have
been allocated to each instance and how much CPU/memory is available on each instance.
When you start a job, the Meadowrun client updates the instance allocation table to
reflect the allocation of that job to the chosen instance(s). When a job completes
normally, the worker tries to update the instance allocation table. If the worker
crashes  before it's able to do this, every instance has a **job deallocation** cron job
that reconciles what the instance allocation table says is running on that instance
compared to the actual processes that are running on that instance. (Each worker writes
a PID file so that the job deallocation cron job can correlate the job ids in the
instance allocation table with the process ids on the machine.)


## Starting and stopping instances

If there aren't enough instances to run your job, Meadowrun will launch one or more
instances for you, choosing the cheapest instance types that meet the CPU/memory
requirements you specify. Meadowrun will optionally choose spot instances, taking into
account the maximum interruption probability that you specify.

`meadowrun-manage-<cloud-provider> install`, will create AWS Lambdas/Azure Functions
that run periodically and adjust the number of running instances. Currently, this will
just terminate and deregister any instances that have been idle for more than 30
seconds, but in the future, we plan to support more complex policies.


## Client/worker communication

The Meadowrun client launches workers on remote machines/instances via SSH, and the
client/worker send data using files that are copied via SCP. On first run, the Meadowrun
client generates a private SSH key and stores it as an AWS Secret/Azure Secret. When the
Meadowrun client launches instances, it sets up the corresponding public key in the
instance so that it is able to SSH into the instance.

Each instance is launched with a Meadowrun image that has the Meadowrun worker
pre-installed, so the client just needs to run the `meadowrun-local` on the instance to
launch the worker. Each worker only lives for a single job.


## Deployment

Every job specifies a deployment, which is made up of an **interpreter** and optionally
**code**. The interpreter determines the python version and what libraries are
installed, and the code is "your code".

See [Deploy your code and libraries using Meadowrun](/explanation/deployment) for
details on how to specify your code and libraries deployments.
  

### Credentials

Some deployments require credentials to e.g. pull a docker container image or a private
git repo. You can upload these credentials, e.g. a username/password or an SSH key, as
an AWS/Azure Secret, and provide Meadowrun with the name of the secret, and the
Meadowrun worker will get the credentials from the secret.

See:

- [Use a private git repo on AWS](/how_to/private_git_repo_aws)
- [Use a private git repo on Azure](/how_to/private_git_repo_azure)
- [Use a private container on AWS](/how_to/private_container_aws)
- [Use a private container on Azure](/how_to/private_container_azure)


## Map jobs and tasks

Map jobs have one or more tasks, and one or more workers. Each worker can execute one or
more tasks, sequentially. In addition to the normal client/worker communication to start
the worker, the client needs a way to send tasks to each worker, get results back, and
either tell the worker to shut down or send it another task. We use AWS SQS/Azure Queues
for this purpose. There is one request queue that the client uses to send tasks to
workers, and a result queue that the workers use to send results back to the client. A
new pair of queues is created for each map job. (One of the AWS Lambdas/Azure Functions
that run periodically will clean up queues that haven't been used recently.)
