# Entry points overview

This page gives quick examples of how to use each of Meadowrun's entry points.

<!--quickstarted-start-->
We will assume you've gone through either the quickstart or one of the tutorials for a
specific platform ([AWS EC2](../../tutorial/aws_ec2), [Azure VMs](../../tutorial/azure_vm),
[GKE](../../tutorial/gke), or [Kubernetes](../../tutorial/kubernetes)). In the examples, we'll use
`host` and `resources` variables that we don't define here. You will need to define
these variables as they were defined (as parameters) in the platform-specific tutorials.
<!--quickstarted-end-->


## [`run_function`][meadowrun.run_function]

Runs a function remotely. Example:

```python
meadowrun.run_function(lambda: sum(range(1000)) / 1000, host, resources)
```

Returns `499.5`.

## [`run_command`][meadowrun.run_command]

Runs a command remotely. See [Run a Jupyter notebook remotely](../../tutorial/jupyter_notebook) for
a fully worked example of using `run_command`.

## [`run_map`][meadowrun.run_map]

`run_map` is like the built-in
[`map`](https://docs.python.org/3/library/functions.html#map) function, but it runs in
parallel and remotely. This is a powerful tool for scaling computations: easily use
hundreds or thousands of cores in parallel to process large data sets.

Example:

```python
meadowrun.run_map(lambda x: x ** x, [2, 3, 4, 5], host, resources)
```

In this case, resources applies per task.

The output is:

```
[4, 27, 256, 3125]
```

With `run_map`, you will no longer see the standard output from the remote processes
locally. To see the logs from your processes you'll want to look at [Stream logs on AWS
or Azure](../../how_to/read_logs).

The Kubernetes implementation of `run_map` currently has a major limitation which is
that the tasks are assigned to workers at the start of the job. This means that if
you have tasks where some take a long time and some finish quickly, the distribution of
tasks will be suboptimal.


## [`run_map_as_completed`][meadowrun.run_map_as_completed]

`run_map_as_completed` is similar to `run_map` but it returns an `AsyncIterable` rather
than as simple list of results. Results will be returned as tasks complete. Example:

```python
async for result in await meadowrun.run_map_as_completed(
    lambda x: x ** x, [2, 3, 4, 5], host, resources
):
    print(result.result_or_raise())
```

The output is:

```
27
4
256
3125
```

In general, there's no guarantee what order the results come back in.

`run_map` is recommended for most use cases. Consider using `run_map_as_completed` if
your use case requires further processing of results locally, and you'd like to
interleave this processing with executing the tasks. This could be beneficial if the
processing is time-intensive.
