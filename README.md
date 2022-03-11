# Meadowrun 

Meadowrun makes developing, experimenting, and deploying your python code on the cloud
as easy and frictionless as working locally.

```python
from meadowrun import run_function, EC2AllocHost
await run_function(
    lambda: analyze_stuff(a_big_file),
    EC2AllocHost(
        logical_cpu_required=4,
        memory_gb_required=32,
        interruption_probability_threshold=15))
```

`run_function` will launch the cheapest EC2 instance type that has at least 4 CPU and
32GB of memory, and a <15% chance of being interrupted (you can set this to 0 to exclude
spot instances and only use on-demand instances).

Then, Meadowrun will package up your current libraries and code (not fully implemented),
copy them onto the newly launched instance, and run your function.

Meadowrun will also fully manage the EC2 instances it creates, reusing them for
subsequent jobs and terminating them after a configurable idle timeout.

Meadowrun also provides a distributed `map`:

```python
from meadowrun import run_map, EC2AllocHosts
await run_map(
    lambda file_name: analyze_stuff(file_name),
    [file1, file2, ...],
    EC2AllocHosts(4, 32, 15)
)
```
