# ![Meadowrun](meadowrun-logo-full.svg)

![PyPI - Python Version](https://img.shields.io/pypi/pyversions/meadowrun) ![PyPI](https://img.shields.io/pypi/v/meadowrun)  ![PyPI - Downloads](https://img.shields.io/pypi/dm/meadowrun) ![Conda](https://img.shields.io/conda/v/meadowdata/meadowrun) ![Conda](https://img.shields.io/conda/dn/meadowdata/meadowrun?label=conda%20downloads)

[![Join the chat at https://gitter.im/meadowdata/meadowrun](https://badges.gitter.im/meadowdata/meadowrun.svg)](https://gitter.im/meadowdata/meadowrun?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)



Meadowrun is a library for data scientists and data engineers who run python code on
AWS, Azure, or Kubernetes. Meadowrun:

- scales from a single function to thousands of distributed tasks.
- syncs your local code and libraries for a faster, easier iteration loop. Edit your
  code and rerun your analysis without worrying about building packages or Docker
  images.
- optimizes for cost, choosing the cheapest instance types and turning them off when
  they're no longer needed.
  
For more context, see our [case
studies](https://docs.meadowrun.io/en/stable/case_studies/) of how Meadowrun is used in
real life, or see the [project homepage](https://meadowrun.io)

To get started, go to our [documentation](https://docs.meadowrun.io), or [join the chat
on Gitter](https://gitter.im/meadowdata/meadowrun)

## Quickstart

First, install Meadowrun using pip:

```
pip install meadowrun
```

Next, assuming you've [configured the AWS
CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html)
and are a root/administrator user, you can run:

```python
import meadowrun
import asyncio

print(
    asyncio.run(
        meadowrun.run_function(
            lambda: sum(range(1000)) / 1000,
            meadowrun.AllocEC2Instance(),
            meadowrun.Resources(logical_cpu=1, memory_gb=8, max_eviction_rate=80),
            meadowrun.Deployment.mirror_local()
        )
    )
)
```

[The documentation](https://docs.meadowrun.io) has examples of how to use other package
managers (conda, poetry), and other platforms (Azure, GKE, Kubernetes).
