# Run a Jupyter notebook remotely

This page walks through an example of how to run a Jupyter notebook server using
Meadowrun.

{%
include-markdown "entry_points.md"
start="<!--quickstarted-start-->"
end="<!--quickstarted-end-->"
%}

## Run the Jupyter notebook server

Let's assume you have an environment with jupyter and meadowrun installed, e.g.

```shell
pip install jupyter meadowrun
```

Then, you can use Meadowrun to launch a Jupyter notebook server on an EC2 instance:

```python
meadowrun.run_command(
    "jupyter notebook --ip 0.0.0.0 --port 8888 --allow-root",
    host,
    resources,
    meadowrun.Deployment.mirror_local(globs="**/*.ipynb"),
    ports=8888
)
```

In prior examples, we haven't seen the
[`mirror_local`][meadowrun.Deployment.mirror_local] parameter specified explicitly
because that is the default parameter for `deployment`. In this case, though, we want to
specify an option: `globs="**/*.ipynb`. By default, `mirror_local` will only copy `.py`
files. The `globs` option allows us to tell Meadowrun to sync other files, in this case
Jupyter notebook files, along with our local code to the remote machine.

The `ports=8888` parameter tells Meadowrun to open the specified port on the EC2
instance/Azure VM/Kubernetes pod that we're running on.


## Connect to the notebook server

To connect to the notebook server, first we'll need the address of the remote machine.
We can find this in the standard output:

=== "AWS"
    ```
    Running job on ec2-3-16-156-255.us-east-2.compute.amazonaws.com ...
    ```

    Now we can navigate to e.g.
    `http://ec2-3-16-156-255.us-east-2.compute.amazonaws.com:8888/` in a web browser
=== "Azure"
    ```
    Running job on 20.85.216.122 ...
    ```

    Now we can navigate to e.g. `http://20.85.216.122:8888/` in a web browser
=== "Kubernetes/GKE"
    ```
    Waiting for pod mdr-reusable-c8902976-03f6-4688-ba0b-2cc7bc285291-0-tdhgf to start running: ContainerCreating (pulling image)
    ```

    For Kubernetes, Meadowrun doesn't create a Kubernetes Service to make the pod accessible
    from outside of the cluster. So we'll need to run
    
    ```shell
    kubectl port-forward pods/mdr-reusable-c8902976-03f6-4688-ba0b-2cc7bc285291-0-tdhgf 8888:8888
    ```

    Now we can navigate to e.g. `http://localhost:8888/` in a web browser

This will bring up the Jupyter notebook UI which will ask us for a token. We can also
find this token in the standard output:

```
Task worker:     Or copy and paste one of these URLs:
Task worker:         http://0ac4f68f6964:8888/?token=620e6382392a984f18c922324729944590f3e91560b8b4d2
Task worker:      or http://127.0.0.1:8888/?token=620e6382392a984f18c922324729944590f3e91560b8b4d2
```

Once we copy/paste the token into the UI, we can use the Jupyter notebook as if it were
running locally.

When you're done, you can press Ctrl+C and Meadowrun will shut down the notebook server
gracefully.


## Caveats on generalizing to other services   

In general, Meadowrun isn't a tool for running services—there are better frameworks and
tools for that. This example makes sense, though, because a Jupyter notebook server
isn't a completely normal service. It usually only ever has one user connected to it,
and it's very helpful to be able to keep the local and remote code in sync as you're
working (hitting Ctrl+C and rerunning `run_command` will re-sync any changes you've made
to the local code).