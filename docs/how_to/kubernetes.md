# Run Meadowrun on Kubernetes

> ⚠️ Meadowrun on Kubernetes is a work in progress. Please [contact
us](mailto:contact@meadowdata.io) or [create a GitHub
issue](https://github.com/meadowdata/meadowrun/issues) to let us know what you need!

There's a wider variety of Kubernetes environments compared to "vanilla" cloud
environment like AWS or Azure. Whereas Meadowrun can assume that if you're running in
AWS, S3 will be available, a Kubernetes environment could be:

- AWS EKS which will have S3 available
- An "on-prem" deployment which primarily uses an S3-compatible object store like Minio
  or Ceph
- An "on-prem" deployment which primarily uses NFS rather than an object storage *(this
  scenario is not currently supported)*

As a result, Meadowrun on Kubernetes requires some configuration which is unique to each
Kubernetes environment.

In this example, we'll show how to run Meadowrun on Minikube using
[meadowrun.Kubernetes][meadowrun.Kubernetes]. Minikube isn't useful for
production workloads, but using Minikube means anyone should be able to follow along
with this article. We'll intersperse a bit of discussion on how to adapt this example to
other environments along the way.

The goal at the end of this article is to be able to run: 

```python
import asyncio
import platform
import time
import meadowrun

def remote_function():
    print(f"Hello! It's {time.time()} on {platform.node()}")
    return 2 + 2

print(
    asyncio.run(
        meadowrun.run_function(
            remote_function,
            meadowrun.Kubernetes(
                storage_bucket="meadowrunbucket",
                storage_endpoint_url="http://127.0.0.1:9000",
                storage_endpoint_url_in_cluster="http://minio-service:9000",
                storage_username_password_secret="minio-credentials",
                kube_config_context="minikube"
            ),
            meadowrun.Deployment.container_image("meadowrun/meadowrun-dev")
        )
    )
)
```

If you're not already familiar with [run_function][meadowrun.run_function], it's
probably a good idea to at least skim the [introductory
tutorial](/tutorial/run_function.md).

## Prepare a container image

If you're using `run_command`, you can use any container image you like.

If you're using `run_function`, the container must have python in it, and whatever
python environment is on the path must have Meadowrun installed. In this example, we'll
use `meadowrun/meadowrun-dev`:

```python
    meadowrun.Deployment.container_image("meadowrun/meadowrun-dev")
```

## Install and start Minikube

First, install and start Minikube, following steps 1 and 2 from
[here](https://minikube.sigs.k8s.io/docs/start/).

If you're using multiple Kubernetes clusters, you'll want to make sure the "minikube"
[config
context](https://kubernetes.io/docs/tasks/access-application-cluster/configure-access-multiple-clusters/)
(created when you install Minikube) is active, with `kubectl config use-context
minikube`. Alternatively, you can also specify a config context explicitly with the
`kube_config_context` parameter to `meadowrun.Kubernetes`.

## Object storage

Meadowrun can optionally use an S3-compatible object store to send inputs back and forth
from a Kubernetes Job.

### Without an S3-compatible object store

An S3-compatible object store is NOT needed if:

- You're only using [run_command][meadowrun.run_command]
- Or, when you use [run_function][meadowrun.run_function], you're only using strings
  (rather than function references) to reference your function, and you're okay with not
  getting back the results of your function

If you're not using an object store, then you can omit the parameters starting with
`storage_`.

Here's an example showing how to run these kinds of jobs on Meadowrun/Kubernetes:

```python
import asyncio
import meadowrun

asyncio.run(
    meadowrun.run_function(
        "time.time",
        meadowrun.Kubernetes(),
        meadowrun.Deployment.container_image("meadowrun/meadowrun-dev")
    )
)
```

This won't have any observable output, and it won't return anything because without the
storage bucket, we have no way of getting the results back. But if your job is
self-contained, e.g. it collects all of its own inputs and writes its outputs somewhere
then this can be a viable option. Any python exceptions will get propagated as
"NON_ZERO_RETURN_CODE", as we aren't able to get the actual content of the exception in
this case.


### With an S3-compatible object store
  
If you already have an S3-compatible object store that is accessible from where you're
using Meadowrun as well as your Kubernetes cluster, then you can just populate
`storage_bucket`, `storage_endpoint_url`, `storage_endpoint_url_in_cluster`, and
`storage_username_password_secret`.

A couple notes:

- `storage_bucket` is the name of the bucket that Meadowrun will use. We recommend
  creating a new bucket, e.g. `meadowrun`
- `storage_endpoint_url_in_cluster` can be used because sometimes your object store will
  will be accessible via different URLs from outside of the Kubernetes cluster and inside
  of it. This can be omitted if the same URL can be used inside and outside of the
  Kubernetes cluster.
- `storage_username_password_secret` should be the name of a Kubernetes secret that has
  a "username" and "password" key. Here's an example of how you could create such a
  secret:

```shell
kubectl create secret generic storage-credentials --from-literal=username=MYUSERNAME --from-literal=password=MYPASSWORD
```

All together, these parameters should be set so that:

```python
import boto3
boto3.Session(
    aws_access_key_id=storage_username,
    aws_secret_access_key=storage_password
).client(
    "s3", endpoint_url=storage_endpoint_url
).download_file(
    Bucket=storage_bucket, Key="test.file", Filename="test.file"
)

```

works, where `storage_username` and `storage_password` should be the values provided by
`storage_username_password_secret`. (boto3 is built to be used with AWS S3, but it
should work with any S3-compatible object store like Minio, Ceph, etc.)

You should also probably create a new bucket for this usage, calling it something like
`meadowrun`.

### With Minio

If you don't already have an S3-compatible object store, the beauty of Kubernetes is
that it's easy to spin one up! We'll use Minio here.

First, create a Kubernetes Deployment running Minio and a Service so that we can access
it from other pods in the cluster. Create a file called `minio.yaml` with the contents:

```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    app.kubernetes.io/name: minio
  name: minio
spec:
  containers:
  - name: minio
    image: quay.io/minio/minio:latest
    args:
      - server
      - /root
      - --console-address
      - ":9090"
    env:
    - name: MINIO_ROOT_USER
      value: "ROOTNAME"
    - name: MINIO_ROOT_PASSWORD
      value: "CHANGEME123"
    ports:
    - containerPort: 9000
    - containerPort: 9090
---
apiVersion: v1
kind: Service
metadata:
  name: minio-service
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: minio
  ports:
    - name: minio-main-port
      protocol: TCP
      port: 9000
      targetPort: 9000
    - name: minio-console-port
      protocol: TCP
      port: 9090
      targetPort: 9090
```

Then run:

```shell
kubectl apply -f minio.yaml 
```

If you're running this on anything other than Minikube, make sure to change `ROOTNAME`
and `CHANGEME123`.

This will create a Minio service accessible from inside of Kubernetes as
`minio-service:9000`, and the web admin UI will be available as `minio-service:9090`

We also set the type of the Service to be LoadBalancer which should create an externally
visible address, but because we're running on Minikube, the LoadBalancer won't work. If
you're running in a different configuration, this Service will probably be externally
visible for you, and you can get the external address via:

```shell
kubectl get services
```

by looking at the `EXTERNAL-IP` column for `minio-service`

If the LoadBalancer Service type doesn't work (as with Minikube), you can use port
forwarding to make the service available from your local machine:

```shell
kubectl port-forward service/minio-service 9000:9000 9090:9090
```

This will make our Minio service available as `127.0.0.1:9000` locally.

We'll need to use the Minio web UI (now available as `127.0.0.1:9090`) to create a
bucket. You should be able to log in with the username/password you set in `minio.yaml`,
and then click on "Create Bucket" to create a new bucket called `meadowrunbucket`.

Finally, we'll need to store our Minio credentials as a Kubernetes secret with username
and password keys:

```shell
kubectl create secret generic minio-credentials --from-literal=username=ROOTNAME --from-literal=password=CHANGEME123
```

That means we're all set for these lines in the sample:

```python
        storage_bucket="meadowrunbucket",
        storage_endpoint_url="http://127.0.0.1:9000",
        storage_endpoint_url_in_cluster="http://minio-service:9000",
        storage_username_password_secret="minio-credentials",
```

## Running the full example

We can now run the full example at the top of this article. Here's a quick walk-through
of the output. The first two lines give us the ids for the Kubernetes Jobs and Pods that
get created:

```
Waiting for Kubernetes to create the pod for job f1f84e52-8fa3-4d06-a08b-3ff27b9a8e3c
Created pod f1f84e52-8fa3-4d06-a08b-3ff27b9a8e3c-clwl2 for job f1f84e52-8fa3-4d06-a08b-3ff27b9a8e3c
```

Next, we wait for the image to get pulled in and for a node to become available to run
our pod:

```
Waiting for container to start running: ContainerCreating (pulling image or waiting for available nodes)
```

Next, we download the pickled version of `remote_function`:

```
Downloading function from meadowrunbucket/f1f84e52-8fa3-4d06-a08b-3ff27b9a8e3c.function
```

Now our code in `remote_function` is running, and the remote stdout is relayed to us:

```
Hello! It's 1659482233.465762 on f1f84e52-8fa3-4d06-a08b-3ff27b9a8e3c-clwl2
```

Finally, `meadowrun.run_function` completes and returns the output of `remote_function`
which we print out:

```
4
```