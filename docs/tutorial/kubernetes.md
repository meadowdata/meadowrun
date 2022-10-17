# Run Meadowrun on Kubernetes (Minikube)

This page shows how to run Meadowrun on Kubernetes using
[Minikube](https://minikube.sigs.k8s.io/docs/start/). Minikube isn't useful for
production workloads, but using Minikube means anyone should be able to follow along
with this article. If you need help adapting Meadowrun to your environment, [get in
touch!](mailto:contact@meadowdata.io).

If you're using GKE (Google Kubernetes Engine), there's [a more tailored version of this
tutorial](../gke).


## Prerequisites

### Install and start Minikube

First, install and start Minikube, following steps 1 and 2 from
[here](https://minikube.sigs.k8s.io/docs/start/).

Optionally, you can start `minikube dashboard --url` to get a link to a dashboard to
observe your Minikube cluster.

### Object storage

Meadowrun requires an S3-compatible object store to communicate with the Kubernetes
pods. We'll specify this using a
[`GenericStorageBucketSpec`][meadowrun.GenericStorageBucketSpec].

#### With an S3-compatible object store
  
If you already have an S3-compatible object store that is accessible from where you're
using Meadowrun as well as your Kubernetes cluster, then you can just create a
`GenericStorageBucketSpec`. Here's an example:

```python
meadowrun.GenericStorageBucketSpec(
    bucket="meadowrunbucket",
    endpoint_url="http://my-storage-system:9000",
    username_password_secret="my-storage-credentials",
)
```

A couple notes:

- `bucket` is the name of the bucket that Meadowrun will use. We recommend creating a
  new bucket specifically for Meadowrun.
- `endpoint_url` is the URL for your object storage system.
- `username_password_secret` is the name of a Kubernetes secret that has a "username"
  and "password" key for accessing your storage system. Here's an example of how you
  could create such a secret:

```shell
kubectl create secret generic storage-credentials --from-literal=username=MYUSERNAME --from-literal=password=MYPASSWORD
```

All together, these parameters should be set so that:

```python
import boto3
boto3.Session(
    aws_access_key_id=username,
    aws_secret_access_key=password
).client(
    "s3", endpoint_url=endpoint_url
).download_file(
    Bucket=bucket, Key="test.file", Filename="test.file"
)
```

works, where `username` and `password` should be the values provided by
`username_password_secret`. (boto3 is built to be used with AWS S3, but it should work
with any S3-compatible object store like Minio, Ceph, etc.)


#### With Minio

If you don't already have an S3-compatible object store, the beauty of Kubernetes is
that it's easy to spin one up! We'll use [Minio](https://min.io/) here.

1. Launch Minio.

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
    
    Change `ROOTNAME` and `CHANGEME123` and then run:
    
    ```shell
    kubectl apply -f minio.yaml 
    ```
    
    If you understand the security risks of running with the default username/password, you
    can also just run:
    
    ```shell
    kubectl apply -f https://raw.githubusercontent.com/meadowdata/meadowrun/main/docs/how_to/minio.yaml
    ```
    
    This will create a Minio service accessible from inside of Kubernetes as
    `minio-service:9000`, and the web admin UI will be available as `minio-service:9090`

2. Make Minio accessible from outside of the cluster:

    From outside of the cluster, you'll need to use port forwarding to make the service
    available from your local machine:
    
    ```
    kubectl port-forward service/minio-service 9000:9000 9090:9090
    ```
    
    This will make our Minio service available as `127.0.0.1:9000` locally.

3. Create a bucket for Meadowrun:
 
    We'll need to use the Minio web UI (now available as `127.0.0.1:9090`) to create a
    bucket. You should be able to log in with the username/password you set in `minio.yaml`,
    and then click on "Create Bucket" to create a new bucket called `meadowrunbucket`.

4. Create a Kubernetes secret with our Minio credentials:
    
    ```shell
    kubectl create secret generic minio-credentials --from-literal=username=ROOTNAME --from-literal=password=CHANGEME123
    ```
   
    Make sure to replace ROOTNAME and CHANGEME123 to whatever values you used above.

That means we can create our `GenericStorageBucketSpec`:

```python
meadowrun.GenericStorageBucketSpec(
    bucket="meadowrunbucket",
    endpoint_url="http://127.0.0.1:9000",
    endpoint_url_in_cluster="http://minio-service:9000",
    username_password_secret="minio-credentials",
)
```

In this case, `endpoint_url` is used from outside of the Kubernetes cluster and
`endpoint_url_in_cluster` is used from inside of the cluster. If `endpoint_url_in_cluster`
is not specified, `endpoint_url` will be used from inside and outside of the cluster.


### Install Meadowrun

{%
include-markdown "../index.md"
start="<!--install-start-->"
end="<!--install-end-->"
%}


## Write a Python script to run a function remotely

Now we're ready to run a Meadowrun job on our Kubernetes cluster. Create a file called
`mdr.py`:

```python
import asyncio
import meadowrun

print(
    asyncio.run(
        meadowrun.run_function(
            # the function to run remotely
            lambda: sum(range(1000)) / 1000,
            host=meadowrun.Kubernetes(
                # replace this with the right GenericStorageBucketSpec for your 
                # environment
                meadowrun.GenericStorageBucketSpec(
                    bucket="meadowrunbucket",
                    endpoint_url="http://127.0.0.1:9000",
                    endpoint_url_in_cluster="http://minio-service:9000",
                    username_password_secret="minio-credentials",
                )
            ),
            # resource requirements when creating or reusing a pod
            resources=meadowrun.Resources(logical_cpu=1, memory_gb=4),
        )
    )
)
```


## Run the script

Assuming you saved the file above as mdr.py:

```
> python -m mdr 
Waiting for pods to be created for the job mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f
Waiting for pod mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f-0-sq9ks to start running: ContainerCreating (pulling image)
Started 1 new pods
Result: 499.5
```

The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:


<!--k8s-output1-start-->
- Meadowrun creates a pod  which will run our function. (Specifically, this pod is
  created via a Kubernetes Job. The Job is just to make it easy to launch multiple pods
  quickly. In this case, because we're using `run_function`, we only need a single pod.
  Using [`run_map`][meadowrun.run_map] would require multiple pods.)
<!--k8s-output1-end-->
<!--k8s-output2-next-steps-start-->
- We didn't provide the `deployment` parameter, which is equivalent to specifying
  `deployment=`[`meadowrun.Deployment.mirror_local`][meadowrun.Deployment.mirror_local].
  This tells Meadowrun to copy your local environment and code to the pod instance.
  Meadowrun detects what kind of environment (conda, pip, or poetry) you're currently in
  and calls the equivalent of `pip freeze` to capture the libraries installed in the
  current environment. The pod will then create a virtualenv/conda environment that
  matches your local environment and caches the environment (using
  [venv-pack](https://jcristharif.com/venv-pack/)/[conda-pack](https://conda.github.io/conda-pack/))
  for reuse in the Google Storage bucket. Meadowrun will also zip up your local code and
  send it to the pod.
- Meadowrun then runs the specified function in that environment in the pod and returns
  the result.
- The pod can be reused by subsequent jobs. If the pod isn't reused for a few minutes,
  it will terminate on its own. (There is a `reusable_pods` parameter on
  `meadowrun.Kubernetes` if you prefer to use a pod-per-job model.)


## Next steps

- In addition to `run_function`, Meadowrun provides [other
  entrypoints](../entry_points). The most important of these is
  [`run_map`][meadowrun.run_map], which allows you to use many pods in parallel.
- By default, Meadowrun will mirror your current local deployment, but there are [other
  ways to specify the code and libraries](../deployment) you want to use when running
  remotely.

<!--k8s-output2-next-steps-end-->
