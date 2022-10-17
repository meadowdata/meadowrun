# Run Meadowrun on GKE (Google Kubernetes Engine)

This page walks you through running a function on a GKE cluster using your local code
and libraries. No need to build any containers!


## Prerequisites

- If you don't already have a GKE cluster, create a new one. We recommend [creating an
  autopilot
  cluster](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-an-autopilot-cluster).
  We'll assume you're working with an Autopilot cluster below. Standard clusters are fully
  supported as well, but require a bit more configuration..
- [Install `kubectl` and configure
  it](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl)
  to work with your GKE cluster.
- [Create a Google Storage
  bucket](https://cloud.google.com/storage/docs/creating-buckets) called something like
  `my-meadowrun-bucket`. Meadowrun will use this storage bucket to communicate with the
  remote processes running in the GKE cluster.
- Create a Kubernetes service account and give it read/write permissions to your new
  storage bucket:
    - [Create a Kubernetes service
      account](https://cloud.google.com/kubernetes-engine/docs/how-to/kubernetes-service-accounts#creating_a_kubernetes_service_account)
      in your GKE cluster called something like `my-k8s-service-account`
    - [Create a Google Cloud service
      account](https://cloud.google.com/iam/docs/creating-managing-service-accounts#creating)
      called `my-gcloud-service-account`
    - Grant the Google Cloud service account the permissions it needs. The simplest way to
      do this is to grant the "Storage Object Admin" role to the `my-gcloud-service-account`
      account for the `my-meadowrun-bucket` bucket. See [Add a principal to a bucket-level
      policy](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add).
    - Link your GKE service account (`my-k8s-service-account`) and your Google Cloud service
      account (`my-gcloud-service-account`) by following steps 6 and 7 on [this
      page](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#authenticating_to)
- Install Meadowrun

    {%
    include-markdown "../index.md"
    start="<!--install-start-->"
    end="<!--install-end-->"
    %}


## Write a Python script to run a function remotely

Create a file called `mdr.py`:

```python
import meadowrun
import asyncio

def pod_customization(pod_template):
    # You must replace this with your Kubernetes service account you created above. This
    # gives the Meadowrun-managed pods permissions to access the Google Storage bucket
    pod_template.spec.service_account_name = "my-k8s-service-account"
    return pod_template

print(
    asyncio.run(
        meadowrun.run_function(
            # the function to run remotely
            lambda: sum(range(1000)) / 1000,
            meadowrun.Kubernetes(
                # you must replace this with your Google Storage bucket
                meadowrun.GoogleBucketSpec("my-meadowrun-bucket"),
                pod_customization=pod_customization,
            ),
            # resource requirements when creating or reusing a pod
            meadowrun.Resources(logical_cpu=1, memory_gb=4),
        )
    )
)
```

## Run the script

Assuming you saved the file above as mdr.py:

```
> python -m mdr 
Waiting for pods to be created for the job mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f
Waiting for pod mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f-0-sq9ks to start running: Unschedulable, 0/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory.
Waiting for pod mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f-0-sq9ks to start running: Unschedulable, 0/2 nodes are available: 1 Insufficient cpu, 1 node(s) had taint {node.kubernetes.io/not-ready: }, that the pod didn't tolerate.
Waiting for pod mdr-reusable-03290565-a610-4d1d-95a2-e09d6056c34f-0-sq9ks to start running: ContainerCreating (pulling image)
Started 1 new pods
Result: 499.5
```


The output will walk you through what Meadowrun's [run_function][meadowrun.run_function]
is doing:

{%
include-markdown "kubernetes.md"
start="<!--k8s-output1-start-->"
end="<!--k8s-output1-end-->"
%}

- With an autopilot GKE cluster, the pod will be "Unschedulable" until the autoscaler kicks
  in and provisions a node to run this pod.

{%
include-markdown "kubernetes.md"
start="<!--k8s-output2-next-steps-start-->"
end="<!--k8s-output2-next-steps-end-->"
%}

