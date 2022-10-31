# Overview

Meadowrun is a library for data scientists and data engineers who run python code on
AWS, Azure, or Kubernetes. Meadowrun:

- scales from a single function to thousands of distributed tasks.
- syncs your local code and libraries for a faster, easier iteration loop. Edit your
  code and rerun your analysis without worrying about building packages or Docker
  images.
- optimizes for cost, choosing the cheapest instance types and turning them off when
  they're no longer needed.
  
For more context, see our [case studies](case_studies) of how Meadowrun is used in real
life, or see the [project homepage](https://meadowrun.io)

## Quickstart

This section provides snippets for running your first job with Meadowrun. Alternatively,
you can start with more in-depth tutorials on [AWS EC2](tutorial/aws_ec2), [Azure
VMs](tutorial/azure_vm), [GKE](tutorial/gke), or [Kubernetes](tutorial/kubernetes).

### 1. Install the Meadowrun package

# <!--install-start-->

=== "Pip"
    ```shell
    pip install meadowrun
    ```
=== "Conda"
    ```shell
    conda install -c defaults -c conda-forge -c meadowdata meadowrun
    ```

    If you're using conda on a Windows or Mac, Meadowrun won't be able to mirror your local
    environment because conda environments aren't cross-platform and Meadowrun runs the
    remote jobs on Linux. If you're in this situation, you can either switch to Pip or
    Poetry, or create a [CondaEnvironmentFile][meadowrun.CondaEnvironmentFile] that's built
    for Linux and pass that in to the
    [mirror_local][meadowrun.Deployment.mirror_local] call in the next step below, like
    `mirror_local(interpreter=CondaEnvironmentFile(...))`
=== "Poetry"
    ```shell
    poetry add meadowrun
    ```

# <!--install-end-->

### 2. Configure your cloud environment:

=== "AWS"
    Make sure you're logged into the AWS CLI as a root/administrator account. Then, set up
    Meadowrun resources in your AWS account:
    
    ```shell
    meadowrun-manage-ec2 install
    ```
=== "Azure"
    Make sure you're logged into the Azure CLI as a root/administrator account. Then, set up
    Meadowrun resources in your Azure account:
    
    ```shell
    meadowrun-manage-azure-vm install
    ```
=== "GKE"
    - Configure `kubectl` to work with your GKE cluster
    - Create a Google Storage bucket called something like `my-meadowrun-bucket`
    - Create a Kubernetes service account called `my-service-account` that is linked
      to a Google Cloud service account that has permissions to read and write to
      `my-meadowrun-bucket`
=== "Kubernetes"
    - Configure `kubectl` to work with your Kubernetes cluster
    - We'll assume you have an S3-compatible object storage system that is accessible from 
      outside and inside the cluster (e.g. Minio)
    - Create a bucket called `meadowrun-bucket` in your object storage system
    - Create a Kubernetes secret called `storage-credentials` that has a `username` and
      `password` field to provide access to the object storage system 

### 3. Run your first job

=== "AWS"
    ```python
    import meadowrun
    import asyncio
    
    print(
        asyncio.run(
            meadowrun.run_function(
                lambda: sum(range(1000)) / 1000,
                meadowrun.AllocEC2Instance(),
                meadowrun.Resources(logical_cpu=1, memory_gb=4, max_eviction_rate=80),
                meadowrun.Deployment.mirror_local()
            )
        )
    )
    ```
=== "Azure"
    ```python
    import meadowrun
    import asyncio
    
    print(
        asyncio.run(
            meadowrun.run_function(
                lambda: sum(range(1000)) / 1000,
                meadowrun.AllocAzureVM(),
                meadowrun.Resources(logical_cpu=1, memory_gb=4, max_eviction_rate=80),
                meadowrun.Deployment.mirror_local()
            )
        )
    )
    ```
=== "GKE"
    ```python
    import meadowrun
    import asyncio

    def pod_customization(pod_template):
        pod_template.spec.service_account_name = "my-service-account"
        return pod_template
    
    print(
        asyncio.run(
            meadowrun.run_function(
                lambda: sum(range(1000)) / 1000,
                meadowrun.Kubernetes(
                    meadowrun.GoogleBucketSpec("my-meadowrun-bucket"),
                    reusable_pods=True,
                    pod_customization=pod_customization,
                ),
                meadowrun.Resources(logical_cpu=1, memory_gb=4),
                meadowrun.Deployment.mirror_local()
            )
        )
    )
    ```
=== "Kubernetes"
    ```python
    import meadowrun
    import asyncio

    print(
        asyncio.run(
            meadowrun.run_function(
                lambda: sum(range(1000)) / 1000,
                meadowrun.Kubernetes(
                    GenericStorageBucketSpec(
                        "meadowrun-bucket",
                        endpoint_url="http://127.0.0.1:9000",
                        endpoint_url_in_cluster="http://storage-service:9000",
                        username_password_secret="storage-credentials",
                    )
                ),
                meadowrun.Resources(logical_cpu=1, memory_gb=4),
                meadowrun.Deployment.mirror_local()
            )
        )
    )
    ```

  
## Next steps

For more background, read about [How Meadowrun works](explanation/how_it_works).
