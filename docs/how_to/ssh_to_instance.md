# SSH to a Meadowrun-launched instance

## AWS and Azure

To SSH to a Meadowrun-launched instance, you'll first need to get a private SSH key that
will allow you to log onto a machine:

=== "AWS EC2"
    ```shell
    meadowrun-manage-ec2 get-ssh-key
    ```

=== "Azure VM"
    ```shell
    meadowrun-manage-azure-vm get-ssh-key
    ```

This will download a key to ~/.ssh/meadowrun_id_rsa.\[EC2|AzureVM\]

Now, you'll be able to use SSH, SCP, etc., as usual. You'll need to supply the host
name from the output of your `run_function`/`run_command`/`run_map` call:

=== "AWS EC2"
    ```shell
    ssh -i ~/.ssh/meadowrun_id_rsa.EC2 ubuntu@ec2-18-216-7-235.us-east-2.compute.amazonaws.com
    ```

=== "Azure VM"
    ```shell
    ssh -i ~/.ssh/meadowrun_id_rsa.AzureVM meadowrunuser@18.216.7.235
    ```

The `-i` parameter to ssh is short for "identity" and tells ssh which private key file
to use for the connection.


## Kubernetes

Assuming you have the name of a Meadowrun-managed pod from the standard output, you can run:

```
kubectl exec -it mdr-reusable-c8902976-03f6-4688-ba0b-2cc7bc285291-0-tdhgf -- /bin/bash
```
