# Stream logs on AWS or Azure

When you use `run_function`, `run_command`, or `run_map` with a single worker, the
stdout of the remote process will be streamed to your client process.

When you run `run_map` with more than a single worker, the stdout of the remote
processes will not be streamed to your client process. If you want to see the logs of
the remote processes, you can look at your local stdout to see where the job is running.
For example, you'll see a line like:

```
Running job on ec2-18-222-147-66.us-east-2.compute.amazonaws.com 
    /var/meadowrun/job_logs/ede23a08-70b8-4eca-8f03-f7ed7ff28f74.log
```

You can read the logs for this worker using SSH. You'll need to [download your SSH
key](../ssh_to_instance), then you can run:

```
ssh -i ~/.ssh/meadowrun_id_rsa.EC2 \
    ubuntu@ec2-18-222-147-66.us-east-2.compute.amazonaws.com \
    "tail -F /var/meadowrun/job_logs/ede23a08-70b8-4eca-8f03-f7ed7ff28f74.log"
```
