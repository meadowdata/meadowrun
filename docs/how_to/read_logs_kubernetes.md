# Stream logs on Kubernetes

When you use `run_function`, `run_command`, or `run_map` with a single worker, the
stdout of the remote process will be streamed to your client process.

When you run `run_map` with more than a single worker, the stdout of the remote
processes will not be streamed to your client process. If you want to see the logs of
the remote processes, you can look at your local stdout to see where the job is running.
For example, you'll see a line like:

```
Created pod(s) 83382607-99e6-43fc-8f34-64b3b69ac9b1-0-qwgjz, 
    83382607-99e6-43fc-8f34-64b3b69ac9b1-1-htqr2 for job 
    83382607-99e6-43fc-8f34-64b3b69ac9b1
```

You can read the logs for any of these pods using the standard Kubernetes tools:

```
kubectl logs --follow 83382607-99e6-43fc-8f34-64b3b69ac9b1-0-qwgjz
```
