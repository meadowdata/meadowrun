# Kubernetes resources created by Meadowrun

Meadowrun creates Kubernetes Jobs named `mdr-<uuid>` or `mdr-reusable-<uuid>`. For
reusable pods, a single Kubernetes Job does not necessarily correspond to a single
Meadowrun Job. All Jobs and pods created for those Jobs will be tagged with
`meadowrun.io=true`

To delete all Meadowrun resources, including those used for currently running jobs, run

```
kubectl delete jobs,pods -l meadowrun.io=true
```
