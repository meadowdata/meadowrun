# Use a private container on Kubernetes

[Run Meadowrun on Kubernetes](kubernetes) walks through the basics of using Meadowrun
with Kubernetes. This article is a small extension that shows how to use a container
from a private registry.

First, you'll need to create a Kubernetes secret that contains your Docker credentials.
One way to do this is:

```shell
kubectl create secret docker-registry mydockerhubcredentials \
  --docker-server=https://index.docker.io/v1/ --docker-username=mydockerusername \
  --docker-password=mydockerpassword --docker-email=myemailaddress
```

- `https://index.docker.io/v1/` is the server for DockerHub. You can put in the URL for
  any container registry
- `mydockerusername`, `mydockerpassword`, and `myemailaddress` obviously will be
  specific to your account.
- `mydockerhubcredentials` is the name of the Kubernetes secret, and you can set that to
  whatever you want. 

[This documentation
page](https://kubernetes.io/docs/concepts/containers/images/#creating-a-secret-with-a-docker-config)
talks about some other ways to create a Kubernetes secret with your Docker credentials.

Now, you just need to specify this secret with:

```python
import meadowrun

meadowrun.Deployment.container_image(
    "mydockerusername/myprivatedockerimage",
    username_password_secret=meadowrun.KubernetesSecret("mydockersecret"))
```
