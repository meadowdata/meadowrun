Instructions for building the Linux conda package.

First, run a container with conda-build and anaconda-client installed:

`docker run -it hrichardlee/conda-build /bin/bash`

(This is built by docker_images/conda-build)

Inside the container, build the package:

```shell
cd ~
conda skeleton pypi meadowrun

# exercise for the reader: open meadowrun/meta.yaml and add "poetry" under requirements: host:

conda build -c defaults -c conda-forge --python 3.9 meadowrun
```

Assuming everything works okay, upload the package:

```shell
# first log in if necessary:
anaconda login

# replace the version number here as appropriate
anaconda upload /opt/conda/conda-bld/linux-64/meadowrun-0.1.2-py39_0.tar.bz2
```
