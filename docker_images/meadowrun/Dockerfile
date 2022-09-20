ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim-buster

WORKDIR /var/meadowrun

RUN mkdir -p /var/meadowrun/machine_cache /var/meadowrun/job_logs /var/meadowrun/io /var/meadowrun/git_repos /var/meadowrun/git_repos/local_copies /var/meadowrun/misc


RUN apt update && \
    apt install -y git && \
    rm -rf /var/lib/apt

RUN git config --global --add safe.directory '*'; exit 0

RUN python -c 'import urllib.request; urllib.request.urlretrieve("https://install.python-poetry.org", "poetry_install.py")' && \
    python poetry_install.py && \
    rm poetry_install.py && \
    /root/.local/bin/poetry config virtualenvs.in-project true

RUN python -c 'import urllib.request; urllib.request.urlretrieve("https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh", "miniconda.sh")' && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

# this should be -c defaults -c conda-forge, but for some reason the version of
# libarchive in the defaults channel has a problem:
# https://github.com/mamba-org/mamba/issues/1728
RUN /opt/conda/bin/conda install -y mamba -n base -c conda-forge -c defaults
RUN pip install venv-pack conda-pack

ARG MEADOWRUN_VERSION
COPY meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl

RUN pip install meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl && rm meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl
