ARG PYTHON_VERSION
FROM python:$PYTHON_VERSION-slim-buster

WORKDIR /meadowrun

RUN pip install venv-pack

ARG MEADOWRUN_VERSION
COPY meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl

RUN pip install meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl && rm meadowrun-$MEADOWRUN_VERSION-py3-none-any.whl
