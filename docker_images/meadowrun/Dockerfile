# This docker image is for tests like test_meadowrun_grid_job, where we need a container
# with meadowrun installed (with whatever local changes to the meadowrun code that we
# want to test).
#
# Building this image requires first building the wheel via poetry build, and then using
# the folder with the wheel files as the context for this image. Use the
# build_docker_image.bat file to build the image.

FROM python:3.9-slim-buster

WORKDIR /meadowrun

COPY meadowrun-0.1.6-py3-none-any.whl meadowrun-0.1.6-py3-none-any.whl

RUN pip install meadowrun-0.1.6-py3-none-any.whl
