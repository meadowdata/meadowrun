# This docker image is for tests like test_meadowgrid_grid_job, where we need a
# container with meadowdata installed (with whatever local changes to the meadowdata
# code that we want to test).
#
# Building this image requires first building the wheel via poetry build, and then using
# the folder with the wheel files as the context for this image. Use the
# build_docker_image.bat file to build the image.

FROM python:3.9-slim-buster

WORKDIR /meadowgrid

COPY meadowdata-0.1.0-py3-none-any.whl meadowdata-0.1.0-py3-none-any.whl

RUN pip install meadowdata-0.1.0-py3-none-any.whl
