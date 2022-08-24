REM this has to be run from the root of the repo

set /p MEADOWRUN_VERSION_FILE=<src/meadowrun/version.py
set MEADOWRUN_VERSION=%MEADOWRUN_VERSION_FILE:~15,-1%

echo Meadowrun version: %MEADOWRUN_VERSION%

call poetry build

docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10 --build-arg PYTHON_VERSION=3.10 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
docker push meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10
docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.9 --build-arg PYTHON_VERSION=3.9 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
docker push meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.9
docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.8 --build-arg PYTHON_VERSION=3.8 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
docker push meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.8
docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.7 --build-arg PYTHON_VERSION=3.7 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
docker push meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.7

docker tag meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10 meadowrun/meadowrun:%MEADOWRUN_VERSION%
docker push meadowrun/meadowrun:%MEADOWRUN_VERSION%

REM comment this out if you're rerunning for an old version!
docker tag meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10 meadowrun/meadowrun:latest
docker push meadowrun/meadowrun:latest
