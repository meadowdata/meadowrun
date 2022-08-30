set /p MEADOWRUN_VERSION_FILE=<src/meadowrun/version.py
set MEADOWRUN_VERSION=%MEADOWRUN_VERSION_FILE:~15,-1%

CALL poetry build

docker build -t meadowrun/meadowrun-dev:py3.10 --build-arg PYTHON_VERSION=3.10 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM docker push meadowrun/meadowrun-dev:py3.10
@REM docker build -t meadowrun/meadowrun-dev:py3.9 --build-arg PYTHON_VERSION=3.9 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM docker push meadowrun/meadowrun-dev:py3.9
@REM docker build -t meadowrun/meadowrun-dev:py3.8 --build-arg PYTHON_VERSION=3.8 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM docker push meadowrun/meadowrun-dev:py3.8
@REM docker build -t meadowrun/meadowrun-dev:py3.7 --build-arg PYTHON_VERSION=3.7 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM docker push meadowrun/meadowrun-dev:py3.7

docker tag meadowrun/meadowrun-dev:py3.10 meadowrun/meadowrun-dev:latest
@REM docker push meadowrun/meadowrun-dev:latest
