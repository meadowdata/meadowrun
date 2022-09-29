@REM This is the same script as build.bat, it just does minikube image load rather than
@REM docker push. Make sure that your version is set to a non-prod version like 0.2.10a1

set /p MEADOWRUN_VERSION_FILE=<src/meadowrun/version.py
set MEADOWRUN_VERSION=%MEADOWRUN_VERSION_FILE:~15,-1%

echo Meadowrun version: %MEADOWRUN_VERSION%

call poetry build

@REM docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10 --build-arg PYTHON_VERSION=3.10 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM minikube image load meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.10

docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.9 --build-arg PYTHON_VERSION=3.9 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
minikube image load meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.9

@REM docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.8 --build-arg PYTHON_VERSION=3.8 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM minikube image load meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.8

@REM docker build -t meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.7 --build-arg PYTHON_VERSION=3.7 --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/meadowrun/Dockerfile dist
@REM minikube image load meadowrun/meadowrun:%MEADOWRUN_VERSION%-py3.7
