REM this has to be run from the root of the repo

set /p MEADOWRUN_VERSION_FILE=<src/meadowrun/version.py
set MEADOWRUN_VERSION=%MEADOWRUN_VERSION_FILE:~15,-1%

echo Meadowrun version: %MEADOWRUN_VERSION%


docker build -t meadowrun/meadowrun_test_env --build-arg MEADOWRUN_VERSION=%MEADOWRUN_VERSION% -f docker_images/testenv/TestEnvDockerfile docker_images/testenv
docker push meadowrun/meadowrun_test_env
