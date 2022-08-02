REM see docker_images/meadowrun/Dockerfile

CALL poetry build
docker build -t meadowrun/meadowrun-dev:latest -f docker_images\meadowrun\Dockerfile dist
