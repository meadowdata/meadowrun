REM see docker_images/meadowrun/Dockerfile

CALL poetry build
docker build -t meadowrun:latest -f docker_images\meadowrun\Dockerfile dist
