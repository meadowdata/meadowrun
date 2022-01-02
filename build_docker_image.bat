REM see docker_images/meadowdata/Dockerfile

CALL poetry build
docker build -t meadowdata:latest -f docker_images\meadowdata\Dockerfile dist
