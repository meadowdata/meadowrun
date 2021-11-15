REM see docker_images/meadowdata/Dockerfile

poetry build
docker build -t meadowdata -f docker_images\meadowdata\Dockerfile dist
