call build_scripts\build_docker_image.bat
docker tag meadowrun:latest meadowrun/meadowrun-dev:latest
docker push meadowrun/meadowrun-dev:latest
