call build_scripts\build_docker_image.bat
docker tag meadowrun:latest meadowrun/meadowrun:latest
docker push meadowrun/meadowrun:latest

docker tag meadowrun:latest meadowrun/meadowrun:0.2.1
docker push meadowrun/meadowrun:0.2.1
