call build_docker_image.bat
docker tag meadowrun:latest hrichardlee/meadowrun:latest
docker push hrichardlee/meadowrun:latest
