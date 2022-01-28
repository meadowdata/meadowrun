call build_docker_image.bat
docker tag meadowdata:latest hrichardlee/meadowdata:latest
docker push hrichardlee/meadowdata:latest
