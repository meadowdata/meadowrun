docker build -t meadowrun/cuda-deadsnakes:cuda11.6.2-cudnn8-ubuntu20.04 --build-arg PYTHON_VERSION=3.10 -f CudaDeadsnakesDockerfile .
docker build -t meadowrun/cuda-python:py3.10-cuda11.6.2-cudnn8-ubuntu20.04 --build-arg PYTHON_VERSION=3.10 -f CudaPythonDockerfile .
docker push meadowrun/cuda-python:py3.10-cuda11.6.2-cudnn8-ubuntu20.04
docker build -t meadowrun/cuda-python:py3.9-cuda11.6.2-cudnn8-ubuntu20.04 --build-arg PYTHON_VERSION=3.9 -f CudaPythonDockerfile .
docker push meadowrun/cuda-python:py3.9-cuda11.6.2-cudnn8-ubuntu20.04
docker build -t meadowrun/cuda-python:py3.8-cuda11.6.2-cudnn8-ubuntu20.04 --build-arg PYTHON_VERSION=3.8 -f CudaPythonDockerfile .
docker push meadowrun/cuda-python:py3.8-cuda11.6.2-cudnn8-ubuntu20.04
docker build -t meadowrun/cuda-python:py3.7-cuda11.6.2-cudnn8-ubuntu20.04 --build-arg PYTHON_VERSION=3.7 -f CudaPythonDockerfile .
docker push meadowrun/cuda-python:py3.7-cuda11.6.2-cudnn8-ubuntu20.04

docker build -t meadowrun/cuda-conda:conda4.12.0-cuda11.6.2-cudnn8-ubuntu20.04 -f CudaCondaDockerfile .
docker push meadowrun/cuda-conda:conda4.12.0-cuda11.6.2-cudnn8-ubuntu20.04
