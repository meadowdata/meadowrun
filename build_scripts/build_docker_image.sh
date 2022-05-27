#!/bin/bash
set -euo pipefail

poetry build
docker build -t meadowrun:latest -f docker_images/meadowrun/Dockerfile dist