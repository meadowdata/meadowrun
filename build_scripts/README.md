To publish meadowrun:

- Increment the version in `pyproject.toml`, `build_scripts/conda_package.bat`, and
  `docker_images/meadowrun/Dockerfile`
- Build the AMI:
  ```shell
  poetry run python build_scripts\build_ami.py
  ```
  That script will output the id of the newly created AMI like `New image id:
  ami-01648c5124551330f`
- Now replicate the AMI to all the regions we support:
  ```shell
  poetry run python build_scripts\replicate_ami.py replicate <new-ami-id>
  ```
  This script will output a region to AMI mapping, copy that to
  ec2_alloc.py:_EC2_ALLOC_AMIS
- Now build and publish the pip package:
  ```shell
  poetry build
  poetry publish
  ```
- Now build and publish the conda package:
  ```shell
  build_scripts/conda_package.bat
  ```
- Optionally, delete old AMIs to save money:
  ```shell
  poetry run python build_scripts/replicate_ami.py delete meadowrun-ec2alloc-0.1.4 --dry-run
  ```
  (Replace the version below with the one you want to delete and remove the `--dry-run`
  parameter when you're ready)