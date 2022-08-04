To publish meadowrun:

- Increment the version in `pyproject.toml`, `build_scripts/conda-recipe/meta.yaml`, and
  `docker_images/meadowrun/Dockerfile`
- Build the EC2 AMIs by following the instructions in build_scripts\build_ami.py
- Build the Azure Image by following the instructions in
  build_scripts\build_azure_image.py
- Now build and publish the pip package:
  ```shell
  poetry build
  poetry publish
  ```
- Now build and publish the conda package by following the manual instructions in
  conda-recipe/meta.yaml
- Optionally, delete old AMIs to save money:
  ```shell
  poetry run python build_scripts/manage_amis.py delete meadowrun-ec2alloc-0.1.4 --dry-run
  ```
  (Replace the version with the one you want to delete and remove the `--dry-run`
  parameter when you're ready)