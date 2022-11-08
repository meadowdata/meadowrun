### To rebuild the base images (should only be necessary once in a while):

- Delete any existing base images via build_scripts\manage_amis.py list and delete
- Build new plain base AMIs with `build_scripts\build_base_ami.py plain all`
- Copy the region/AMI ID mapping from the output of that script into ami_listing.py under BASE_AMIS["plain"]
- Build new cuda base AMIs with `build_scripts\build_base_ami.py cuda all`
- Copy the region/AMI ID mapping from the output of that script into ami_listing.py under BASE_AMIS["cuda"]

## To rebuild the AWS management lambda layer (only necessary if aws_integration/management_lambdas/requirements.txt changed)

- Run `build_aws_lambda_layer.py`.
- Copy the Layer ARNs from the output of the script to the constants in `aws_mgmt_lambda_install.py`
### To publish meadowrun:

- Increment the version in `pyproject.toml` and `version.py`
- Build the EC2 AMIs and copy the new AMI ids into the code by following the
  instructions in build_scripts\build_ami.py.
- Build the Azure Image by following the instructions in
  build_scripts\build_azure_image.py.
- Now test the code changes
- Now commit the code changes
- Write release notes on GitHub

These steps can be done in parallel:
- Build and publish the pip package:
  ```shell
  poetry build
  poetry publish
  ```
- Build and publish the conda package by running `build_scripts/build_conda.py`
- Build and publish the docker images by running
  `docker_images/meadowrun/build.bat`

Finally:
- Optionally, delete old AMIs to save money:
  ```shell
  poetry run python build_scripts/manage_amis.py delete meadowrun0.1.4 --dry-run
  ```
  (Replace the version with the one you want to delete and remove the `--dry-run`
  parameter when you're ready)