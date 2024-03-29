from __future__ import annotations

import argparse
import asyncio
import datetime
import os.path
import time
from typing import TYPE_CHECKING

import meadowrun.aws_integration.aws_install_uninstall as aws
import meadowrun.azure_integration.azure_install_uninstall as azure
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_permissions_install import (
    grant_permission_to_ecr_repo,
    grant_permission_to_s3_bucket,
    grant_permission_to_secret,
)
from meadowrun.aws_integration.ec2_instance_allocation import SSH_USER
from meadowrun.aws_integration.ec2_pricing import clear_prices_cache
from meadowrun.aws_integration.ec2_ssh_keys import (
    download_ssh_key as ec2_download_ssh_key,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _deregister_and_terminate_instances,
)
from meadowrun.aws_integration.management_lambdas.clean_up import (
    delete_old_task_queues as aws_delete_old_task_queues,
    delete_unused_images as aws_delete_unused_images,
)
from meadowrun.azure_integration.azure_meadowrun_core import (
    ensure_meadowrun_resource_group,
    ensure_meadowrun_storage_account,
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.azure_ssh_keys import (
    download_ssh_key as azure_download_ssh_key,
    _ensure_meadowrun_vault,
)
from meadowrun.azure_integration.azure_vms import _MEADOWRUN_USERNAME
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
    MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE,
    MEADOWRUN_STORAGE_ACCOUNT_VARIABLE,
    MEADOWRUN_SUBSCRIPTION_ID,
)
from meadowrun.azure_integration.mgmt_functions.clean_up import (
    delete_old_task_queues as azure_delete_old_task_queues,
    delete_unused_images as azure_delete_unused_images,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import (
    _deregister_and_terminate_vms,
    terminate_all_vms,
)

if TYPE_CHECKING:
    from meadowrun.run_job_core import CloudProviderType


def _strtobool(val: str) -> int:
    # copied from distutils.util
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ("y", "yes", "t", "true", "True", "on", "1"):
        return 1
    elif val in ("n", "no", "f", "false", "False", "off", "0"):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


async def async_main(cloud_provider: CloudProviderType) -> None:
    if cloud_provider == "EC2":
        region_name = await _get_default_region_name()
    elif cloud_provider == "AzureVM":
        region_name = get_default_location()
    else:
        raise ValueError(f"Unexpected cloud_provider {cloud_provider}")

    parser = argparse.ArgumentParser()

    if cloud_provider == "EC2":
        parser.add_argument(
            "--region",
            type=str,
            default=region_name,
            help=(
                f"An optional argument to specify a region name, e.g. {region_name}. If"
                " this is not specified, will use the defaults as specified by `aws "
                "configure`"
            ),
        )

    subparsers = parser.add_subparsers(dest="command")

    # equivalent language for logging
    if cloud_provider == "EC2":
        functions = "AWS lambdas"
        cloud_name = "AWS"
        role = "EC2 role"
        secret = "AWS secret"
        vm_instances = "EC2 instances"
        ssh_user = SSH_USER
        vm_instance_address = "<ec2-instance-public-address>"
        ecr = "ECR"
    elif cloud_provider == "AzureVM":
        functions = "Azure Functions"
        cloud_name = "Azure"
        role = "managed identity"
        secret = "Azure secret"
        vm_instances = "Azure VMs"
        ssh_user = _MEADOWRUN_USERNAME
        vm_instance_address = "<azure-vm-public-ip>"
        ecr = "ACR"
    else:
        raise ValueError(f"Unexpected value for cloud_provider {cloud_provider}")

    install_parser = subparsers.add_parser(
        "install",
        help=f"Does one-time setup of {functions} that automatically periodically "
        "clean up unused temporary resources. Must be re-run when meadowrun is updated "
        f"so that {functions} pick up updated code.",
    )
    install_parser.add_argument(
        "--allow-authorize-ips",
        action="store_true",
        help=(
            "Users' machines need to be authorized to access Meadowrun-created "
            f"{vm_instances}. If this option is set, users will be given permissions "
            "to automatically authorize their IPs to SSH into Meadowrun-created "
            f"{vm_instances}. If this option is not set, an administrator must "
            "manually edit the Meadowrun security group to grant access for users."
        ),
    )
    if cloud_provider == "EC2":
        install_parser.add_argument(
            "--vpc-id",
            type=str,
            help=(
                "An optional argument to specify which VPC to set up the "
                "meadowrun_ssh_security_group in"
            ),
        )
        install_parser.add_argument(
            "--config-file",
            type=str,
            metavar="CONFIGFILENAME",
            help="Path to a config file. Use `config --defaults` to generate a file "
            "with the defaults, and `config --current` to download the currently used "
            "config.",
        )
        config_parser = subparsers.add_parser("config", help="Manage configuration.")
        get_set = config_parser.add_mutually_exclusive_group()
        get_set.add_argument(
            "--get",
            type=str,
            choices=["default", "current"],
            help="Create a file 'meadowrun_config.py' in the current directory, "
            "containing the default or current configuration.",
        )
        get_set.add_argument(
            "--set",
            type=str,
            metavar="CONFIGFILENAME",
            help="Upload the given file as the new configuration.",
        )

    subparsers.add_parser(
        "uninstall",
        help=f"Removes all {cloud_name} resources created by meadowrun",
    )

    clean_parser = subparsers.add_parser(
        "clean",
        help=f"Cleans up temporary resources: shuts down idle {vm_instances}, removes "
        "unused container images, queues, and caches. Running this with defaults is "
        "safe, but it can cause jobs to start slowly next time. Some additional options"
        " interrupt running jobs.",
    )
    active_idle = clean_parser.add_mutually_exclusive_group()
    active_idle.add_argument(
        "--active",
        "--clean-active",
        action="store_true",
        help=f"Additionally terminate {vm_instances} that are actively running jobs.",
    )
    active_idle.add_argument(
        "--idle",
        type=int,
        metavar="IDLE_TIME_SECS",
        help=f"Only terminate {vm_instances} if they've been idle for given time (in "
        "seconds).",
    )

    if cloud_provider == "EC2":
        clean_parser.add_argument(
            "--cache", action="store_true", help="Additionally clean up local caches."
        )

    if cloud_provider == "EC2":
        grant_permission_to_secret_parser = subparsers.add_parser(
            "grant-permission-to-secret",
            help=f"Gives the meadowrun {role} access to the specified {secret}",
        )
        grant_permission_to_secret_parser.add_argument(
            "secret_name", help=f"The name of the {secret} to give permissions to"
        )

        grant_permission_to_s3_bucket_parser = subparsers.add_parser(
            "grant-permission-to-s3-bucket",
            help=f"Gives the meadowrun {role} access to the specified S3 bucket",
        )
        grant_permission_to_s3_bucket_parser.add_argument(
            "bucket_name", help="The name of the bucket to give permissions to"
        )

        grant_permission_to_ecr_repo_parser = subparsers.add_parser(
            "grant-permission-to-ecr-repo",
            help=f"Gives the meadowrun {role} access to the specified ECR repository",
        )
        grant_permission_to_ecr_repo_parser.add_argument(
            "repo_name", help="The name of the repo to give permissions to"
        )

    get_ssh_key_parser = subparsers.add_parser(
        "get-ssh-key",
        help="Downloads the SSH key used to connect meadowrun-launched "
        f"{vm_instances}",
    )
    get_ssh_key_parser.add_argument(
        "--output",
        help="The path to write the SSH key to. If it is not provided, the default is "
        "~/.ssh/meadowrun_id_rsa. This can be used with e.g. `ssh -i "
        f"~/.ssh/meadowrun_id_rsa {ssh_user}@{vm_instance_address}`",
    )

    args = parser.parse_args()

    t0 = time.perf_counter()
    if args.command == "install":
        print("Creating resources for running meadowrun")
        if cloud_provider == "EC2":
            await aws.install(
                args.region,
                args.allow_authorize_ips,
                args.vpc_id,
                args.config_file,
            )
        elif cloud_provider == "AzureVM":
            await azure.install(region_name)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(f"Created resources in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "config":
        if args.set:
            print("Changing management lambda config")
            if cloud_provider == "EC2":
                await aws.edit_management_lambda_config(args.set, args.region)
            else:
                raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
            print(
                f"Changed management lambda config in {time.perf_counter() - t0:.2f} "
                "seconds"
            )
        elif args.get:
            custom_config = "custom_config.py"
            if args.get == "default":
                print(f"Creating default configuration as {custom_config}")
                aws.get_default_management_lambda_config(custom_config)
                got_file = True
            elif args.get == "current":
                print(f"Getting current configuration as {custom_config}")
                got_file = aws.get_current_management_lambda_config(
                    custom_config, args.region
                )
            if got_file:
                print(
                    f"Edit {custom_config} to your liking, then upload it with --set "
                    f"{custom_config}"
                )

    elif args.command == "uninstall":
        print("Deleting all meadowrun resources")
        if cloud_provider == "EC2":
            aws.delete_meadowrun_resources(args.region)
        elif cloud_provider == "AzureVM":
            await azure.delete_meadowrun_resource_group()
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted all meadowrun resources in {time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "clean":
        idle_time = (
            datetime.timedelta(seconds=args.idle)
            if args.idle
            else datetime.timedelta.min
        )

        if args.active:
            print(
                f"Terminating and deregistering all {vm_instances}. This will "
                f"interrupt actively running jobs."
            )
        elif args.idle:
            print(
                f"Terminating and deregistering {vm_instances} that have been idle for "
                f"longer than {idle_time}."
            )
        else:
            print(
                f"Terminating and deregistering all idle {vm_instances} (specify "
                "--active to also terminate and deregister active instances)"
            )

        if cloud_provider == "EC2":
            if args.active:
                aws.terminate_all_instances(args.region, False)

            await _deregister_and_terminate_instances(args.region, idle_time, [])
            if args.cache:
                print("Cleaning cache.")
                clear_prices_cache()
        elif cloud_provider == "AzureVM":
            resource_group_path = await ensure_meadowrun_resource_group(region_name)
            storage_account = await ensure_meadowrun_storage_account(
                region_name, "raise"
            )
            if args.active:
                await terminate_all_vms(resource_group_path)
            for log_line in await _deregister_and_terminate_vms(
                storage_account, resource_group_path, idle_time
            ):
                print(log_line)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Terminated and deregistered {vm_instances} in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused grid task queues")
        if cloud_provider == "EC2":
            aws_delete_old_task_queues(args.region)
        elif cloud_provider == "AzureVM":
            # set some environment variables as if we're running in an Azure Function
            storage_account = await ensure_meadowrun_storage_account(
                region_name, "create"
            )
            os.environ[MEADOWRUN_STORAGE_ACCOUNT_VARIABLE] = storage_account.name
            os.environ[MEADOWRUN_STORAGE_ACCOUNT_KEY_VARIABLE] = storage_account.key
            os.environ[MEADOWRUN_SUBSCRIPTION_ID] = await get_subscription_id()
            for log_line in await azure_delete_old_task_queues():
                print(log_line)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted unused grid task queues in {time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print(f"Deleting unused meadowrun-generated {ecr} images")
        if cloud_provider == "EC2":
            aws_delete_unused_images(args.region)
        elif cloud_provider == "AzureVM":
            for log_line in await azure_delete_unused_images():
                print(log_line)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted unused meadowrun-generated {ecr} images in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "grant-permission-to-secret":
        print(f"Granting access to the meadowrun {role} to access {args.secret_name}")
        if cloud_provider == "EC2":
            grant_permission_to_secret(args.secret_name, args.region)
        elif cloud_provider == "AzureVM":
            raise NotImplementedError(
                "Granting permission to individual secrets is not implemented for "
                "Azure. The meadowrun managed identity already has permissions to all "
                "secrets in the meadowrun-created Vault: "
                f"{await _ensure_meadowrun_vault(get_default_location())}"
            )
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(f"Granted access in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "grant-permission-to-s3-bucket":
        print(f"Granting access to the meadowrun {role} to access {args.bucket_name}")
        if cloud_provider == "EC2":
            grant_permission_to_s3_bucket(args.bucket_name, args.region)
        elif cloud_provider == "AzureVM":
            raise NotImplementedError(
                "Granting permission to S3 buckets is not implemented for Azure. The "
                "meadowrun managed identity already has permissions to all storage "
                f"accounts in the {MEADOWRUN_RESOURCE_GROUP_NAME} resource group"
            )
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(f"Granted access in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "grant-permission-to-ecr-repo":
        print(f"Granting access to the meadowrun {role} to access {args.repo_name}")
        if cloud_provider == "EC2":
            grant_permission_to_ecr_repo(args.repo_name, args.region)
        elif cloud_provider == "AzureVM":
            raise NotImplementedError(
                "Granting permission to ACR repos is not implemented for Azure. The "
                "meadowrun managed identity already has permissions to all storage "
                f"accounts in the {MEADOWRUN_RESOURCE_GROUP_NAME} resource group"
            )
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
    elif args.command == "get-ssh-key":
        if not args.output:
            output = os.path.expanduser(
                os.path.join("~", ".ssh", f"meadowrun_id_rsa.{cloud_provider}")
            )
        else:
            output = args.output
        print(f"Writing meadowrun ssh key to {output}")
        if cloud_provider == "EC2":
            ec2_download_ssh_key(output, args.region)
        elif cloud_provider == "AzureVM":
            await azure_download_ssh_key(output, region_name)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(f"Wrote meadowrun ssh key in {time.perf_counter() - t0:.2f} seconds")
    else:
        ValueError(f"Unrecognized command: {args.command}")


def main_ec2() -> None:
    asyncio.run(async_main("EC2"))


def main_azure_vm() -> None:
    asyncio.run(async_main("AzureVM"))
