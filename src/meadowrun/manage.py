import argparse
import asyncio
import datetime
import os.path
import time

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_mgmt_lambda_setup import (
    ensure_ec2_alloc_lambda,
    ensure_clean_up_lambda,
)
from meadowrun.aws_integration.aws_uninstall import delete_meadowrun_resources
from meadowrun.aws_integration.ec2_alloc_role import (
    grant_permission_to_secret,
    _ensure_ec2_alloc_role,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    download_ssh_key as ec2_download_ssh_key,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _deregister_and_terminate_instances,
    terminate_all_instances,
)
from meadowrun.aws_integration.management_lambdas.clean_up import (
    delete_old_task_queues as aws_delete_old_task_queues,
    delete_unused_images as aws_delete_unused_images,
)
from meadowrun.azure_integration.azure_meadowrun_core import (
    delete_meadowrun_resource_group,
    ensure_meadowrun_resource_group,
    ensure_meadowrun_storage_account,
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.azure_mgmt_functions_setup import (
    create_or_update_mgmt_function,
)
from meadowrun.azure_integration.azure_ssh_keys import (
    download_ssh_key as azure_download_ssh_key,
    _ensure_meadowrun_vault,
)
from meadowrun.azure_integration.mgmt_functions.azure_constants import (
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
from meadowrun.run_job_core import CloudProviderType


async def async_main(cloud_provider: CloudProviderType) -> None:
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="command")

    # equivalent language for logging
    if cloud_provider == "EC2":
        lambdas = "AWS lambdas"
        aws = "AWS"
        ec2_role = "EC2 role"
        secret = "AWS secret"
        ec2_instances = "EC2 instances"
    elif cloud_provider == "AzureVM":
        lambdas = "Azure Functions"
        aws = "Azure"
        ec2_role = "managed identity"
        secret = "Azure secret"
        ec2_instances = "Azure VMs"
    else:
        raise ValueError(f"Unexpected value for cloud_provider {cloud_provider}")

    subparsers.add_parser(
        "install",
        help=f"Does one-time setup of {lambdas} that automatically periodically clean "
        "up unused temporary resources. Must be re-run when meadowrun is updated "
        f"so that {lambdas} pick up updated code.",
    )

    subparsers.add_parser(
        "uninstall",
        help=f"Removes all {aws} resources created by meadowrun",
    )

    clean_parser = subparsers.add_parser(
        "clean",
        help=f"Cleans up all temporary resources, runs the same code as the {lambdas} "
        "created by install",
    )
    clean_parser.add_argument("--clean-active", action="store_true")

    grant_permission_to_secret_parser = subparsers.add_parser(
        "grant-permission-to-secret",
        help=f"Gives the meadowrun {ec2_role} access to the specified {secret}",
    )
    grant_permission_to_secret_parser.add_argument(
        "secret_name", help=f"The name of the {secret} to give permissions to"
    )

    get_ssh_key_parser = subparsers.add_parser(
        "get-ssh-key",
        help="Downloads the SSH key used to connect meadowrun-launched "
        f"{ec2_instances}",
    )
    get_ssh_key_parser.add_argument(
        "--output",
        help="The path to write the SSH key to. If it is not provided, the default is "
        "~/.ssh/meadowrun_id_rsa. This can be used with e.g. `ssh -i "
        "~/.ssh/meadowrun_id_rsa ubuntu@<ec2-address>`",
    )

    args = parser.parse_args()

    if cloud_provider == "EC2":
        region_name = await _get_default_region_name()
    elif cloud_provider == "AzureVM":
        region_name = get_default_location()
    else:
        raise ValueError(f"Unexpected cloud_provider {cloud_provider}")

    t0 = time.perf_counter()
    if args.command == "install":
        print(f"Creating {lambdas} for cleaning up meadowrun resources")
        if cloud_provider == "EC2":
            await ensure_ec2_alloc_lambda(True)
            await ensure_clean_up_lambda(True)
            _ensure_ec2_alloc_role(region_name)
        elif cloud_provider == "AzureVM":
            await create_or_update_mgmt_function(get_default_location())
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(f"Created {lambdas} in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "uninstall":
        print("Deleting all meadowrun resources")
        if cloud_provider == "EC2":
            delete_meadowrun_resources(region_name)
        elif cloud_provider == "AzureVM":
            await delete_meadowrun_resource_group()
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted all meadowrun resources in {time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "clean":
        if args.clean_active:
            print(
                f"Terminating and deregistering all {ec2_instances}. This will "
                f"interrupt actively running jobs."
            )
        else:
            print(
                f"Terminating and deregistering all inactive {ec2_instances} (specify "
                "--clean-active to also terminate and deregister active instances)"
            )

        if cloud_provider == "EC2":
            if args.clean_active:
                terminate_all_instances(region_name)
            _deregister_and_terminate_instances(region_name, datetime.timedelta.min)
        elif cloud_provider == "AzureVM":
            resource_group_path = await ensure_meadowrun_resource_group(region_name)
            storage_account = await ensure_meadowrun_storage_account(
                region_name, "raise"
            )
            if args.clean_active:
                await terminate_all_vms(resource_group_path)
            for log_line in await _deregister_and_terminate_vms(
                storage_account, resource_group_path, datetime.timedelta.min
            ):
                print(log_line)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Terminated and deregistered {ec2_instances} in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused grid task queues")
        if cloud_provider == "EC2":
            aws_delete_old_task_queues(region_name)
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

        print("Deleting unused meadowrun-generated ECR images")
        if cloud_provider == "EC2":
            aws_delete_unused_images(region_name)
        elif cloud_provider == "AzureVM":
            for log_line in await azure_delete_unused_images():
                print(log_line)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            "Deleted unused meadowrun-generated ECR images in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "grant-permission-to-secret":
        print(
            f"Granting access to the meadowrun {ec2_role} to access {args.secret_name}"
        )
        if cloud_provider == "EC2":
            grant_permission_to_secret(args.secret_name, region_name)
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
    elif args.command == "get-ssh-key":
        if not args.output:
            output = os.path.expanduser(os.path.join("~", ".ssh", "meadowrun_id_rsa"))
        else:
            output = args.output
        print(f"Writing meadowrun ssh key to {output}")
        if cloud_provider == "EC2":
            ec2_download_ssh_key(output, region_name)
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
