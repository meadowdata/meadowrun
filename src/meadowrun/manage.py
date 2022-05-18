import argparse
import asyncio
import os.path
import time

from azure.mgmt.compute.aio import ComputeManagementClient

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_mgmt_lambda_setup import (
    ensure_ec2_alloc_lambda,
    ensure_clean_up_lambda,
)
from meadowrun.aws_integration.aws_uninstall import delete_meadowrun_resources
from meadowrun.aws_integration.ec2_alloc import (
    grant_permission_to_secret,
    _ensure_ec2_alloc_role,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    download_ssh_key as ec2_download_ssh_key,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    deregister_all_inactive_instances,
)
from meadowrun.aws_integration.management_lambdas.clean_up import (
    delete_old_task_queues,
    delete_unused_images,
)
from meadowrun.azure_integration.azure_core import (
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.azure_mgmt_functions_setup import (
    create_or_update_mgmt_function,
)
from meadowrun.azure_integration.azure_ssh_keys import (
    download_ssh_key as azure_download_ssh_key,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    get_credential_aio,
)
from meadowrun.azure_integration.mgmt_functions.vm_adjust import terminate_all_vms
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

    subparsers.add_parser(
        "clean",
        help=f"Cleans up all temporary resources, runs the same code as the {lambdas} "
        "created by install",
    )

    parser_grant_permission_to_secret = subparsers.add_parser(
        "grant-permission-to-secret",
        help=f"Gives the meadowrun {ec2_role} access to the specified {secret}",
    )
    parser_grant_permission_to_secret.add_argument(
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

    region_name = await _get_default_region_name()

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
            # TODO not implemented
            pass
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted all meadowrun resources in {time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "clean":
        print(f"Terminating all inactive {ec2_instances}")
        if cloud_provider == "EC2":
            deregister_all_inactive_instances(region_name)
        elif cloud_provider == "AzureVM":
            async with ComputeManagementClient(
                get_credential_aio(), await get_subscription_id()
            ) as compute_client:
                await terminate_all_vms(compute_client)
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Terminated all inactive {ec2_instances} in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused grid task queues")
        if cloud_provider == "EC2":
            delete_old_task_queues(region_name)
        elif cloud_provider == "AzureVM":
            # TODO implement
            pass
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            f"Deleted unused grid task queues in {time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused meadowrun-generated ECR images")
        if cloud_provider == "EC2":
            delete_unused_images(region_name)
        elif cloud_provider == "AzureVM":
            # TODO implement
            pass
        else:
            raise ValueError(f"Unexpected cloud_provider {cloud_provider}")
        print(
            "Deleted unused meadowrun-generated ECR images in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "grant-permission-to-secret":
        print(f"Granting access to the meadowrun EC2 role to access {args.secret_name}")
        if cloud_provider == "EC2":
            grant_permission_to_secret(args.secret_name, region_name)
        elif cloud_provider == "AzureVM":
            # TODO implement
            pass
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
