import argparse
import asyncio
import os.path
import time

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_mgmt_lambda_setup import (
    ensure_ec2_alloc_lambda,
    ensure_clean_up_lambda,
)
from meadowrun.aws_integration.ec2_alloc import (
    grant_permission_to_secret,
    _ensure_ec2_alloc_role,
)
from meadowrun.aws_integration.aws_uninstall import delete_meadowrun_resources
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    deregister_all_inactive_instances,
)
from meadowrun.aws_integration.management_lambdas.clean_up import (
    delete_old_task_queues,
    delete_unused_images,
)
from meadowrun.aws_integration.ec2_ssh_keys import download_ssh_key


async def async_main() -> None:
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser(
        "install",
        help="Does one-time setup of AWS lambdas that automatically periodically clean "
        "up unused temporary resources. Must be re-run when meadowrun is updated "
        "so that lambdas pick up updated code.",
    )

    subparsers.add_parser(
        "uninstall",
        help="Removes all AWS resources created by meadowrun (e.g. lambdas, IAM roles)",
    )

    subparsers.add_parser(
        "clean",
        help="Cleans up all temporary resources, runs the same code as the lambdas "
        "created by install",
    )

    parser_grant_permission_to_secret = subparsers.add_parser(
        "grant-permission-to-secret",
        help="Gives the meadowrun EC2 role access to the specified AWS secret",
    )
    parser_grant_permission_to_secret.add_argument(
        "secret_name", help="The name of the AWS secret to give permissions to"
    )

    get_ssh_key_parser = subparsers.add_parser(
        "get-ssh-key",
        help="Downloads the SSH key used to connect meadowrun-launched EC2 instances.",
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
        print("Creating lambdas for cleaning up meadowrun resources")
        await ensure_ec2_alloc_lambda(True)
        await ensure_clean_up_lambda(True)
        _ensure_ec2_alloc_role(region_name)
        print(f"Created lambdas in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "uninstall":
        print("Deleting all meadowrun resources")
        delete_meadowrun_resources(region_name)
        print(
            f"Deleted all meadowrun resources in {time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "clean":
        print("Terminating all inactive EC2 instances")
        deregister_all_inactive_instances(region_name)
        print(
            "Terminated all inactive EC2 instances in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused grid task queues")
        delete_old_task_queues(region_name)
        print(
            f"Deleted unused grid task queues in {time.perf_counter() - t0:.2f} seconds"
        )
        t0 = time.perf_counter()

        print("Deleting unused meadowrun-generated ECR images")
        delete_unused_images(region_name)
        print(
            "Deleted unused meadowrun-generated ECR images in "
            f"{time.perf_counter() - t0:.2f} seconds"
        )
    elif args.command == "grant-permission-to-secret":
        print(f"Granting access to the meadowrun EC2 role to access {args.secret_name}")
        grant_permission_to_secret(args.secret_name, region_name)
        print(f"Granted access in {time.perf_counter() - t0:.2f} seconds")
    elif args.command == "get-ssh-key":
        if not args.output:
            output = os.path.expanduser(os.path.join("~", ".ssh", "meadowrun_id_rsa"))
        else:
            output = args.output
        print(f"Writing meadowrun ssh key to {output}")
        download_ssh_key(output, region_name)
        print(f"Wrote meadowrun ssh key in {time.perf_counter() - t0:.2f} seconds")
    else:
        ValueError(f"Unrecognized command: {args.command}")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
