import argparse
import asyncio

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_alloc import (
    ensure_ec2_alloc_lambda,
    ensure_clean_up_lambda,
    delete_meadowrun_resources,
    grant_permission_to_secret,
    _ensure_ec2_alloc_role,
)
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    deregister_all_inactive_instances,
)
from meadowrun.aws_integration.management_lambdas.clean_up import (
    delete_old_task_queues,
    delete_unused_images,
)


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
        help="Cleans up all temporary"
        " resources, runs the same code as the lambdas created by install",
    )

    parser_grant_permission_to_secret = subparsers.add_parser(
        "grant-permission-to-secret",
        help="Gives the meadowrun EC2 role access to the specified AWS secret",
    )
    parser_grant_permission_to_secret.add_argument(
        "secret_name", help="The name of the AWS secret to give permissions to"
    )

    args = parser.parse_args()

    region_name = await _get_default_region_name()

    if args.command == "install":
        print("Creating lambdas for cleaning up meadowrun resources")
        await ensure_ec2_alloc_lambda(True)
        await ensure_clean_up_lambda(True)
        _ensure_ec2_alloc_role(region_name)
    elif args.command == "uninstall":
        print("Deleting all meadowrun resources")
        delete_meadowrun_resources(region_name)
    elif args.command == "clean":
        print("Adjusting EC2 instances")
        deregister_all_inactive_instances(region_name)

        print("Deleting unused grid task queues")
        delete_old_task_queues(region_name)

        print("Deleting unused meadowrun-generated ECR images")
        delete_unused_images(region_name)
    elif args.command == "grant-permission-to-secret":
        print(f"Granting access to the meadowrun EC2 role to access {args.secret_name}")
        grant_permission_to_secret(args.secret_name, region_name)
    else:
        ValueError(f"Unrecognized command: {args.command}")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
