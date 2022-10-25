from __future__ import annotations

import argparse
from typing import Any, Optional, Dict, List
import boto3

import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances
import meadowrun.aws_integration.management_lambdas.clean_up
from meadowrun.aws_integration.aws_core import (
    _MEADOWRUN_USER_GROUP_NAME,
    _get_current_ip_for_ssh,
    get_bucket_name,
)
from meadowrun.aws_integration.aws_mgmt_lambda_install import (
    _CLEAN_UP_LAMBDA_NAME,
    _CLEAN_UP_LAMBDA_SCHEDULE_RULE,
    _EC2_ALLOC_LAMBDA_NAME,
    _EC2_ALLOC_LAMBDA_SCHEDULE_RULE,
    ensure_clean_up_lambda,
    ensure_ec2_alloc_lambda,
)
from meadowrun.aws_integration.aws_permissions_install import (
    _EC2_ROLE_INSTANCE_PROFILE,
    _EC2_ROLE_NAME,
    _MANAGEMENT_LAMBDA_POLICY_NAME,
    _MANAGEMENT_LAMBDA_ROLE,
    _EC2_ROLE_POLICY_NAME,
    _MEADOWRUN_USER_POLICY_NAME,
    _policy_arn_from_name,
    ensure_management_lambda_role,
    ensure_meadowrun_ec2_role,
    ensure_meadowrun_user_group,
)
from meadowrun.aws_integration.ec2 import (
    _MEADOWRUN_SSH_SECURITY_GROUP,
    ensure_security_group,
)
from meadowrun.aws_integration.ec2_instance_allocation import ensure_ec2_alloc_table
from meadowrun.aws_integration.ec2_pricing import clear_prices_cache
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    _MEADOWRUN_KEY_PAIR_SECRET_NAME,
    ensure_meadowrun_key_pair,
)
from meadowrun.aws_integration.ecr import _ensure_repository
from meadowrun.aws_integration.management_lambdas.adjust_ec2_instances import (
    _get_non_terminated_instances,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    ignore_boto3_error_code,
)


def add_management_lambda_override_arguments(parser: argparse.ArgumentParser) -> None:
    """Modifies parser in place!"""
    parser.add_argument("--terminate-instances-if-idle-for-secs")


def get_management_lambda_overrides_from_args(args: Any) -> Dict[str, str]:
    overrides = {}
    if args.terminate_instances_if_idle_for_secs:
        overrides[
            "TERMINATE_INSTANCES_IF_IDLE_FOR_SECS"
        ] = args.terminate_instances_if_idle_for_secs
    return overrides


async def install(
    region_name: str,
    allow_authorize_ips: bool,
    vpc_id: Optional[str],
    management_lambda_overrides: Dict[str, str],
) -> None:
    """Installs resources needed to run Meadowrun jobs"""

    print(f"Installing Meadowrun in {region_name}")

    ensure_security_group(_MEADOWRUN_SSH_SECURITY_GROUP, region_name, vpc_id)

    if not allow_authorize_ips:
        print(
            "--allow-authorize-ips was set to false. This means you must manually edit "
            f"the {_MEADOWRUN_SSH_SECURITY_GROUP} security group to allow users to SSH "
            "over port 22 to that security group. For example, you can run `aws ec2 "
            "authorize-security-group-ingress --group-name "
            f"{_MEADOWRUN_SSH_SECURITY_GROUP} --protocol tcp --port 22 --cidr "
            f"{await _get_current_ip_for_ssh()}/32`"
        )

    iam_client = boto3.client("iam", region_name=region_name)
    ensure_meadowrun_user_group(iam_client, allow_authorize_ips)
    ensure_meadowrun_ec2_role(iam_client)
    ensure_management_lambda_role(iam_client)

    ensure_ec2_alloc_table(region_name)

    await ensure_ec2_alloc_lambda(True, management_lambda_overrides, region_name)
    await ensure_clean_up_lambda(True, management_lambda_overrides, region_name)

    ensure_meadowrun_key_pair(region_name)

    ensure_bucket(region_name)

    _ensure_repository(_MEADOWRUN_GENERATED_DOCKER_REPO, region_name)


async def edit_management_lambda_config(
    overrides: Dict[str, str], region_name: str
) -> None:
    await ensure_ec2_alloc_lambda(True, overrides, region_name)
    await ensure_clean_up_lambda(True, overrides, region_name)


def _delete_user_group(iam: Any, group_name: str) -> None:
    """
    Deletes an IAM user group, requires removing all users and detaching all policies
    """
    success, group = ignore_boto3_error_code(
        lambda: iam.get_group(GroupName=group_name), "NoSuchEntity"
    )
    if success:
        assert group is not None  # just for mypy
        for user in group["Users"]:
            iam.remove_user_from_group(GroupName=group_name, UserName=user["UserName"])

    success, attached_policies = ignore_boto3_error_code(
        lambda: iam.list_attached_group_policies(GroupName=group_name), "NoSuchEntity"
    )
    if success:
        assert attached_policies is not None  # just for mypy
        for attached_policy in attached_policies["AttachedPolicies"]:
            iam.detach_group_policy(
                GroupName=group_name, PolicyArn=attached_policy["PolicyArn"]
            )

    success, inline_policies = ignore_boto3_error_code(
        lambda: iam.list_group_policies(GroupName=group_name), "NoSuchEntity"
    )
    if success:
        assert inline_policies is not None  # just for mypy
        for inline_policy in inline_policies["PolicyNames"]:
            iam.delete_group_policy(GroupName=group_name, PolicyName=inline_policy)

    ignore_boto3_error_code(
        lambda: iam.delete_group(GroupName=group_name), "NoSuchEntity"
    )


def _delete_iam_role(iam: Any, role_name: str) -> None:
    """Deletes an IAM role, requires detaching all policies first"""
    success, attached_policies = ignore_boto3_error_code(
        lambda: iam.list_attached_role_policies(RoleName=role_name), "NoSuchEntity"
    )
    if success:
        assert attached_policies is not None  # just for mypy
        for attached_policy in attached_policies["AttachedPolicies"]:
            iam.detach_role_policy(
                RoleName=role_name, PolicyArn=attached_policy["PolicyArn"]
            )

    success, inline_policies = ignore_boto3_error_code(
        lambda: iam.list_role_policies(RoleName=role_name), "NoSuchEntity"
    )
    if success:
        assert inline_policies is not None  # just for mypy
        for inline_policy in inline_policies["PolicyNames"]:
            iam.delete_role_policy(RoleName=role_name, PolicyName=inline_policy)

    ignore_boto3_error_code(lambda: iam.delete_role(RoleName=role_name), "NoSuchEntity")


def _delete_policy(iam: Any, policy_name: str) -> None:
    policy_arn = _policy_arn_from_name(policy_name)

    # non-default versions need to be deleted explicitly
    success, versions = ignore_boto3_error_code(
        lambda: iam.list_policy_versions(PolicyArn=policy_arn), "NoSuchEntity"
    )
    if success:
        assert versions is not None  # just for mypy
        for version in versions["Versions"]:
            if not version["IsDefaultVersion"]:
                iam.delete_policy_version(
                    PolicyArn=policy_arn, VersionId=version["VersionId"]
                )
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=_policy_arn_from_name(policy_name)),
        "NoSuchEntity",
    )


def _delete_event_rule(events_client: Any, rule_name: str) -> None:
    """Deletes an EventBridge rule, requires deleting targets first"""
    success, target_ids = ignore_boto3_error_code(
        lambda: [
            target["Id"]
            for target in events_client.list_targets_by_rule(Rule=rule_name)["Targets"]
        ],
        "ResourceNotFoundException",
    )
    if success and target_ids:
        events_client.remove_targets(Rule=rule_name, Ids=target_ids)
    events_client.delete_rule(Name=rule_name)


def terminate_all_instances(region_name: str, wait: bool) -> None:
    """
    Terminates all Meadowrun-tagged instances, regardless of whether they are registered
    or not. If wait is true, we wait for the instances to terminate, otherwise we return
    "immediately". WARNING this will kill running jobs.
    """
    instances = _get_non_terminated_instances(
        boto3.resource("ec2", region_name=region_name)
    )
    for instance in instances:
        print(f"Terminating {instance.id}")
        instance.terminate()
    if wait:
        print("Waiting for instances to terminate cleanly")
        for instance in instances:
            instance.wait_until_terminated()


def delete_meadowrun_resources(region_name: str) -> None:
    """
    Delete all AWS resources that meadowrun creates

    This needs to contain all old names/types of resources in every published version of
    this library.
    """

    terminate_all_instances(region_name, True)

    meadowrun.aws_integration.management_lambdas.clean_up.delete_all_task_queues(
        region_name
    )

    delete_bucket(region_name)

    lambda_client = boto3.client("lambda", region_name=region_name)
    ignore_boto3_error_code(
        lambda: lambda_client.delete_function(FunctionName=_EC2_ALLOC_LAMBDA_NAME),
        "ResourceNotFoundException",
    )
    ignore_boto3_error_code(
        lambda: lambda_client.delete_function(FunctionName=_CLEAN_UP_LAMBDA_NAME),
        "ResourceNotFoundException",
    )

    logs_client = boto3.client("logs", region_name=region_name)
    ignore_boto3_error_code(
        lambda: logs_client.delete_log_group(
            logGroupName=f"/aws/lambda/{_EC2_ALLOC_LAMBDA_NAME}"
        ),
        "ResourceNotFoundException",
    )
    ignore_boto3_error_code(
        lambda: logs_client.delete_log_group(
            logGroupName=f"/aws/lambda/{_CLEAN_UP_LAMBDA_NAME}"
        ),
        "ResourceNotFoundException",
    )

    events_client = boto3.client("events", region_name=region_name)
    _delete_event_rule(events_client, _EC2_ALLOC_LAMBDA_SCHEDULE_RULE)
    _delete_event_rule(events_client, _CLEAN_UP_LAMBDA_SCHEDULE_RULE)

    ec2_client = boto3.client("ec2", region_name=region_name)
    ec2_client.delete_key_pair(KeyName=MEADOWRUN_KEY_PAIR_NAME)
    # TODO this will fail if there are unterminated instances using this security group.
    # We terminate all instances at the beginning of this function, but it takes time to
    # terminate them.
    ignore_boto3_error_code(
        lambda: ec2_client.delete_security_group(
            GroupName=_MEADOWRUN_SSH_SECURITY_GROUP
        ),
        "InvalidGroup.NotFound",
    )

    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    ignore_boto3_error_code(
        lambda: secrets_client.delete_secret(SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME),
        "ResourceNotFoundException",
    )

    dynamodb_client = boto3.client("dynamodb", region_name=region_name)
    ignore_boto3_error_code(
        lambda: dynamodb_client.delete_table(TableName=_EC2_ALLOC_TABLE_NAME),
        "ResourceNotFoundException",
    )

    ecr_client = boto3.client("ecr", region_name=region_name)
    ignore_boto3_error_code(
        lambda: ecr_client.delete_repository(
            repositoryName=_MEADOWRUN_GENERATED_DOCKER_REPO, force=True
        ),
        "RepositoryNotFoundException",
    )

    clear_prices_cache()

    print(
        "Deleting IAM resources. This may fail if you have installed Meadowrun in other"
        " regions. Non-IAM resources have been successfully deleted"
    )
    iam = boto3.client("iam", region_name=region_name)

    ignore_boto3_error_code(
        lambda: iam.remove_role_from_instance_profile(
            RoleName=_EC2_ROLE_NAME,
            InstanceProfileName=_EC2_ROLE_INSTANCE_PROFILE,
        ),
        "NoSuchEntity",
    )
    ignore_boto3_error_code(
        lambda: iam.delete_instance_profile(
            InstanceProfileName=_EC2_ROLE_INSTANCE_PROFILE
        ),
        "NoSuchEntity",
    )

    _delete_user_group(iam, _MEADOWRUN_USER_GROUP_NAME)
    _delete_iam_role(iam, _EC2_ROLE_NAME)
    _delete_iam_role(iam, _MANAGEMENT_LAMBDA_ROLE)

    _delete_policy(iam, _MEADOWRUN_USER_POLICY_NAME)
    _delete_policy(iam, _EC2_ROLE_POLICY_NAME)
    _delete_policy(iam, _MANAGEMENT_LAMBDA_POLICY_NAME)


def delete_bucket(region_name: str) -> None:
    """Deletes the meadowrun bucket"""
    s3 = boto3.resource("s3", region_name=region_name)

    bucket = s3.Bucket(get_bucket_name(region_name))
    success, _ = ignore_boto3_error_code(
        lambda: list(bucket.objects.limit(1)),
        "NoSuchBucket",
    )
    if success:
        # S3 doesn't allow deleting a bucket with anything in it, so delete all objects
        # in chunks of up to 1000, which is the maximum allowed.
        key_chunk: List[Dict[str, str]] = []
        for s3object in bucket.objects.all():
            if len(key_chunk) == 1000:
                bucket.delete_objects(Delete=dict(Objects=key_chunk))
                key_chunk.clear()
            key_chunk.append(dict(Key=s3object.key))
        if key_chunk:
            bucket.delete_objects(Delete=dict(Objects=key_chunk))

        bucket.delete()


def ensure_bucket(
    region_name: str,
    expire_days: int = 14,
) -> None:
    """
    Create an S3 bucket in a specified region if it does not exist yet.

    If a region is not specified, the bucket is created in the configured default
    region.

    The bucket is created with a default lifecycle policy of 14 days.

    Since bucket names must be globally unique, the name is bucket_prefix + region +
    account number

    :param bucket_name: Bucket to create
    :param region_name: String region to create bucket in, e.g., 'us-west-2'
    :param expire_days: int number of days after which keys are deleted by lifecycle
        policy.
    :return: the full bucket name
    """

    s3 = boto3.client("s3", region_name=region_name)

    bucket_name = get_bucket_name(region_name)

    # us-east-1 cannot be specified as a LocationConstraint because it is the default
    # region
    # https://stackoverflow.com/questions/51912072/invalidlocationconstraint-error-while-creating-s3-bucket-when-the-used-command-i
    if region_name == "us-east-1":
        additional_parameters = {}
    else:
        additional_parameters = {
            "CreateBucketConfiguration": {"LocationConstraint": region_name}
        }

    success, _ = ignore_boto3_error_code(
        lambda: s3.create_bucket(Bucket=bucket_name, **additional_parameters),
        "BucketAlreadyOwnedByYou",
    )
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=dict(
            Rules=[
                dict(
                    Expiration=dict(
                        Days=expire_days,
                    ),
                    ID="meadowrun-lifecycle-policy",
                    # Filter is mandatory, but we don't want one:
                    Filter=dict(Prefix=""),
                    Status="Enabled",
                )
            ]
        ),
    )
