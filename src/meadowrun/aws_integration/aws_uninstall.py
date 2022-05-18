from typing import Any

import boto3

import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances
import meadowrun.aws_integration.management_lambdas.clean_up
from meadowrun.aws_integration.ec2 import _MEADOWRUN_SSH_SECURITY_GROUP
from meadowrun.aws_integration.ec2_alloc_role import (
    _CLEAN_UP_LAMBDA_NAME,
    _CLEAN_UP_LAMBDA_SCHEDULE_RULE,
    _EC2_ALLOC_LAMBDA_NAME,
    _EC2_ALLOC_LAMBDA_SCHEDULE_RULE,
    _EC2_ALLOC_ROLE,
    _EC2_ALLOC_ROLE_INSTANCE_PROFILE,
    _MANAGEMENT_LAMBDA_ROLE,
    _ensure_ec2_alloc_table_access_policy,
    _ensure_meadowrun_ecr_access_policy,
    _ensure_meadowrun_sqs_access_policy,
    _ensure_s3_access_policy,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    _MEADOWRUN_KEY_PAIR_SECRET_NAME,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    ignore_boto3_error_code,
)
import meadowrun.aws_integration.s3


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


def delete_meadowrun_resources(region_name: str) -> None:
    """
    Delete all AWS resources that meadowrun creates

    This needs to contain all old names/types of resources in every published version of
    this library.
    """

    meadowrun.aws_integration.management_lambdas.adjust_ec2_instances.terminate_all_instances(  # noqa: E501
        region_name
    )

    meadowrun.aws_integration.management_lambdas.clean_up.delete_all_task_queues(
        region_name
    )

    meadowrun.aws_integration.s3.delete_all_buckets(region_name)

    iam = boto3.client("iam", region_name=region_name)

    ignore_boto3_error_code(
        lambda: iam.remove_role_from_instance_profile(
            RoleName=_EC2_ALLOC_ROLE,
            InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
        ),
        "NoSuchEntity",
    )
    ignore_boto3_error_code(
        lambda: iam.delete_instance_profile(
            InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE
        ),
        "NoSuchEntity",
    )

    _delete_iam_role(iam, _EC2_ALLOC_ROLE)
    _delete_iam_role(iam, _MANAGEMENT_LAMBDA_ROLE)

    table_access_policy_arn = _ensure_ec2_alloc_table_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=table_access_policy_arn), "NoSuchEntity"
    )

    sqs_access_policy_arn = _ensure_meadowrun_sqs_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=sqs_access_policy_arn), "NoSuchEntity"
    )

    ecr_access_policy_arn = _ensure_meadowrun_ecr_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=ecr_access_policy_arn), "NoSuchEntity"
    )

    s3_access_policy_arn = _ensure_s3_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=s3_access_policy_arn), "NoSuchEntity"
    )

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
