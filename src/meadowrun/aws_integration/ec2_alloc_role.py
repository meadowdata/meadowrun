"""
Policies and roles needed for meadowrun to use EC2 instances and access meadowrun
resources from meadowrun-launched EC2 instances
"""

from __future__ import annotations

from typing import Any

import boto3

from meadowrun.aws_integration.aws_core import _get_account_number, _iam_role_exists
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    ignore_boto3_error_code,
)


# an IAM role/an associated policy that grants permission to read/write the EC2 alloc
# dynamodb table
_EC2_ALLOC_ROLE = "meadowrun_ec2_alloc_role"
_EC2_ALLOC_ROLE_INSTANCE_PROFILE = "meadowrun_ec2_alloc_role_instance_profile"
_EC2_ASSUME_ROLE_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"}
        }
    ]
}"""
# a policy that grants read/write to the dynamodb table that registers EC2
# instances/jobs
_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME = "meadowrun_ec2_alloc_table_access"
_EC2_TABLE_ACCESS_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SpecificTable",
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchGet*",
                "dynamodb:DescribeStream",
                "dynamodb:DescribeTable",
                "dynamodb:Get*",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:BatchWrite*",
                "dynamodb:CreateTable",
                "dynamodb:Delete*",
                "dynamodb:Update*",
                "dynamodb:PutItem"
            ],
            "Resource": "arn:aws:dynamodb:*:*:table/$TABLE_NAME"
        }
    ]
}""".replace(
    "$TABLE_NAME", _EC2_ALLOC_TABLE_NAME
)
# a policy that grants read/write to SQS queues starting with meadowrun*. This is
# really for grid_task_queue.py functionality
_MEADOWRUN_SQS_ACCESS_POLICY_NAME = "meadowrun_sqs_access"
_MEADOWRUN_SQS_ACCESS_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "sqs:DeleteMessage",
                "sqs:GetQueueUrl",
                "sqs:ListDeadLetterSourceQueues",
                "sqs:ChangeMessageVisibility",
                "sqs:PurgeQueue",
                "sqs:ReceiveMessage",
                "sqs:DeleteQueue",
                "sqs:SendMessage",
                "sqs:GetQueueAttributes",
                "sqs:ListQueueTags",
                "sqs:CreateQueue",
                "sqs:SetQueueAttributes"
            ],
            "Resource": "arn:aws:sqs:*:*:meadowrun*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "sqs:ListQueues",
            "Resource": "*"
        }
    ]
}"""

_MEADOWRUN_ECR_ACCESS_POLICY_NAME = "meadowrun_ecr_access"
_MEADOWRUN_ECR_ACCESS_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "ecr:GetRegistryPolicy",
                "ecr:DescribeRegistry",
                "ecr:DescribePullThroughCacheRules",
                "ecr:GetAuthorizationToken",
                "ecr:PutRegistryScanningConfiguration",
                "ecr:DeleteRegistryPolicy",
                "ecr:CreatePullThroughCacheRule",
                "ecr:DeletePullThroughCacheRule",
                "ecr:PutRegistryPolicy",
                "ecr:GetRegistryScanningConfiguration",
                "ecr:PutReplicationConfiguration"
            ],
            "Resource": "*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "ecr:*",
            "Resource": "arn:aws:ecr:*:*:repository/$REPO_NAME"
        }
    ]
}""".replace(
    "$REPO_NAME", _MEADOWRUN_GENERATED_DOCKER_REPO
)

_ACCESS_SECRET_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "$SECRET_ARN"
        }
    ]
}"""

_MEADOWRUN_S3_ACCESS_POLICY_NAME = "meadowrun_s3_access"
_MEADOWRUN_S3_ACCESS_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::meadowrun-*"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::meadowrun-*/*"
        }
    ]
}
"""


# the name of the lambda that runs adjust_ec2_instances.py
_EC2_ALLOC_LAMBDA_NAME = "meadowrun_ec2_alloc_lambda"
# the EventBridge rule that triggers the lambda
_EC2_ALLOC_LAMBDA_SCHEDULE_RULE = "meadowrun_ec2_alloc_lambda_schedule_rule"

# the name of the lambda that runs clean_up.py
_CLEAN_UP_LAMBDA_NAME = "meadowrun_clean_up"
_CLEAN_UP_LAMBDA_SCHEDULE_RULE = "meadowrun_clean_up_lambda_schedule_rule"

# the role that these lambdas run as
_MANAGEMENT_LAMBDA_ROLE = "meadowrun_management_lambda_role"


def _ensure_meadowrun_sqs_access_policy(iam_client: Any) -> str:
    """
    Creates a policy that gives permission to read/write SQS queues for use with
    grid_task_queue.py
    """
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
    ignore_boto3_error_code(
        lambda: iam_client.create_policy(
            PolicyName=_MEADOWRUN_SQS_ACCESS_POLICY_NAME,
            PolicyDocument=_MEADOWRUN_SQS_ACCESS_POLICY_DOCUMENT,
        ),
        "EntityAlreadyExists",
    )
    return (
        f"arn:aws:iam::{_get_account_number()}:policy/"
        f"{_MEADOWRUN_SQS_ACCESS_POLICY_NAME}"
    )


def _ensure_meadowrun_ecr_access_policy(iam_client: Any) -> str:
    """
    Creates a policy that gives permission to read/write ECR repositories for use with
    deployment_manager.py
    """
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
    ignore_boto3_error_code(
        lambda: iam_client.create_policy(
            PolicyName=_MEADOWRUN_ECR_ACCESS_POLICY_NAME,
            PolicyDocument=_MEADOWRUN_ECR_ACCESS_POLICY_DOCUMENT,
        ),
        "EntityAlreadyExists",
    )
    return (
        f"arn:aws:iam::{_get_account_number()}:policy/"
        f"{_MEADOWRUN_ECR_ACCESS_POLICY_NAME}"
    )


def _ensure_ec2_alloc_table_access_policy(iam_client: Any) -> str:
    """Creates a policy that gives permission to read/write the EC2 alloc table"""
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
    ignore_boto3_error_code(
        lambda: iam_client.create_policy(
            PolicyName=_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME,
            PolicyDocument=_EC2_TABLE_ACCESS_POLICY_DOCUMENT,
        ),
        "EntityAlreadyExists",
    )
    return (
        f"arn:aws:iam::{_get_account_number()}:policy/"
        f"{_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME}"
    )


def _ensure_s3_access_policy(iam_client: Any) -> str:
    """Creates a policy that gives permission to list and read meadowrun S3 buckets"""
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
    ignore_boto3_error_code(
        lambda: iam_client.create_policy(
            PolicyName=_MEADOWRUN_S3_ACCESS_POLICY_NAME,
            PolicyDocument=_MEADOWRUN_S3_ACCESS_POLICY_DOCUMENT,
        ),
        "EntityAlreadyExists",
    )
    return (
        f"arn:aws:iam::{_get_account_number()}:policy/"
        f"{_MEADOWRUN_S3_ACCESS_POLICY_NAME}"
    )


def _ensure_ec2_alloc_role(region_name: str) -> None:
    """
    Creates the meadowrun EC2 alloc IAM role if it doesn't exist, and gives it
    permissions to read/write to the EC2 alloc table. The agents need this so that they
    can deallocate jobs when they finish.

    TODO does not try to update the role if/when we change the policies below in code
    """

    iam = boto3.client("iam", region_name=region_name)
    if not _iam_role_exists(iam, _EC2_ALLOC_ROLE):
        # create the role
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
        ignore_boto3_error_code(
            lambda: iam.create_role(
                RoleName=_EC2_ALLOC_ROLE,
                # allow EC2 instances to assume this role
                AssumeRolePolicyDocument=_EC2_ASSUME_ROLE_POLICY_DOCUMENT,
                Description="Allows reading/writing the EC2 alloc table",
            ),
            "EntityAlreadyExists",
        )

        # create the table access policy and attach it to the role
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.attach_role_policy
        iam.attach_role_policy(
            RoleName=_EC2_ALLOC_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn=_ensure_ec2_alloc_table_access_policy(iam),
        )

        # create the sqs access policy and attach it to the role
        iam.attach_role_policy(
            RoleName=_EC2_ALLOC_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn=_ensure_meadowrun_sqs_access_policy(iam),
        )

        # create the ecr access policy and attach it to the role
        iam.attach_role_policy(
            RoleName=_EC2_ALLOC_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn=_ensure_meadowrun_ecr_access_policy(iam),
        )

        # create the S3 access policy and attach it to the role
        iam.attach_role_policy(
            RoleName=_EC2_ALLOC_ROLE,
            PolicyArn=_ensure_s3_access_policy(iam),
        )

        # create an instance profile (so that EC2 instances can assume it) and attach
        # the role to the instance profile
        ignore_boto3_error_code(
            lambda: iam.create_instance_profile(
                InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE
            ),
            "EntityAlreadyExists",
        )
        ignore_boto3_error_code(
            lambda: iam.add_role_to_instance_profile(
                InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
                RoleName=_EC2_ALLOC_ROLE,
            ),
            "LimitExceeded",
        )


def grant_permission_to_secret(secret_name: str, region_name: str) -> None:
    """Grants permission to the meadowrun EC2 role to access the specified secret."""

    _ensure_ec2_alloc_role(region_name)

    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    secret_arn = secrets_client.describe_secret(SecretId=secret_name)["ARN"]
    iam_client = boto3.client("iam", region_name=region_name)
    iam_client.put_role_policy(
        RoleName=_EC2_ALLOC_ROLE,
        PolicyName=f"AccessSecret_{secret_name}",
        PolicyDocument=_ACCESS_SECRET_POLICY.replace("$SECRET_ARN", secret_arn),
    )
