"""
Policies and roles needed for meadowrun to use EC2 instances and access meadowrun
resources from meadowrun-launched EC2 instances
"""

from __future__ import annotations

from typing import Any

import boto3

from meadowrun.aws_integration.aws_core import (
    _MEADOWRUN_USER_GROUP_NAME,
    _get_account_number,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    _MEADOWRUN_KEY_PAIR_SECRET_NAME,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _EC2_ALLOC_TABLE_NAME,
    _EC2_ALLOC_TAG,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
    ignore_boto3_error_code,
)


# an IAM role for Meadowrun-managed EC2 instances
_EC2_ROLE_NAME = "meadowrun_ec2_role"
_EC2_ROLE_INSTANCE_PROFILE = "meadowrun_ec2_role_instance_profile"
_EC2_ROLE_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {"Service": "ec2.amazonaws.com"}
        }
    ]
}"""
_EC2_ROLE_POLICY_NAME = "meadowrun_ec2_policy"

# the policy for the user group for meadowrun users
_MEADOWRUN_USER_POLICY_NAME = "meadowrun_user_policy"

# the IAM role that the management lambdas will run as
_MANAGEMENT_LAMBDA_ROLE = "meadowrun_management_lambda_role"
_MANAGEMENT_LAMBDA_POLICY_NAME = "meadowrun_management_lambda_policy"
_LAMBDA_ASSUME_ROLE_POLICY_DOCUMENT = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": { "Service": "lambda.amazonaws.com" }
        }
    ]
}"""

# A policy for granting access to a secret
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

# A policy for granting access to a bucket
_ACCESS_S3_BUCKET_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::$BUCKET_NAME"
        },
        {
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": "arn:aws:s3:::$BUCKET_NAME/*"
        }
    ]
}"""

# A policy for granting access to an ECR repo
_ACCESS_ECR_REPO_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ecr:BatchCheckLayerAvailability",
                "ecr:BatchDeleteImage",
                "ecr:BatchGetImage",
                "ecr:CompleteLayerUpload",
                "ecr:DescribeImageReplicationStatus",
                "ecr:DescribeImages",
                "ecr:DescribeRepositories",
                "ecr:GetDownloadUrlForLayer",
                "ecr:InitiateLayerUpload",
                "ecr:ListImages",
                "ecr:PutImage",
                "ecr:ReplicateImage",
                "ecr:UploadLayerPart"
            ],
            "Resource": "arn:aws:ecr:*:*:repository/$REPO_NAME"
        }
    ]
}"""

# A policy template that generally grants permissions needed to use Meadowrun
_MEADOWRUN_POLICY_TEMPLATE = """{
    "Version": "2012-10-17",
    "Statement": [

        # Create and terminate instances
        {
            "Sid": "ec2",
            "Effect": "Allow",
            "Action": [
                "ec2:TerminateInstances",
                "ec2:RequestSpotInstances",
                "ec2:CreateTags",
                "ec2:RunInstances"
            ],
            "Resource": [
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:spot-instances-request/*",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:security-group/%SECURITY_GROUP_ID%",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:network-interface/*",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:instance/*",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:volume/*",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:subnet/*",
                "arn:aws:ec2:*:%ACCOUNT_NUMBER%:key-pair/%MEADOWRUN_KEY_PAIR_NAME%",
                "arn:aws:ec2:*::image/*"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": "%MEADOWRUN_TAG%"
                }
            }
        },

        # Get and edit instance attributes, for modifying security groups
        {
            "Sid": "ec2attributes",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeInstanceAttribute",
                "ec2:ModifyInstanceAttribute"
            ],
            "Resource": "*",
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": "%MEADOWRUN_TAG%"
                }
            }
        },

        # Create EC2 instances with the meadowrun role
        {
            "Sid": "iamrole",
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::%ACCOUNT_NUMBER%:role/%MEADOWRUN_ROLE%"
        },

        # List instances. Needed for cleaning up running instances
        {
            "Sid": "ec2instances",
            "Effect": "Allow",
            "Action": "ec2:DescribeInstances",
            "Resource": "*",
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": "%MEADOWRUN_TAG%"
                }
            }
        },

        # Get the security group id for the meadowrun ssh group
        {
            "Sid": "securitygroups",
            "Effect": "Allow",
            "Action": "ec2:DescribeSecurityGroups",
            "Resource": "*"
        },

        %AUTHORIZE_IPS%

        # Get the meadowrun private SSH key secret
        {
            "Sid": "secrets",
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource":
            "arn:aws:secretsmanager:*:%ACCOUNT_NUMBER%:secret:%MEADOWRUN_SSH_KEY_SECRET%-*"
        },

        # Getting products and prices
        {
            "Sid": "prices",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeSpotPriceHistory",
                "pricing:GetProducts"
            ],
            "Resource": "*"
        },

        # Read/write the alloc table to keep track of instances/jobs
        {
            "Sid": "alloctable",
            "Effect": "Allow",
            "Action": [
                "dynamodb:BatchGetItem",
                "dynamodb:BatchWriteItem",
                "dynamodb:PutItem",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:Scan",
                "dynamodb:Query",
                "dynamodb:UpdateItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:%ACCOUNT_NUMBER%:table/%MEADOWRUN_ALLOC_TABLE_NAME%/index/*",
                "arn:aws:dynamodb:*:%ACCOUNT_NUMBER%:table/%MEADOWRUN_ALLOC_TABLE_NAME%"
            ]
        },

        # Read/write ECR for caching docker images
        {
            "Sid": "ecrmeadowrun",
            "Effect": "Allow",
            "Action": [
                "ecr:BatchCheckLayerAvailability",
                "ecr:BatchDeleteImage",
                "ecr:BatchGetImage",
                "ecr:CompleteLayerUpload",
                "ecr:DescribeImageReplicationStatus",
                "ecr:DescribeImages",
                "ecr:DescribeRepositories",
                "ecr:GetDownloadUrlForLayer",
                "ecr:InitiateLayerUpload",
                "ecr:ListImages",
                "ecr:PutImage",
                "ecr:ReplicateImage",
                "ecr:UploadLayerPart"
            ],
            "Resource": "arn:aws:ecr:*:*:repository/%MEADOWRUN_REPO%"
        },
        {
            "Sid": "ecrgeneral",
            "Effect": "Allow",
            "Action": [
                "ecr:BatchImportUpstreamImage",
                "ecr:GetAuthorizationToken"
            ],
            "Resource": "*"
        },

        # Read/write S3 for caching local code
        {
            "Sid": "s3buckets",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::meadowrun-*"
        },
        {
            "Sid": "s3objects",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::meadowrun-*/*"
        },

        # SQS for run_map support
        {
            "Sid": "meadowrunqueues",
            "Effect": "Allow",
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:CreateQueue",
                "sqs:DeleteMessage",
                "sqs:DeleteQueue",
                "sqs:GetQueueAttributes",
                "sqs:GetQueueUrl",
                "sqs:PurgeQueue",
                "sqs:ReceiveMessage",
                "sqs:SendMessage",
                "sqs:SetQueueAttributes",
                "sqs:TagQueue"
            ],
            "Resource": "arn:aws:sqs:*:%ACCOUNT_NUMBER%:meadowrun*",
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": "%MEADOWRUN_TAG%"
                }
            }
        },
        {
            "Sid": "queues",
            "Effect": "Allow",
            "Action": "sqs:ListQueues",
            "Resource": "*"
        }
    ]
}
"""

_AUTHORIZE_IPS_STATEMENT = """
        # Adding IP addresses to the meadowrun ssh security group. Needed so that we can
        # add our current ip
        {
            "Sid": "authorizeips",
            "Effect": "Allow",
            "Action": [
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:CreateSecurityGroup",
                "ec2:CreateTags"
            ],
            "Resource": "*",
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": "%MEADOWRUN_TAG%"
                }
            }
        },
"""


def _policy_arn_from_name(policy_name: str) -> str:
    return f"arn:aws:iam::{_get_account_number()}:policy/{policy_name}"


def _materialize_meadowrun_policy(
    security_group_id: str, allow_authorize_ips: bool, include_comments: bool
) -> str:
    policy = _MEADOWRUN_POLICY_TEMPLATE
    if allow_authorize_ips:
        policy = policy.replace("%AUTHORIZE_IPS%", _AUTHORIZE_IPS_STATEMENT)
    else:
        policy = policy.replace("%AUTHORIZE_IPS%", "")

    policy = (
        policy.replace("%SECURITY_GROUP_ID%", security_group_id)
        .replace("%MEADOWRUN_KEY_PAIR_NAME%", MEADOWRUN_KEY_PAIR_NAME)
        .replace("%MEADOWRUN_SSH_KEY_SECRET%", _MEADOWRUN_KEY_PAIR_SECRET_NAME)
        .replace("%ACCOUNT_NUMBER%", _get_account_number())
        .replace("%MEADOWRUN_ROLE%", _EC2_ROLE_NAME)
        .replace("%MEADOWRUN_TAG%", _EC2_ALLOC_TAG)
        .replace("%MEADOWRUN_ALLOC_TABLE_NAME%", _EC2_ALLOC_TABLE_NAME)
        .replace("%MEADOWRUN_REPO%", _MEADOWRUN_GENERATED_DOCKER_REPO)
    )
    if not include_comments:
        policy = "\n".join(
            line for line in policy.splitlines() if not line.lstrip().startswith("#")
        )

    return policy


def _create_or_update_policy(
    iam_client: Any, policy_name: str, policy_document: str
) -> None:
    policy_arn = _policy_arn_from_name(policy_name)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
    success, _ = ignore_boto3_error_code(
        lambda: iam_client.create_policy(
            PolicyName=policy_name,
            PolicyDocument=policy_document,
        ),
        "EntityAlreadyExists",
    )
    if not success:
        # If the policy already exists, we don't want to delete it because that requires
        # detaching it from roles/user groups. Instead, we need to create a new version
        # and delete the old version.
        existing_policy_version = iam_client.get_policy(PolicyArn=policy_arn)["Policy"][
            "DefaultVersionId"
        ]
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy_version
        iam_client.create_policy_version(
            PolicyArn=policy_arn, PolicyDocument=policy_document, SetAsDefault=True
        )
        iam_client.delete_policy_version(
            PolicyArn=policy_arn, VersionId=existing_policy_version
        )


def ensure_meadowrun_user_group(
    iam_client: Any, security_group_id: str, allow_authorize_ips: bool
) -> None:
    """
    Creates the meadowrun user group if it doesn't exist. If it does already exist, does
    not modify it, other than making sure the meadowrun user policy is attached to it.
    Creates or replaces the meadowrun user policy.
    """

    _create_or_update_policy(
        iam_client,
        _MEADOWRUN_USER_POLICY_NAME,
        _materialize_meadowrun_policy(security_group_id, allow_authorize_ips, False),
    )

    # create/update the role
    ignore_boto3_error_code(
        lambda: iam_client.create_group(GroupName=_MEADOWRUN_USER_GROUP_NAME),
        "EntityAlreadyExists",
    )
    iam_client.attach_group_policy(
        GroupName=_MEADOWRUN_USER_GROUP_NAME,
        PolicyArn=_policy_arn_from_name(_MEADOWRUN_USER_POLICY_NAME),
    )

    print(
        f"Created/updated user group {_MEADOWRUN_USER_GROUP_NAME}. Add users to this "
        "group to allow them to use Meadowrun with `aws iam add-user-to-group "
        f"--group-name {_MEADOWRUN_USER_GROUP_NAME} --user-name <username>`"
    )


def ensure_meadowrun_ec2_role(iam_client: Any, security_group_id: str) -> None:
    """
    Creates the meadowrun IAM role if it doesn't exist. If it does exist, does not
    modify it, other than making sure the meadowrun user policy is attached to it.
    Creates or replaces the meadowrun EC2 policy.

    Also makes sure that the corresponding instance profile exists.

    The meadowrun EC2 policy and the meadowrun user policy are basically identical for
    now, which makes sense because it doesn't "backdoor" any permissions for the
    meadowrun user inadvertently, and you might want to run another meadowrun job from a
    meadowrun worker. We create distinct policies here, though, so that it's easy to
    update them later on if we do want them to diverge.
    """

    _create_or_update_policy(
        iam_client,
        _EC2_ROLE_POLICY_NAME,
        _materialize_meadowrun_policy(security_group_id, False, False),
    )

    # create/update the role
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
    ignore_boto3_error_code(
        lambda: iam_client.create_role(
            RoleName=_EC2_ROLE_NAME,
            # allow EC2 instances to assume this role
            AssumeRolePolicyDocument=_EC2_ROLE_POLICY_DOCUMENT,
            Description="The role that meadowrun workers will run as",
        ),
        "EntityAlreadyExists",
    )
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.attach_role_policy
    iam_client.attach_role_policy(
        RoleName=_EC2_ROLE_NAME,
        PolicyArn=_policy_arn_from_name(_EC2_ROLE_POLICY_NAME),
    )

    # create an instance profile (so that EC2 instances can assume it) and attach
    # the role to the instance profile
    ignore_boto3_error_code(
        lambda: iam_client.create_instance_profile(
            InstanceProfileName=_EC2_ROLE_INSTANCE_PROFILE
        ),
        "EntityAlreadyExists",
    )
    ignore_boto3_error_code(
        lambda: iam_client.add_role_to_instance_profile(
            InstanceProfileName=_EC2_ROLE_INSTANCE_PROFILE,
            RoleName=_EC2_ROLE_NAME,
        ),
        "LimitExceeded",
    )


def ensure_management_lambda_role(iam_client: Any, security_group_id: str) -> None:
    """
    Creates the role for the meadowrun management lambdas to run as. If it already
    exists, does not modify it, other than making sure the management lambda policy is
    attached to it. Creates or replaces the management lambda policy.

    TODO this should really have its own policy with a subset of the meadowrun user
    policy's permissions
    """

    _create_or_update_policy(
        iam_client,
        _MANAGEMENT_LAMBDA_POLICY_NAME,
        _materialize_meadowrun_policy(security_group_id, False, False),
    )

    # create/update the role
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
    ignore_boto3_error_code(
        lambda: iam_client.create_role(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            # allow lambdas to assume this role
            AssumeRolePolicyDocument=_LAMBDA_ASSUME_ROLE_POLICY_DOCUMENT,
            Description="Role for meadowrun management lambdas",
        ),
        "EntityAlreadyExists",
    )
    iam_client.attach_role_policy(
        RoleName=_MANAGEMENT_LAMBDA_ROLE,
        PolicyArn=_policy_arn_from_name(_MANAGEMENT_LAMBDA_POLICY_NAME),
    )
    # allow writing CloudWatch logs
    iam_client.attach_role_policy(
        RoleName=_MANAGEMENT_LAMBDA_ROLE,
        PolicyArn="arn:aws:iam::aws:policy/service-role/" "AWSLambdaBasicExecutionRole",
    )


def grant_permission_to_secret(secret_name: str, region_name: str) -> None:
    """Grants permission to the meadowrun EC2 role to access the specified secret."""

    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    secret_arn = secrets_client.describe_secret(SecretId=secret_name)["ARN"]
    iam_client = boto3.client("iam", region_name=region_name)
    iam_client.put_role_policy(
        RoleName=_EC2_ROLE_NAME,
        PolicyName=f"AccessSecret_{secret_name}",
        PolicyDocument=_ACCESS_SECRET_POLICY.replace("$SECRET_ARN", secret_arn),
    )


def grant_permission_to_s3_bucket(bucket_name: str, region_name: str) -> None:
    iam_client = boto3.client("iam", region_name=region_name)
    iam_client.put_role_policy(
        RoleName=_EC2_ROLE_NAME,
        PolicyName=f"AccessS3Bucket_{bucket_name}",
        PolicyDocument=_ACCESS_S3_BUCKET_POLICY.replace("$BUCKET_NAME", bucket_name),
    )


def grant_permission_to_ecr_repo(repo_name: str, region_name: str) -> None:
    iam_client = boto3.client("iam", region_name=region_name)
    iam_client.put_role_policy(
        RoleName=_EC2_ROLE_NAME,
        PolicyName=f"AccessECRRepo_{repo_name}",
        PolicyDocument=_ACCESS_ECR_REPO_POLICY.replace("$REPO_NAME", repo_name),
    )
