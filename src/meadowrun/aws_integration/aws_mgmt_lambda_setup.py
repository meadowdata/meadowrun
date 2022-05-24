from __future__ import annotations

import asyncio
import io
import os
import pkgutil
import zipfile
from typing import Any, Tuple, Callable, TypeVar

import boto3

import meadowrun.aws_integration
from meadowrun.aws_integration.aws_core import (
    _get_account_number,
    _get_default_region_name,
    _iam_role_exists,
)
from meadowrun.aws_integration.ec2_alloc_role import (
    _CLEAN_UP_LAMBDA_NAME,
    _CLEAN_UP_LAMBDA_SCHEDULE_RULE,
    _EC2_ALLOC_LAMBDA_NAME,
    _EC2_ALLOC_LAMBDA_SCHEDULE_RULE,
    _MANAGEMENT_LAMBDA_ROLE,
    _ensure_ec2_alloc_table_access_policy,
    _ensure_meadowrun_ecr_access_policy,
    _ensure_meadowrun_sqs_access_policy,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    ignore_boto3_error_code,
)


_T = TypeVar("_T")

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


def _get_zipped_lambda_code() -> bytes:
    """
    Gets the contents of the ec2_alloc_lambda folder as a zip file. This is the code we
    want to run as a lambda.

    Warning, this doesn't recurse into any subdirectories (because it is not currently
    needed)
    """
    lambda_root_path = meadowrun.aws_integration.management_lambdas.__path__[0]
    module_names = [name for _, name, _ in pkgutil.iter_modules([lambda_root_path])]
    path_prefix = meadowrun.aws_integration.management_lambdas.__name__.replace(
        ".", os.path.sep
    )

    with io.BytesIO() as buffer:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for module_name in module_names:
                zf.write(
                    os.path.join(lambda_root_path, module_name + ".py"),
                    os.path.join(path_prefix, module_name + ".py"),
                )

        buffer.seek(0)

        return buffer.read()


def _ensure_management_lambda_role(region_name: str) -> None:
    """Creates the role for the ec2 alloc lambda to run as"""
    iam = boto3.client("iam", region_name=region_name)
    if not _iam_role_exists(iam, _MANAGEMENT_LAMBDA_ROLE):
        # create the role
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
        ignore_boto3_error_code(
            lambda: iam.create_role(
                RoleName=_MANAGEMENT_LAMBDA_ROLE,
                # allow EC2 instances to assume this role
                AssumeRolePolicyDocument=_LAMBDA_ASSUME_ROLE_POLICY_DOCUMENT,
                Description="Allows reading/writing the EC2 alloc table and "
                "creating/terminating EC2 instances",
            ),
            "EntityAlreadyExists",
        )

        # allow accessing the EC2 alloc dynamodb table
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.attach_role_policy
        iam.attach_role_policy(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            PolicyArn=_ensure_ec2_alloc_table_access_policy(iam),
        )

        # allow creating/terminating EC2 instances
        iam.attach_role_policy(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn="arn:aws:iam::aws:policy/AmazonEC2FullAccess",
        )

        # allow deleting SQS queues
        iam.attach_role_policy(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            PolicyArn=_ensure_meadowrun_sqs_access_policy(iam),
        )

        # allow deleting unused ECR images
        iam.attach_role_policy(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            PolicyArn=_ensure_meadowrun_ecr_access_policy(iam),
        )

        # allow writing CloudWatch logs
        # TODO also configure CloudWatch retention so that we don't keep everything
        # forever
        iam.attach_role_policy(
            RoleName=_MANAGEMENT_LAMBDA_ROLE,
            PolicyArn="arn:aws:iam::aws:policy/service-role/"
            "AWSLambdaBasicExecutionRole",
        )


async def _create_management_lambda(
    lambda_client: Any,
    lambda_handler: Any,
    lambda_name: str,
    schedule_rule_name: str,
    schedule_expression: str,
    region_name: str,
) -> None:
    """Creates the ec2 alloc lambda assuming it does not already exist"""
    account_number = _get_account_number()

    # create the lambda
    def create_function_if_not_exists() -> Tuple[bool, None]:
        ignore_boto3_error_code(
            lambda: lambda_client.create_function(
                FunctionName=lambda_name,
                Runtime="python3.9",
                Role=f"arn:aws:iam::{account_number}:role/{_MANAGEMENT_LAMBDA_ROLE}",
                Handler=f"{lambda_handler.__module__}.{lambda_handler.__name__}",
                Code={"ZipFile": _get_zipped_lambda_code()},
                Timeout=120,
                MemorySize=128,  # memory available in MB
            ),
            "ResourceConflictException",
        )
        return True, None

    # totally crazy, but sometimes you just have to wait 5-10 seconds after
    # creating the role to be able to create a lambda with that role:
    # https://stackoverflow.com/a/37438525
    await _retry(
        lambda: ignore_boto3_error_code(
            create_function_if_not_exists, "InvalidParameterValueException"
        ),
        10,
        2,
        "Waiting for newly created AWS IAM role to become available...",
    )

    # now create an EventBridge rule that triggers every 1 minute
    events_client = boto3.client("events")
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#EventBridge.Client.put_rule
    events_client.put_rule(
        Name=schedule_rule_name, ScheduleExpression=schedule_expression
    )

    # add the lambda as a target for that rule
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#EventBridge.Client.put_targets
    events_client.put_targets(
        Rule=schedule_rule_name,
        Targets=[
            {
                "Id": lambda_name,
                "Arn": f"arn:aws:lambda:{region_name}:{account_number}:function:"
                f"{lambda_name}",
            }
        ],
    )

    # add permissions for that rule to invoke this lambda
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.add_permission
    ignore_boto3_error_code(
        lambda: lambda_client.add_permission(
            FunctionName=lambda_name,
            StatementId=f"{schedule_rule_name}_invokes_{lambda_name}",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=(
                f"arn:aws:events:{region_name}:{account_number}:rule/"
                f"{schedule_rule_name}"
            ),
        ),
        "ResourceConflictException",
    )


def _set_retention_policy_lambda_logs(lambda_name: str, region_name: str) -> None:
    """
    Cloudwatch log groups are automatically created for lambdas when they are first run
    with an infinite retention period. This function preemptively creates the log group
    so that we can set the retention period.
    """
    logs_client = boto3.client("logs", region_name=region_name)
    ignore_boto3_error_code(
        lambda: logs_client.create_log_group(logGroupName=f"/aws/lambda/{lambda_name}"),
        "ResourceAlreadyExistsException",
    )
    logs_client.put_retention_policy(
        logGroupName=f"/aws/lambda/{lambda_name}", retentionInDays=3
    )


async def _ensure_management_lambda(
    lambda_handler: Any,
    lambda_name: str,
    schedule_rule_name: str,
    schedule_expression: str,
    update_if_exists: bool,
) -> None:
    """
    Create the specified management lambda if it doesn't exist. If update_if_exists is
    true, updates the code if the lambda already exists.

    Even if this is called with update_if_exists, it is not guaranteed to update the
    code if another process creates the lambda after this function starts executing.
    """

    region_name = await _get_default_region_name()
    lambda_client = boto3.client("lambda", region_name=region_name)

    exists, _ = ignore_boto3_error_code(
        lambda: lambda_client.get_function(FunctionName=lambda_name),
        "ResourceNotFoundException",
    )

    if not exists:
        # create the role that the lambda will run as
        _ensure_management_lambda_role(region_name)

        await _create_management_lambda(
            lambda_client,
            lambda_handler,
            lambda_name,
            schedule_rule_name,
            schedule_expression,
            region_name,
        )
        _set_retention_policy_lambda_logs(lambda_name, region_name)
    elif update_if_exists:
        lambda_client.update_function_code(
            FunctionName=lambda_name, ZipFile=_get_zipped_lambda_code()
        )
        _set_retention_policy_lambda_logs(lambda_name, region_name)


async def ensure_ec2_alloc_lambda(update_if_exists: bool = False) -> None:
    await _ensure_management_lambda(
        meadowrun.aws_integration.management_lambdas.adjust_ec2_instances.lambda_handler,  # noqa: E501
        _EC2_ALLOC_LAMBDA_NAME,
        _EC2_ALLOC_LAMBDA_SCHEDULE_RULE,
        "rate(1 minute)",
        update_if_exists,
    )


async def ensure_clean_up_lambda(update_if_exists: bool = False) -> None:
    await _ensure_management_lambda(
        meadowrun.aws_integration.management_lambdas.clean_up.lambda_handler,
        _CLEAN_UP_LAMBDA_NAME,
        _CLEAN_UP_LAMBDA_SCHEDULE_RULE,
        "rate(3 hours)",
        update_if_exists,
    )


async def _retry(
    function: Callable[[], Tuple[bool, _T]],
    max_num_attempts: int = 3,
    delay_seconds: float = 1,
    retry_message: str = "Retrying on error",
) -> _T:
    i = 0
    while True:
        success, result = function()
        if success:
            return result
        else:
            i += 1
            if i >= max_num_attempts:
                raise ValueError(f"Failed after {i} attempts")
            else:
                print(retry_message)
                await asyncio.sleep(delay_seconds)
