from __future__ import annotations

import asyncio
import io
import os
import pkgutil
import zipfile
from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, TypeVar

import boto3

import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances
import meadowrun.aws_integration.management_lambdas.clean_up
from meadowrun.aws_integration.aws_permissions_install import _MANAGEMENT_LAMBDA_ROLE
from meadowrun.aws_integration.boto_utils import ignore_boto3_error_code
from meadowrun.shared import create_zipfile

if TYPE_CHECKING:
    from types import ModuleType

    from mypy_boto3_lambda import LambdaClient


_T = TypeVar("_T")

# the name of the lambda that runs adjust_ec2_instances.py
_EC2_ALLOC_LAMBDA_NAME = "meadowrun_ec2_alloc_lambda"
# the EventBridge rule that triggers the lambda
_EC2_ALLOC_LAMBDA_SCHEDULE_RULE = "meadowrun_ec2_alloc_lambda_schedule_rule"
# the name of the lambda that runs clean_up.py
_CLEAN_UP_LAMBDA_NAME = "meadowrun_clean_up"
_CLEAN_UP_LAMBDA_SCHEDULE_RULE = "meadowrun_clean_up_lambda_schedule_rule"

# Constants to identify the lambda layer with the lambda code's dependencies.
# The account where the layer is published, NOT the user account.
_LAMBDA_LAYER_ACCOUNT = "344606234287"
# The name of the layer.
_LAMBDA_LAYER_NAME = "meadowrun-dependencies"
# The version of the layer - this is printed by build_aws_lambda_layer.
# Since AWS never forgets the version for a given layer name, it's likely that
# us-east-2 is going to be ahead.
_LAMBDA_LAYER_DEFAULT_VERSION = "1"
_LAMBDA_LAYER_REGION_TO_VERSION = {"us-east-2": "3"}


def _get_prefix_and_root_path(module_name: ModuleType) -> Tuple[str, str]:
    return (module_name.__name__.replace(".", os.path.sep), module_name.__path__[0])


def _get_zipped_lambda_code(config_file: Optional[str]) -> bytes:
    """
    Gets the contents of the ec2_alloc_lambda folder as a zip file. This is the code we
    want to run as a lambda.

    Since the code is (currently lightly) dependent on files in meadowrun and
    meadowrun.aws_integration, those folders are also added.

    Warning, this doesn't recurse into any subdirectories (because it is not currently
    needed)
    """
    path_prefix, lambda_root_path = _get_prefix_and_root_path(
        meadowrun.aws_integration.management_lambdas
    )
    extras = [
        _get_prefix_and_root_path(meadowrun),
        _get_prefix_and_root_path(meadowrun.aws_integration),
    ]
    lambda_module_names = [
        name for _, name, _ in pkgutil.iter_modules([lambda_root_path])
    ]

    with io.BytesIO() as buffer:
        with create_zipfile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for module_name in lambda_module_names:
                if config_file is not None and module_name == "config":
                    zf.write(config_file, os.path.join(path_prefix, "config.py"))
                else:
                    zf.write(
                        os.path.join(lambda_root_path, module_name + ".py"),
                        os.path.join(path_prefix, module_name + ".py"),
                    )
            for path_prefix, root_path in extras:
                for _, module_name, is_pkg in pkgutil.iter_modules([root_path]):
                    if not is_pkg:
                        zf.write(
                            os.path.join(root_path, module_name + ".py"),
                            os.path.join(path_prefix, module_name + ".py"),
                        )

        buffer.seek(0)

        return buffer.read()


async def _create_management_lambda(
    lambda_client: LambdaClient,
    lambda_handler: ModuleType,
    lambda_name: str,
    schedule_rule_name: str,
    schedule_expression: str,
    region_name: str,
    config_file: Optional[str],
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
                Code={"ZipFile": _get_zipped_lambda_code(config_file)},
                Layers=[
                    f"arn:aws:lambda:{region_name}:{_LAMBDA_LAYER_ACCOUNT}:layer:"
                    f"{_LAMBDA_LAYER_NAME}:"
                    f"{_LAMBDA_LAYER_REGION_TO_VERSION.get(region_name, _LAMBDA_LAYER_DEFAULT_VERSION)}"  # noqa
                ],
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
    events_client = boto3.client("events", region_name=region_name)
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
    config_file: Optional[str],
    region_name: str,
) -> None:
    """
    Create the specified management lambda if it doesn't exist. If update_if_exists is
    true, updates the code if the lambda already exists.

    Even if this is called with update_if_exists, it is not guaranteed to update the
    code if another process creates the lambda after this function starts executing.

    The meadowrun management lambda role must exist already
    """

    lambda_client = boto3.client("lambda", region_name=region_name)

    exists, _ = ignore_boto3_error_code(
        lambda: lambda_client.get_function(FunctionName=lambda_name),
        "ResourceNotFoundException",
    )

    if not exists:
        await _create_management_lambda(
            lambda_client,
            lambda_handler,
            lambda_name,
            schedule_rule_name,
            schedule_expression,
            region_name,
            config_file,
        )
        _set_retention_policy_lambda_logs(lambda_name, region_name)
    elif update_if_exists:
        lambda_client.update_function_code(
            FunctionName=lambda_name, ZipFile=_get_zipped_lambda_code(config_file)
        )
        _set_retention_policy_lambda_logs(lambda_name, region_name)
        await _retry(
            lambda: ignore_boto3_error_code(
                lambda: lambda_client.update_function_configuration(
                    FunctionName=lambda_name,
                    Layers=[
                        f"arn:aws:lambda:{region_name}:{_LAMBDA_LAYER_ACCOUNT}:layer:"
                        f"{_LAMBDA_LAYER_NAME}:"
                        f"{_LAMBDA_LAYER_REGION_TO_VERSION.get(region_name, _LAMBDA_LAYER_DEFAULT_VERSION)}"  # noqa
                    ],
                ),
                "ResourceConflictException",
            ),
            10,
            2,
            f"Waiting to update lambda {lambda_name}'s configuration...",
        )


async def ensure_ec2_alloc_lambda(
    update_if_exists: bool, config_file: Optional[str], region_name: str
) -> None:
    await _ensure_management_lambda(
        meadowrun.aws_integration.management_lambdas.adjust_ec2_instances.lambda_handler,  # noqa: E501
        _EC2_ALLOC_LAMBDA_NAME,
        _EC2_ALLOC_LAMBDA_SCHEDULE_RULE,
        "rate(1 minute)",
        update_if_exists,
        config_file,
        region_name,
    )


async def ensure_clean_up_lambda(
    update_if_exists: bool, config_file: Optional[str], region_name: str
) -> None:
    await _ensure_management_lambda(
        meadowrun.aws_integration.management_lambdas.clean_up.lambda_handler,
        _CLEAN_UP_LAMBDA_NAME,
        _CLEAN_UP_LAMBDA_SCHEDULE_RULE,
        "rate(3 hours)",
        update_if_exists,
        config_file,
        region_name,
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
