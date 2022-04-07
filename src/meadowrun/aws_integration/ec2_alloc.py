import asyncio
import dataclasses
import datetime
import decimal
import io
import os.path
import pkgutil
import uuid
import zipfile
from typing import Any, List, Dict, Tuple, Callable, TypeVar

import boto3

import meadowrun.aws_integration.management_lambdas
import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances
import meadowrun.aws_integration.management_lambdas.clean_up
from meadowrun.aws_integration.aws_core import (
    _EC2_ASSUME_ROLE_POLICY_DOCUMENT,
    _LAMBDA_ASSUME_ROLE_POLICY_DOCUMENT,
    _get_default_region_name,
    _iam_role_exists,
    ensure_meadowrun_ssh_security_group,
    launch_ec2_instances,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _ALLOCATED_TIME,
    _EC2_ALLOC_TABLE_NAME,
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
    _LAST_UPDATE_TIME,
    _LOGICAL_CPU_ALLOCATED,
    _LOGICAL_CPU_AVAILABLE,
    _MEMORY_GB_ALLOCATED,
    _MEMORY_GB_AVAILABLE,
    _PUBLIC_ADDRESS,
    _RUNNING_JOBS,
    ignore_boto3_error_code,
    _MEADOWRUN_GENERATED_DOCKER_REPO,
)
from meadowrun.aws_integration.ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    _MEADOWRUN_KEY_PAIR_SECRET_NAME,
)
from meadowrun.instance_selection import (
    Resources,
    assert_is_not_none,
    remaining_resources_sort_key,
)

_T = TypeVar("_T")

# AWS resources needed for the serverless coordinator to function

# SEE ALSO ec2_alloc_stub.py

# AMIs that have meadowrun pre-installed. These are all identical, we just need to
# replicate into each region.
_EC2_ALLOC_AMIS = {
    "us-east-2": "ami-01beff5e097467c68",
    "us-east-1": "ami-0eaaee0e641721c21",
    "us-west-1": "ami-0881d1e136f652b36",
    "us-west-2": "ami-0017f03ae82ec6a14",
    "eu-central-1": "ami-043eb3326da409061",
    "eu-west-1": "ami-087fca1b01a5abdfc",
    "eu-west-2": "ami-039f3aaf15aae7b6a",
    "eu-west-3": "ami-0c63c9937b61d2c86",
    "eu-north-1": "ami-0cd8131ac20e061bb",
}

# an IAM role/an associated policy that grants permission to read/write the EC2 alloc
# dynamodb table
_EC2_ALLOC_ROLE = "meadowrun_ec2_alloc_role"
_EC2_ALLOC_ROLE_INSTANCE_PROFILE = "meadowrun_ec2_alloc_role_instance_profile"
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

# the name of the lambda that runs adjust_ec2_instances.py
_EC2_ALLOC_LAMBDA_NAME = "meadowrun_ec2_alloc_lambda"
# the EventBridge rule that triggers the lambda
_EC2_ALLOC_LAMBDA_SCHEDULE_RULE = "meadowrun_ec2_alloc_lambda_schedule_rule"

# the name of the lambda that runs clean_up.py
_CLEAN_UP_LAMBDA_NAME = "meadowrun_clean_up"
_CLEAN_UP_LAMBDA_SCHEDULE_RULE = "meadowrun_clean_up_lambda_schedule_rule"

# the role that these lambdas run as
_MANAGEMENT_LAMBDA_ROLE = "meadowrun_management_lambda_role"


def _get_account_number() -> str:
    # weird that we have to do this to get the account number to construct the ARN
    return boto3.client("sts").get_caller_identity().get("Account")


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


async def _ensure_ec2_alloc_table() -> Any:
    """
    Gets the EC2 instances table if it exists, otherwise creates it and then gets the
    newly created table.

    See _register_ec2_instance for a description of the expected schema
    """

    db = boto3.resource("dynamodb", region_name=await _get_default_region_name())
    # empirically, trying to create the table vs checking whether it exists seem to be
    # about the same performance

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.ServiceResource.create_table
    success, table = ignore_boto3_error_code(
        lambda: db.create_table(
            TableName=_EC2_ALLOC_TABLE_NAME,
            AttributeDefinitions=[
                {
                    "AttributeName": _PUBLIC_ADDRESS,
                    "AttributeType": "S",
                }
            ],
            KeySchema=[
                {
                    "AttributeName": _PUBLIC_ADDRESS,
                    "KeyType": "HASH",
                }
            ],
            BillingMode="PAY_PER_REQUEST",
            TableClass="STANDARD",
        ),
        "ResourceInUseException",
    )

    if success:
        assert table is not None  # just for mypy
        table.wait_until_exists()
    else:
        table = db.Table(_EC2_ALLOC_TABLE_NAME)

    return table


@dataclasses.dataclass
class _EC2InstanceState:
    """Represents an existing EC2 instance"""

    public_address: str
    available_resources: Resources


async def _register_ec2_instance(
    public_address: str,
    logical_cpu_available: int,
    memory_gb_available: float,
    running_jobs: List[Tuple[str, Resources]],
) -> None:
    """
    Registers a (presumably newly created) EC2 instance with the serverless coordinator.
    """

    # TODO we should probably enforce types on logical_cpu_available and
    # memory_gb_available

    now = datetime.datetime.utcnow().isoformat()
    table = await _ensure_ec2_alloc_table()

    success, result = ignore_boto3_error_code(
        lambda: table.put_item(
            Item={
                # the public address of the EC2 instance
                _PUBLIC_ADDRESS: public_address,
                # the CPU/memory available on the EC2 instance (after allocating
                # resources to the already running jobs below)
                _LOGICAL_CPU_AVAILABLE: decimal.Decimal(logical_cpu_available),
                _MEMORY_GB_AVAILABLE: decimal.Decimal(memory_gb_available),
                # the jobs currently allocated to run on this instance
                _RUNNING_JOBS: {
                    job_id: {
                        # Represents how many resources this job is "using". These
                        # values will be used to add resources back to the _AVAILABLE*
                        # fields when the jobs get deallocated
                        _LOGICAL_CPU_ALLOCATED: decimal.Decimal(
                            allocated_resources.logical_cpu
                        ),
                        _MEMORY_GB_ALLOCATED: decimal.Decimal(
                            allocated_resources.memory_gb
                        ),
                        # When the job was allocated. This gets used by
                        # deallocate_tasks.py in the case where a client allocates a job
                        # but crashes before it can launch the job. After a timeout,
                        # deallocate_tasks.py will assume this job will never start and
                        # deallocates it.
                        _ALLOCATED_TIME: now,
                    }
                    for job_id, allocated_resources in running_jobs
                },
                # The last time a job was allocated or deallocated to this machine
                _LAST_UPDATE_TIME: now,
            },
            ConditionExpression=f"attribute_not_exists({_PUBLIC_ADDRESS})",
        ),
        "ConditionalCheckFailedException",
    )

    if not success:
        # It's possible that an existing EC2 instance crashed unexpectedly, the
        # coordinator record hasn't been deleted yet, and a new instance was created
        # that has the same address
        raise ValueError(
            f"Tried to register an ec2_instance {public_address} but it already exists,"
            " this should never happen!"
        )


def _get_ec2_instances(table: Any) -> List[_EC2InstanceState]:
    """
    Gets all existing EC2 instances and how many resources are available on each one
    """

    response = table.scan(
        Select="SPECIFIC_ATTRIBUTES",
        ProjectionExpression=",".join(
            [_PUBLIC_ADDRESS, _LOGICAL_CPU_AVAILABLE, _MEMORY_GB_AVAILABLE]
        ),
    )

    if response.get("LastEvaluatedKey"):
        # TODO scan maxes out at 1MB of returned data before requiring pagination. We
        # could implement pagination, but scanning for so much data will cost a lot of
        # money, so we should eventually add either secondary indexes that let us hone
        # in on the instances we want (e.g. exclude instances that don't have enough
        # resources to run our job) or limit the scan to some number of instances and
        # not do a global optimization
        raise NotImplementedError(
            "We don't currently support a very large number of EC2 instances"
        )

    return [
        _EC2InstanceState(
            item[_PUBLIC_ADDRESS],
            Resources(
                float(item[_MEMORY_GB_AVAILABLE]), int(item[_LOGICAL_CPU_AVAILABLE]), {}
            ),
        )
        for item in response["Items"]
    ]


async def get_jobs_on_ec2_instance(public_address: str) -> Dict[str, Dict[str, Any]]:
    """
    Gets the jobs that are currently running on the specified EC2 instance. See the
    description of the _RUNNING_JOBS field in _register_ec2_instance to see what is
    returned (keys are job_ids, values are metadata about the job).
    """
    result = (await _ensure_ec2_alloc_table()).get_item(
        Key={_PUBLIC_ADDRESS: public_address}, ProjectionExpression=_RUNNING_JOBS
    )
    if "Item" not in result:
        raise ValueError(f"ec2 instance {public_address} was not found")

    return result["Item"][_RUNNING_JOBS]


def _allocate_job_to_ec2_instance(
    table: Any,
    public_address: str,
    resources_allocated_per_job: Resources,
    new_job_ids: List[str],
) -> bool:
    """
    Adds the specified job_ids to the specified ec2 instance's _RUNNING_JOBS. This will
    allocate resources_allocated_per_job for each job from the ec2 instance.

    Returns False if the allocation failed. The allocation could fail because instance
    doesn't have enough resources (which could be because another process allocated
    something to this instance between when the caller queried for the available
    resources on the instance). Another reason could be that the ec2 instance is already
    running a job with an id in new_job_ids
    """

    if len(new_job_ids) == 0:
        raise ValueError("Must provide at least one new_job_ids")

    expression_attribute_names = {}
    set_expressions = []
    now = datetime.datetime.utcnow().isoformat()
    expression_attribute_values: Dict[str, Any] = {
        ":logical_cpu_to_allocate": decimal.Decimal(
            resources_allocated_per_job.logical_cpu * len(new_job_ids)
        ),
        ":memory_gb_to_allocate": decimal.Decimal(
            resources_allocated_per_job.memory_gb * len(new_job_ids)
        ),
        ":now": now,
    }
    attribute_not_exists_expressions = []
    for i, job_id in enumerate(new_job_ids):
        # job_ids might not be valid dynamodb identifiers
        expression_attribute_names[f"#j{i}"] = job_id
        # add the jobs with their metadata to _RUNNING_JOBS
        set_expressions.append(f"{_RUNNING_JOBS}.#j{i} = :j{i}")
        expression_attribute_values[f":j{i}"] = {
            _LOGICAL_CPU_ALLOCATED: decimal.Decimal(
                resources_allocated_per_job.logical_cpu
            ),
            _MEMORY_GB_ALLOCATED: decimal.Decimal(
                resources_allocated_per_job.memory_gb
            ),
            _ALLOCATED_TIME: now,
        }
        # check that the job_id doesn't already exist
        attribute_not_exists_expressions.append(
            f"attribute_not_exists({_RUNNING_JOBS}.#j{i})"
        )

    success, result = ignore_boto3_error_code(
        lambda: table.update_item(
            Key={_PUBLIC_ADDRESS: public_address},
            # subtract resources that we're allocating
            UpdateExpression=(
                (
                    f"SET {_LOGICAL_CPU_AVAILABLE}="
                    f"{_LOGICAL_CPU_AVAILABLE} - :logical_cpu_to_allocate, "
                    f"{_MEMORY_GB_AVAILABLE}="
                    f"{_MEMORY_GB_AVAILABLE} - :memory_gb_to_allocate, "
                    f"{_LAST_UPDATE_TIME}=:now, "
                )
                + ", ".join(set_expressions)
            ),
            # Check to make sure the allocation is still valid
            ConditionExpression=(
                f"{_LOGICAL_CPU_AVAILABLE} >= :logical_cpu_to_allocate "
                f"AND {_MEMORY_GB_AVAILABLE} >= :memory_gb_to_allocate "
                "AND " + " AND ".join(attribute_not_exists_expressions)
            ),
            ExpressionAttributeValues=expression_attribute_values,
            ExpressionAttributeNames=expression_attribute_names,
        ),
        "ConditionalCheckFailedException",
    )
    return success


async def deallocate_job_from_ec2_instance(
    public_address: str, job_id: str, job: Dict[str, Any]
) -> bool:
    """
    Removes the specified job from the specified EC2 instance and restores the resources
    that were allocated to that job. The job parameter should be populated by
    get_jobs_on_ec2_instance(public_address)[job_id]

    Returns True if the job was removed, returns False if the job does not exist
    (e.g. it was already removed or never existed in the first place).
    """
    table = await _ensure_ec2_alloc_table()

    success, result = ignore_boto3_error_code(
        lambda: table.update_item(
            Key={_PUBLIC_ADDRESS: public_address},
            UpdateExpression=(
                f"SET {_LOGICAL_CPU_AVAILABLE}="
                f"{_LOGICAL_CPU_AVAILABLE} + :logical_cpu_allocated, "
                f"{_MEMORY_GB_AVAILABLE}="
                f"{_MEMORY_GB_AVAILABLE} + :memory_gb_allocated, "
                f"{_LAST_UPDATE_TIME}=:now "
                f"REMOVE {_RUNNING_JOBS}.#job_id"
            ),
            # make sure we haven't already removed this job
            ConditionExpression=f"attribute_exists({_RUNNING_JOBS}.#job_id)",
            ExpressionAttributeNames={"#job_id": job_id},
            ExpressionAttributeValues={
                ":logical_cpu_allocated": job[_LOGICAL_CPU_ALLOCATED],
                ":memory_gb_allocated": job[_MEMORY_GB_ALLOCATED],
                ":now": datetime.datetime.utcnow().isoformat(),
            },
        ),
        "ConditionalCheckFailedException",
    )
    return success


async def _choose_existing_ec2_instances(
    resources_required_per_job: Resources, num_jobs: int
) -> Dict[str, List[str]]:
    """
    Chooses existing EC2 instances to run the specified job(s). The general strategy is
    to pack instances as tightly as possible to allow larger jobs to come along later.

    Returns {public_address: [job_ids]}
    """

    table = await _ensure_ec2_alloc_table()

    # these represent jobs that have been allocated in the EC2 instances table
    num_jobs_allocated = 0
    # {public_address: [job_ids]}
    allocated_jobs: Dict[str, List[str]] = {}

    # try to allocate a maximum of 3 times. We will retry if there's an optimistic
    # concurrency issue (i.e. someone else allocates to an instance at the same time as
    # us)
    i = 0
    all_success = False
    while i < 3 and not all_success:
        ec2_instances = _get_ec2_instances(table)

        sort_keys = [
            remaining_resources_sort_key(
                ec2_instance.available_resources, resources_required_per_job
            )
            for ec2_instance in ec2_instances
        ]

        # these represent proposed allocations--they are not actually allocated until we
        # update the EC2 instances table
        num_jobs_proposed = 0
        # {public_address: [job_ids]}
        proposed_jobs: Dict[str, List[str]] = {}

        if sort_keys:
            while num_jobs_allocated + num_jobs_proposed < num_jobs:
                # choose an ec2 instance
                chosen_index = min(range(len(sort_keys)), key=lambda i: sort_keys[i])
                # if the indicator is 1, that means none of the instances can run our
                # job
                if sort_keys[chosen_index][0] == 1:
                    break

                # we successfully chose an instance!
                chosen_ec2_instance = ec2_instances[chosen_index]
                proposed_jobs.setdefault(chosen_ec2_instance.public_address, []).append(
                    str(uuid.uuid4())
                )
                num_jobs_proposed += 1

                # decrease the agent's available_resources
                chosen_ec2_instance.available_resources = assert_is_not_none(
                    (
                        chosen_ec2_instance.available_resources.subtract(
                            resources_required_per_job
                        )
                    )
                )
                # decrease the sort key for the chosen agent
                sort_keys[chosen_index] = remaining_resources_sort_key(
                    chosen_ec2_instance.available_resources, resources_required_per_job
                )

        # now that we've chosen which instance(s) will run our job(s), try to actually
        # get the allocation in the EC2 alloc table. This could fail if another process
        # is trying to do an allocation at the same time as us so the instances we've
        # chosen actually don't have enough resources (even though they did at the top
        # of this function).
        all_success = True
        for public_address, job_ids in proposed_jobs.items():
            success = _allocate_job_to_ec2_instance(
                table, public_address, resources_required_per_job, job_ids
            )
            if success:
                allocated_jobs.setdefault(public_address, []).extend(job_ids)
                num_jobs_allocated += len(job_ids)
            else:
                all_success = False

        i += 1

    if num_jobs == 1:
        if num_jobs_allocated == 0:
            print(
                "Job was not allocated to any existing EC2 instances, will launch a new"
                " EC2 instance"
            )
        else:
            print(
                "Job was allocated to an existing EC2 instance: "
                f"{' '.join(allocated_jobs.keys())}"
            )
    else:
        print(
            f"{num_jobs_allocated}/{num_jobs} workers allocated to existing EC2 "
            f"instances: {' '.join(allocated_jobs.keys())}"
        )

    return allocated_jobs


async def _launch_new_ec2_instances(
    resources_required_per_job: Resources,
    interruption_probability_threshold: float,
    num_jobs: int,
    original_num_jobs: int,
    region_name: str,
) -> Dict[str, List[str]]:
    """
    Chooses the cheapest EC2 instances to launch that can run the specified jobs,
    launches them, adds them to the EC2 alloc table, and allocates the specified jobs to
    them.

    Returns {public_address: [job_ids]}

    Warning, ensure_meadowrun_key_pair must be called before calling this function

    original_num_jobs is just to produce more coherent logging.
    """

    if region_name not in _EC2_ALLOC_AMIS:
        raise ValueError(
            f"The meadowrun AMI is not available in {region_name}. Please ask the "
            "meadowrun maintainers to add support for this region: "
            "https://github.com/meadowdata/meadowrun/issues"
        )
    ami = _EC2_ALLOC_AMIS[region_name]

    meadowrun_ssh_security_group_id = await ensure_meadowrun_ssh_security_group()
    _ensure_ec2_alloc_role(region_name)

    ec2_instances = await launch_ec2_instances(
        resources_required_per_job.logical_cpu,
        resources_required_per_job.memory_gb,
        num_jobs,
        interruption_probability_threshold,
        ami,
        region_name=region_name,
        # TODO we should let users add their own security groups
        security_group_ids=[meadowrun_ssh_security_group_id],
        # TODO we should let users set their own IAM role as long as it grants access to
        # the dynamodb table we need for deallocation
        iam_role_name=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
        # assumes that we've already called ensure_meadowrun_key_pair!
        key_name=MEADOWRUN_KEY_PAIR_NAME,
        tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
    )

    description_strings = []
    total_num_allocated_jobs = 0
    total_cost_per_hour: float = 0
    allocated_jobs = {}

    for ec2_instance in ec2_instances:
        # just to make the code more readable
        instance_info = ec2_instance.instance_type.ec2_instance_type

        # the number of jobs to allocate to this EC2 instance
        num_allocated_jobs = min(
            num_jobs - total_num_allocated_jobs,
            ec2_instance.instance_type.workers_per_instance_full,
        )
        total_num_allocated_jobs += num_allocated_jobs
        job_ids = [str(uuid.uuid4()) for _ in range(num_allocated_jobs)]

        await _register_ec2_instance(
            ec2_instance.public_dns_name,
            instance_info.logical_cpu
            - (num_allocated_jobs * resources_required_per_job.logical_cpu),
            instance_info.memory_gb
            - (num_allocated_jobs * resources_required_per_job.memory_gb),
            [(job_id, resources_required_per_job) for job_id in job_ids],
        )

        allocated_jobs[ec2_instance.public_dns_name] = job_ids
        description_strings.append(
            f"{ec2_instance.public_dns_name}: {instance_info.name} "
            f"({instance_info.logical_cpu} CPU/{instance_info.memory_gb} GB), "
            f"{instance_info.on_demand_or_spot} (${instance_info.price}/hr, "
            f"{instance_info.interruption_probability}% chance of interruption), "
            f"will run {num_allocated_jobs} job/worker"
        )
        total_cost_per_hour += instance_info.price

    if original_num_jobs == 1:
        # there should only ever be one description_strings
        print(
            f"Launched a new EC2 instance for the job: {' '.join(description_strings)}"
        )
    else:
        print(
            f"Launched {len(description_strings)} new EC2 instances (total "
            f"${total_cost_per_hour}/hr) for the remaining {num_jobs} workers:\n"
            + "\n".join(["\t" + s for s in description_strings])
        )

    return allocated_jobs


async def allocate_ec2_instances(
    resources_required_per_job: Resources,
    num_jobs: int,
    interruption_probability_threshold: float,
    region_name: str,
) -> Dict[str, List[str]]:
    """
    ec2_alloc is a serverless way to manage a cluster of ec2 instances. This function is
    the main API, which allows callers to request an allocation of the specified
    resources on an ec2 instance. This function launches the cheapest possible new
    instances that have the requested resources when necessary and keeps track of
    existing ec2 instances in a dynamodb table named _EC2_ALLOC_TABLE_NAME.

    Returns {public_address: [job_ids]}
    """

    # TODO this should take interruption_probability_threshold into account for existing
    # instances as well
    allocated = await _choose_existing_ec2_instances(
        resources_required_per_job, num_jobs
    )
    num_jobs_remaining = num_jobs - sum(len(jobs) for jobs in allocated.values())
    if num_jobs_remaining > 0:
        allocated.update(
            await _launch_new_ec2_instances(
                resources_required_per_job,
                interruption_probability_threshold,
                num_jobs_remaining,
                num_jobs,
                region_name,
            )
        )

    return allocated


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
    elif update_if_exists:
        lambda_client.update_function_code(
            FunctionName=lambda_name, ZipFile=_get_zipped_lambda_code()
        )


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


def _detach_all_policies(iam: Any, role_name: str) -> None:
    """Detach all policies from a role"""
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


def delete_meadowrun_resources(region_name: str) -> None:
    """
    Delete all AWS resources that meadowrun creates

    This needs to contain all old names/types of resources in every published version of
    this library.
    """

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

    _detach_all_policies(iam, _EC2_ALLOC_ROLE)
    ignore_boto3_error_code(
        lambda: iam.delete_role(RoleName=_EC2_ALLOC_ROLE), "NoSuchEntity"
    )

    _detach_all_policies(iam, _MANAGEMENT_LAMBDA_ROLE)
    ignore_boto3_error_code(
        lambda: iam.delete_role(RoleName=_MANAGEMENT_LAMBDA_ROLE), "NoSuchEntity"
    )

    table_access_policy_arn = _ensure_ec2_alloc_table_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=table_access_policy_arn), "NoSuchEntity"
    )

    sqs_access_policy_arn = _ensure_meadowrun_sqs_access_policy(iam)
    ignore_boto3_error_code(
        lambda: iam.delete_policy(PolicyArn=sqs_access_policy_arn), "NoSuchEntity"
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

    ec2_client = boto3.client("ec2", region_name=region_name)
    ec2_client.delete_key_pair(KeyName=MEADOWRUN_KEY_PAIR_NAME)

    secrets_client = boto3.client("secretsmanager", region_name=region_name)
    ignore_boto3_error_code(
        lambda: secrets_client.delete_secret(SecretId=_MEADOWRUN_KEY_PAIR_SECRET_NAME),
        "ResourceNotFoundException",
    )

    # TODO also delete schedule rules?

    # TODO also delete other resources like security groups, dynamodb table, SQS queues
