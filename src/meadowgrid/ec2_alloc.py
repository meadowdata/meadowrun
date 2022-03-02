import dataclasses
import datetime
import decimal
import uuid
from typing import Any, List, Dict, Tuple

import boto3

from meadowgrid.aws_integration import (
    _ignore_boto3_error_code,
    launch_ec2_instances,
    _EC2ALLOC_AWS_AMI,
    ensure_meadowgrid_ssh_security_group,
    _iam_role_exists,
    _EC2_ASSUME_ROLE_POLICY_DOCUMENT,
    _get_default_region_name,
)
from meadowgrid.resource_allocation import (
    Resources,
    _remaining_resources_sort_key,
    _assert_is_not_none,
)

# AWS resources needed for the serverless coordinator to function

# a dynamodb table that holds information about EC2 instances we've created and what has
# been allocated to which instances
_EC2_ALLOC_TABLE_NAME = "_meadowgrid_ec2_alloc_table"

# an IAM role/an associated policy that grants permission to read/write the EC2 alloc
# dynamodb table
_EC2_ALLOC_ROLE = "meadowgrid_ec2_alloc_role"
_EC2_ALLOC_ROLE_INSTANCE_PROFILE = "meadowgrid_ec2_alloc_role_instance_profile"
_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME = "meadowgrid_ec2_alloc_table_access"
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

# names of attributes/keys in the EC2 alloc table
_PUBLIC_ADDRESS = "public_address"
_LOGICAL_CPU_AVAILABLE = "logical_cpu_available"
_LOGICAL_CPU_ALLOCATED = "logical_cpu_allocated"
_MEMORY_GB_AVAILABLE = "memory_gb_available"
_MEMORY_GB_ALLOCATED = "memory_gb_allocated"
_ALLOCATED_TIME = "allocated_time"
_RUNNING_JOBS = "running_jobs"
_JOB_ID = "job_id"


def _ensure_ec2_alloc_role(region_name: str) -> None:
    """
    Creates the meadowgrid EC2 alloc IAM role if it doesn't exist, and gives it
    permissions to read/write to the EC2 alloc table. The agents need this so that they
    can deallocate jobs when they finish.

    TODO does not try to update the role if/when we change the policies below in code
    """

    iam = boto3.client("iam", region_name=region_name)
    if not _iam_role_exists(iam, _EC2_ALLOC_ROLE):
        # create the role
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.ServiceResource.create_role
        _ignore_boto3_error_code(
            lambda: iam.create_role(
                RoleName=_EC2_ALLOC_ROLE,
                # allow EC2 instances to assume this role
                AssumeRolePolicyDocument=_EC2_ASSUME_ROLE_POLICY_DOCUMENT,
                Description="Allows reading/writing the EC2 alloc table",
            ),
            "EntityAlreadyExists",
        )

        # create the table access policy and attach it to the role
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.create_policy
        _ignore_boto3_error_code(
            lambda: iam.create_policy(
                PolicyName=_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME,
                PolicyDocument=_EC2_TABLE_ACCESS_POLICY_DOCUMENT,
            ),
            "EntityAlreadyExists",
        )
        # weird that we have to do this to get the account number to construct the ARN
        account_number = boto3.client("sts").get_caller_identity().get("Account")
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.attach_role_policy
        iam.attach_role_policy(
            RoleName=_EC2_ALLOC_ROLE,
            # TODO should create a policy that only allows what we actually need
            PolicyArn=f"arn:aws:iam::{account_number}:policy"
            f"/{_EC2_ALLOC_TABLE_ACCESS_POLICY_NAME}",
        )

        # create an instance profile (so that EC2 instances can assume it) and attach
        # the role to the instance profile
        _ignore_boto3_error_code(
            lambda: iam.create_instance_profile(
                InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE
            ),
            "EntityAlreadyExists",
        )
        _ignore_boto3_error_code(
            lambda: iam.add_role_to_instance_profile(
                InstanceProfileName=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
                RoleName=_EC2_ALLOC_ROLE,
            ),
            "LimitExceeded",
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
    success, table = _ignore_boto3_error_code(
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

    success, result = _ignore_boto3_error_code(
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


async def _deregister_ec2_instance(public_address: str) -> None:
    """
    Deregisters an EC2 instance.

    TODO currently unused, need to create a lambda that queries for EC2 instances that
    have died and deregisters them
    """
    (await _ensure_ec2_alloc_table()).delete_item(Key={_PUBLIC_ADDRESS: public_address})


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
    expression_attribute_values: Dict[str, Any] = {
        ":logical_cpu_to_allocate": decimal.Decimal(
            resources_allocated_per_job.logical_cpu * len(new_job_ids)
        ),
        ":memory_gb_to_allocate": decimal.Decimal(
            resources_allocated_per_job.memory_gb * len(new_job_ids)
        ),
    }
    attribute_not_exists_expressions = []
    now = datetime.datetime.utcnow().isoformat()
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

    success, result = _ignore_boto3_error_code(
        lambda: table.update_item(
            Key={_PUBLIC_ADDRESS: public_address},
            # subtract resources that we're allocating
            UpdateExpression=(
                (
                    f"SET {_LOGICAL_CPU_AVAILABLE}="
                    f"{_LOGICAL_CPU_AVAILABLE} - :logical_cpu_to_allocate, "
                    f"{_MEMORY_GB_AVAILABLE}="
                    f"{_MEMORY_GB_AVAILABLE} - :memory_gb_to_allocate, "
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

    success, result = _ignore_boto3_error_code(
        lambda: table.update_item(
            Key={_PUBLIC_ADDRESS: public_address},
            UpdateExpression=(
                f"SET {_LOGICAL_CPU_AVAILABLE}="
                f"{_LOGICAL_CPU_AVAILABLE} + :logical_cpu_allocated, "
                f"{_MEMORY_GB_AVAILABLE}="
                f"{_MEMORY_GB_AVAILABLE} + :memory_gb_allocated "
                f"REMOVE {_RUNNING_JOBS}.#job_id"
            ),
            # make sure we haven't already removed this job
            ConditionExpression=f"attribute_exists({_RUNNING_JOBS}.#job_id)",
            ExpressionAttributeNames={"#job_id": job_id},
            ExpressionAttributeValues={
                ":logical_cpu_allocated": job[_LOGICAL_CPU_ALLOCATED],
                ":memory_gb_allocated": job[_MEMORY_GB_ALLOCATED],
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
            _remaining_resources_sort_key(
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
                chosen_ec2_instance.available_resources = _assert_is_not_none(
                    (
                        chosen_ec2_instance.available_resources.subtract(
                            resources_required_per_job
                        )
                    )
                )
                # decrease the sort key for the chosen agent
                sort_keys[chosen_index] = _remaining_resources_sort_key(
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

    print(
        f"For {num_jobs}, allocated {num_jobs_allocated} tasks to existing EC2 "
        f"instances: {' '.join(allocated_jobs.keys())}"
    )

    return allocated_jobs


async def _launch_new_ec2_instances(
    resources_required_per_job: Resources,
    interruption_probability_threshold: float,
    num_jobs: int,
    region_name: str,
) -> Dict[str, List[str]]:
    """
    Chooses the cheapest EC2 instances to launch that can run the specified jobs,
    launches them, adds them to the EC2 alloc table, and allocates the specified jobs to
    them.

    Returns {public_address: [job_ids]}
    """

    meadowgrid_ssh_security_group_id = await ensure_meadowgrid_ssh_security_group()
    _ensure_ec2_alloc_role(await _get_default_region_name())

    ec2_instances = await launch_ec2_instances(
        resources_required_per_job.logical_cpu,
        resources_required_per_job.memory_gb,
        num_jobs,
        interruption_probability_threshold,
        _EC2ALLOC_AWS_AMI,
        region_name=region_name,
        # TODO we should let users add their own security groups
        security_group_ids=[meadowgrid_ssh_security_group_id],
        # TODO we should let users set their own IAM role as long as it grants access to
        # the dynamodb table we need for deallocation
        iam_role_name=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
    )

    description_strings = []
    total_num_allocated_jobs = 0
    allocated_jobs = {}

    for ec2_instance in ec2_instances:
        # the number of jobs to allocate to this EC2 instance
        num_allocated_jobs = min(
            num_jobs - total_num_allocated_jobs, ec2_instance.max_jobs
        )
        total_num_allocated_jobs += num_allocated_jobs
        job_ids = [str(uuid.uuid4()) for _ in range(num_allocated_jobs)]

        await _register_ec2_instance(
            ec2_instance.public_dns_name,
            ec2_instance.logical_cpus
            - (num_allocated_jobs * resources_required_per_job.logical_cpu),
            ec2_instance.memory_gb
            - (num_allocated_jobs * resources_required_per_job.memory_gb),
            [(job_id, resources_required_per_job) for job_id in job_ids],
        )

        allocated_jobs[ec2_instance.public_dns_name] = job_ids
        description_strings.append(
            f"\t{ec2_instance.public_dns_name}: {ec2_instance.instance_type} "
            f"({ec2_instance.logical_cpus} CPU/{ec2_instance.memory_gb} GB), "
            f"{ec2_instance.on_demand_or_spot} "
            f"({ec2_instance.interruption_probability}% chance of interruption), "
            f"will run {num_allocated_jobs} tasks"
        )

    print(
        f"For {num_jobs}, created new EC2 instances:\n" + "\n".join(description_strings)
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
                region_name,
            )
        )

    return allocated
