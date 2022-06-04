from __future__ import annotations

import datetime
import decimal
from types import TracebackType
from typing import List, Tuple, Any, Dict, Sequence, Optional, Literal, Type

import boto3

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2 import (
    ensure_meadowrun_ssh_security_group,
    launch_ec2_instances,
)
from meadowrun.aws_integration.ec2_alloc_role import (
    _EC2_ALLOC_ROLE_INSTANCE_PROFILE,
    _ensure_ec2_alloc_role,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    ensure_meadowrun_key_pair,
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
)
from meadowrun.instance_allocation import (
    InstanceRegistrar,
    _InstanceState,
    allocate_jobs_to_instances,
)
from meadowrun.instance_selection import Resources, CloudInstance
from meadowrun.meadowrun_pb2 import Job
from meadowrun.run_job_core import AllocCloudInstancesInternal, JobCompletion, SshHost

# SEE ALSO ec2_alloc_stub.py

# AMIs that have meadowrun pre-installed. These are all identical, we just need to
# replicate into each region.
_EC2_ALLOC_AMIS = {
    "us-east-2": "ami-004b3424bf92d9177",
    "us-east-1": "ami-012e9d193b89375d5",
    "us-west-1": "ami-0c52a37a6e38158dd",
    "us-west-2": "ami-0575fbcd477b0cf64",
    "eu-central-1": "ami-0491e6d53114982f9",
    "eu-west-1": "ami-03a40cc6e0deee8ee",
    "eu-west-2": "ami-0fa4a1d0e75ef8adb",
    "eu-west-3": "ami-0d16b7b5918ea8361",
    "eu-north-1": "ami-004dc0266e779cc12",
}


class EC2InstanceRegistrar(InstanceRegistrar[_InstanceState]):
    """
    The EC2 instance registrar uses a DynamoDB table to keep track of instances and job
    allocations.
    """

    def __init__(
        self,
        table_region_name: Optional[str],
        on_table_missing: Literal["create", "raise"],
    ):
        self._table_region_name = table_region_name
        self._on_table_missing = on_table_missing
        self._table: Any = None

    async def __aenter__(self) -> EC2InstanceRegistrar:
        """
        Gets the EC2 instances table if it exists, otherwise creates it and then gets
        the newly created table.

        See register_instance for a description of the expected schema
        """

        if self._table_region_name is None:
            self._table_region_name = await _get_default_region_name()

        db = boto3.resource("dynamodb", region_name=self._table_region_name)

        # this constructor will always succeed, regardless of whether the table already
        # exists or not
        self._table = db.Table(_EC2_ALLOC_TABLE_NAME)
        success, _ = ignore_boto3_error_code(
            lambda: self._table.load(), "ResourceNotFoundException"
        )
        if not success:
            # this means the table does not exist

            if self._on_table_missing == "raise":
                raise ValueError(
                    f"Table {_EC2_ALLOC_TABLE_NAME} does not exist in region "
                    f"{self._table_region_name}"
                )
            elif self._on_table_missing == "create":
                # This could be more robust against ResourceInUseException which would
                # indicate that someone else created the table at the same time
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.ServiceResource.create_table
                db.create_table(
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
                )
                self._table.wait_until_exists()
            else:
                raise ValueError(
                    f"Unexpected value for on_table_missing {self._on_table_missing}"
                )

        return self

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    def get_region_name(self) -> str:
        """Just for testing"""
        if self._table_region_name is None:
            raise ValueError(
                "Programming error: requested table_region_name but __aenter__ was not "
                "called to initialize"
            )
        return self._table_region_name

    async def register_instance(
        self,
        public_address: str,
        name: str,
        resources_available: Resources,
        running_jobs: List[Tuple[str, Resources]],
    ) -> None:
        # TODO we should probably enforce types on logical_cpu_available and
        # memory_gb_available

        now = datetime.datetime.utcnow().isoformat()

        success, result = ignore_boto3_error_code(
            lambda: self._table.put_item(
                Item={
                    # the public address of the EC2 instance
                    _PUBLIC_ADDRESS: public_address,
                    # the CPU/memory available on the EC2 instance (after allocating
                    # resources to the already running jobs below)
                    _LOGICAL_CPU_AVAILABLE: decimal.Decimal(
                        resources_available.logical_cpu
                    ),
                    _MEMORY_GB_AVAILABLE: decimal.Decimal(
                        resources_available.memory_gb
                    ),
                    # the jobs currently allocated to run on this instance
                    _RUNNING_JOBS: {
                        job_id: {
                            # Represents how many resources this job is "using". These
                            # values will be used to add resources back to the
                            # _AVAILABLE* fields when the jobs get deallocated
                            _LOGICAL_CPU_ALLOCATED: decimal.Decimal(
                                allocated_resources.logical_cpu
                            ),
                            _MEMORY_GB_ALLOCATED: decimal.Decimal(
                                allocated_resources.memory_gb
                            ),
                            # When the job was allocated. This gets used by
                            # deallocate_tasks.py in the case where a client allocates a
                            # job but crashes before it can launch the job. After a
                            # timeout, deallocate_tasks.py will assume this job will
                            # never start and deallocates it.
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
                f"Tried to register an ec2_instance {public_address} but it already "
                f"exists, this should never happen!"
            )

    async def get_registered_instances(self) -> List[_InstanceState]:
        response = self._table.scan(
            Select="SPECIFIC_ATTRIBUTES",
            ProjectionExpression=",".join(
                [_PUBLIC_ADDRESS, _LOGICAL_CPU_AVAILABLE, _MEMORY_GB_AVAILABLE]
            ),
        )

        if response.get("LastEvaluatedKey"):
            # TODO scan maxes out at 1MB of returned data before requiring pagination.
            # We could implement pagination, but scanning for so much data will cost a
            # lot of money, so we should eventually add either secondary indexes that
            # let us hone in on the instances we want (e.g. exclude instances that don't
            # have enough resources to run our job) or limit the scan to some number of
            # instances and not do a global optimization
            raise NotImplementedError(
                "We don't currently support a very large number of EC2 instances"
            )

        return [
            _InstanceState(
                item[_PUBLIC_ADDRESS],
                Resources(
                    float(item[_MEMORY_GB_AVAILABLE]),
                    int(item[_LOGICAL_CPU_AVAILABLE]),
                    {},
                ),
                # This is a bit of hack, but for the EC2InstanceRegistrar, we know that
                # we won't need the running_jobs in the context that this function is
                # called, so we just set it to None to save bandwidth/memory/etc.
                None,
            )
            for item in response["Items"]
        ]

    async def get_registered_instance(self, public_address: str) -> _InstanceState:
        result = self._table.get_item(
            Key={_PUBLIC_ADDRESS: public_address}, ProjectionExpression=_RUNNING_JOBS
        )
        if "Item" not in result:
            raise ValueError(f"ec2 instance {public_address} was not found")

        return _InstanceState(
            public_address,
            # similar to above, also a hack. We know we won't need the
            # available_resources in the context that this function is called
            None,
            result["Item"][_RUNNING_JOBS],
        )

    async def allocate_jobs_to_instance(
        self,
        instance: _InstanceState,
        resources_allocated_per_job: Resources,
        new_job_ids: List[str],
    ) -> bool:
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
            lambda: self._table.update_item(
                Key={_PUBLIC_ADDRESS: instance.public_address},
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

    async def deallocate_job_from_instance(
        self, instance: _InstanceState, job_id: str
    ) -> bool:
        job = instance.get_running_jobs()[job_id]
        success, result = ignore_boto3_error_code(
            lambda: self._table.update_item(
                Key={_PUBLIC_ADDRESS: instance.public_address},
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

    async def launch_instances(
        self, instances_spec: AllocCloudInstancesInternal
    ) -> Sequence[CloudInstance]:
        if instances_spec.region_name not in _EC2_ALLOC_AMIS:
            raise ValueError(
                f"The meadowrun AMI is not available in {instances_spec.region_name}. "
                "Please ask the meadowrun maintainers to add support for this region: "
                "https://github.com/meadowdata/meadowrun/issues"
            )
        ami = _EC2_ALLOC_AMIS[instances_spec.region_name]

        meadowrun_ssh_security_group_id = await ensure_meadowrun_ssh_security_group()
        _ensure_ec2_alloc_role(instances_spec.region_name)

        return await launch_ec2_instances(
            instances_spec.logical_cpu_required_per_task,
            instances_spec.memory_gb_required_per_task,
            instances_spec.num_concurrent_tasks,
            instances_spec.interruption_probability_threshold,
            ami,
            region_name=instances_spec.region_name,
            # TODO we should let users add their own security groups
            security_group_ids=[meadowrun_ssh_security_group_id],
            # TODO we should let users set their own IAM role as long as it grants
            # access to the dynamodb table we need for deallocation
            iam_role_name=_EC2_ALLOC_ROLE_INSTANCE_PROFILE,
            # assumes that we've already called ensure_meadowrun_key_pair!
            key_name=MEADOWRUN_KEY_PAIR_NAME,
            tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
        )


async def run_job_ec2_instance_registrar(
    job: Job,
    logical_cpu_required: int,
    memory_gb_required: float,
    interruption_probability_threshold: float,
    region_name: Optional[str],
) -> JobCompletion[Any]:
    """Runs the specified job on EC2. Creates an EC2InstanceRegistrar"""
    region_name = region_name or await _get_default_region_name()
    pkey = ensure_meadowrun_key_pair(region_name)

    async with EC2InstanceRegistrar(region_name, "create") as instance_registrar:
        hosts = await allocate_jobs_to_instances(
            instance_registrar,
            AllocCloudInstancesInternal(
                logical_cpu_required,
                memory_gb_required,
                interruption_probability_threshold,
                1,
                region_name,
            ),
        )

    fabric_kwargs: Dict[str, Any] = {
        "user": "ubuntu",
        "connect_kwargs": {"pkey": pkey},
    }

    if len(hosts) != 1:
        raise ValueError(f"Asked for one host, but got back {len(hosts)}")
    host, job_ids = list(hosts.items())[0]
    if len(job_ids) != 1:
        raise ValueError(f"Asked for one job allocation but got {len(job_ids)}")

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_ids[0]

    return await SshHost(host, fabric_kwargs, ("EC2", region_name)).run_job(job)
