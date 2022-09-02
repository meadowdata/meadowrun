from __future__ import annotations

import asyncio
import datetime
import decimal
import itertools
from typing import (
    TYPE_CHECKING,
    List,
    Tuple,
    Any,
    Dict,
    Sequence,
    Optional,
    Type,
    Iterable,
)

if TYPE_CHECKING:
    from types import TracebackType
    from typing_extensions import Literal

import boto3

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2 import (
    authorize_current_ip_for_meadowrun_ssh,
    ensure_port_security_group,
    get_ssh_security_group_id,
    launch_ec2_instances,
)
from meadowrun.aws_integration.aws_permissions_install import _EC2_ROLE_INSTANCE_PROFILE
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _ALLOCATED_TIME,
    _EC2_ALLOC_TABLE_NAME,
    _EC2_ALLOC_TAG,
    _EC2_ALLOC_TAG_VALUE,
    _INSTANCE_ID,
    _LAST_UPDATE_TIME,
    _NON_CONSUMABLE_RESOURCES,
    _PREVENT_FURTHER_ALLOCATION,
    _PUBLIC_ADDRESS,
    _RESOURCES_ALLOCATED,
    _RESOURCES_AVAILABLE,
    _RUNNING_JOBS,
    ignore_boto3_error_code,
)
from meadowrun.instance_allocation import (
    InstanceRegistrar,
    _InstanceState,
    _TInstanceState,
    allocate_jobs_to_instances,
)
from meadowrun.instance_selection import ResourcesInternal, CloudInstance

if TYPE_CHECKING:
    from meadowrun.meadowrun_pb2 import Job
from meadowrun.run_job_core import JobCompletion, SshHost

# SEE ALSO ec2_alloc_stub.py

# AMIs that have meadowrun pre-installed. These are all identical, we just need to
# replicate into each region.
_AMIS = {
    "plain": {
        "us-east-2": "ami-006426834f282c3d7",
        "us-east-1": "ami-0a4540d3f3b17083b",
        "us-west-1": "ami-0a6d423b268bd06f9",
        "us-west-2": "ami-05b88ca63237af1c3",
        "eu-central-1": "ami-069c39267dc71cdae",
        "eu-west-1": "ami-006504409325858f5",
        "eu-west-2": "ami-06d58467933fa0c6a",
        "eu-west-3": "ami-0f1785b9161c75451",
        "eu-north-1": "ami-07fbc877ae78b7395",
    },
    "cuda": {
        "us-east-2": "ami-07865bb6761dc8657",
        "us-east-1": "ami-077aa9248383469ed",
        "us-west-1": "ami-0b8f848d261a6109b",
        "us-west-2": "ami-00f484124d8047b1f",
        "eu-central-1": "ami-0094b1e101a507ce2",
        "eu-west-1": "ami-0c160e3d57cac1ef6",
        "eu-west-2": "ami-007cda2f67dfdc1f7",
        "eu-west-3": "ami-080ead48cf58b236e",
        "eu-north-1": "ami-0ac5d5a37b917c70a",
    },
}
SSH_USER = "ubuntu"


def ensure_ec2_alloc_table(region_name: str) -> None:
    """Creates the EC2 alloc DynamoDB table if it doesn't already exist"""
    db = boto3.resource("dynamodb", region_name=region_name)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.ServiceResource.create_table
    ignore_boto3_error_code(
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

    db.Table(_EC2_ALLOC_TABLE_NAME).wait_until_exists()


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
        self._table = db.Table(_EC2_ALLOC_TABLE_NAME)
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
        resources_available: ResourcesInternal,
        running_jobs: List[Tuple[str, ResourcesInternal]],
    ) -> None:
        # TODO we should probably enforce types on logical_cpu_available and
        # memory_gb_available

        now = datetime.datetime.utcnow().isoformat()

        success, result = ignore_boto3_error_code(
            lambda: self._table.put_item(
                Item={
                    # the public address of the EC2 instance
                    _PUBLIC_ADDRESS: public_address,
                    _INSTANCE_ID: name,
                    # the resources (e.g. CPU/memory) available on the EC2 instance
                    # (after allocating resources to the already running jobs below)
                    _RESOURCES_AVAILABLE: resources_available.consumable_as_decimals(),
                    _NON_CONSUMABLE_RESOURCES: (
                        resources_available.non_consumable_as_decimals()
                    ),
                    # the jobs currently allocated to run on this instance
                    _RUNNING_JOBS: {
                        job_id: {
                            # Represents how many resources this job is "using". These
                            # values will be used to add resources back to the
                            # _AVAILABLE* fields when the jobs get deallocated
                            _RESOURCES_ALLOCATED: (
                                allocated_resources.consumable_as_decimals()
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
                    # Normally False. Set to True for cases like when a spot instance is
                    # being interrupted so we shouldn't assign any new jobs to this
                    # instance
                    _PREVENT_FURTHER_ALLOCATION: False,
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
                [
                    _PUBLIC_ADDRESS,
                    _INSTANCE_ID,
                    _RESOURCES_AVAILABLE,
                    _NON_CONSUMABLE_RESOURCES,
                    _PREVENT_FURTHER_ALLOCATION,
                ]
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
                item[_INSTANCE_ID],
                ResourcesInternal.from_decimals(
                    item[_RESOURCES_AVAILABLE], item[_NON_CONSUMABLE_RESOURCES]
                ),
                # This is a bit of hack, but for the EC2InstanceRegistrar, we know that
                # we won't need the running_jobs in the context that this function is
                # called, so we just set it to None to save bandwidth/memory/etc.
                None,
                item[_PREVENT_FURTHER_ALLOCATION],
            )
            for item in response["Items"]
        ]

    async def get_registered_instance(self, public_address: str) -> _InstanceState:
        result = self._table.get_item(
            Key={_PUBLIC_ADDRESS: public_address},
            ProjectionExpression=",".join(
                [_RUNNING_JOBS, _INSTANCE_ID, _PREVENT_FURTHER_ALLOCATION]
            ),
        )
        if "Item" not in result:
            raise ValueError(f"ec2 instance {public_address} was not found")

        return _InstanceState(
            public_address,
            result["Item"][_INSTANCE_ID],
            # similar to above, also a hack. We know we won't need the
            # available_resources in the context that this function is called
            None,
            result["Item"][_RUNNING_JOBS],
            result["Item"][_PREVENT_FURTHER_ALLOCATION],
        )

    async def allocate_jobs_to_instance(
        self,
        instance: _InstanceState,
        resources_allocated_per_job: ResourcesInternal,
        new_job_ids: List[str],
    ) -> bool:
        if len(new_job_ids) == 0:
            raise ValueError("Must provide at least one new_job_ids")

        now = datetime.datetime.utcnow().isoformat()

        expression_attribute_names = {}
        set_expressions = [f"{_LAST_UPDATE_TIME}=:now"]
        expression_attribute_values: Dict[str, object] = {":now": now}
        conditional_expressions = []

        for i, (key, value) in enumerate(
            resources_allocated_per_job.multiply(len(new_job_ids)).consumable.items()
        ):
            # resource names might not be valid dynamodb identifiers
            expression_attribute_names[f"#r{i}"] = key
            # subtract the resources used
            set_expressions.append(
                f"{_RESOURCES_AVAILABLE}.#r{i} = {_RESOURCES_AVAILABLE}.#r{i} - :r{i}"
            )
            expression_attribute_values[f":r{i}"] = decimal.Decimal(value)
            # check that we don't use resources we don't have
            conditional_expressions.append(f"{_RESOURCES_AVAILABLE}.#r{i} >= :r{i}")
            # non_consumable resources need to be checked by the caller

        for i, job_id in enumerate(new_job_ids):
            # job_ids might not be valid dynamodb identifiers
            expression_attribute_names[f"#j{i}"] = job_id
            # add the jobs with their metadata to _RUNNING_JOBS
            set_expressions.append(f"{_RUNNING_JOBS}.#j{i} = :j{i}")
            expression_attribute_values[f":j{i}"] = {
                _RESOURCES_ALLOCATED: (
                    resources_allocated_per_job.consumable_as_decimals()
                ),
                _ALLOCATED_TIME: now,
            }
            # check that the job_id doesn't already exist
            conditional_expressions.append(
                f"attribute_not_exists({_RUNNING_JOBS}.#j{i})"
            )

        success, result = ignore_boto3_error_code(
            lambda: self._table.update_item(
                Key={_PUBLIC_ADDRESS: instance.public_address},
                # subtract resources that we're allocating
                UpdateExpression="SET " + ", ".join(set_expressions),
                # Check to make sure the allocation is still valid
                ConditionExpression=" AND ".join(conditional_expressions),
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_names,
            ),
            "ConditionalCheckFailedException",
        )
        return success

    def allocate_jobs_to_instance_max_chunk(self) -> int:
        # the limit for update/conditional expressions in DynamoDB is 4KB:
        # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
        # We hit this at around 90 jobs, and we give ourselves a bit of extra buffer
        # here
        return 85

    async def deallocate_job_from_instance(
        self, instance: _InstanceState, job_id: str
    ) -> bool:
        job = instance.get_running_jobs()[job_id]

        expression_attribute_names = {"#job_id": job_id}
        set_expressions = [f"{_LAST_UPDATE_TIME}=:now"]
        expression_attribute_values: Dict[str, object] = {
            ":now": datetime.datetime.utcnow().isoformat()
        }

        for i, (key, value) in enumerate(job[_RESOURCES_ALLOCATED].items()):
            # resource names might not be valid dynamodb identifiers
            expression_attribute_names[f"#r{i}"] = key
            # add back the resources that were held by the job
            set_expressions.append(
                f"{_RESOURCES_AVAILABLE}.#r{i} = {_RESOURCES_AVAILABLE}.#r{i} + :r{i}"
            )
            expression_attribute_values[f":r{i}"] = decimal.Decimal(value)

        success, result = ignore_boto3_error_code(
            lambda: self._table.update_item(
                Key={_PUBLIC_ADDRESS: instance.public_address},
                UpdateExpression=(
                    f"SET {', '.join(set_expressions)} REMOVE {_RUNNING_JOBS}.#job_id"
                ),
                # make sure we haven't already removed this job
                ConditionExpression=f"attribute_exists({_RUNNING_JOBS}.#job_id)",
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            ),
            "ConditionalCheckFailedException",
        )
        return success

    async def set_prevent_further_allocation(
        self, public_address: str, value: bool
    ) -> bool:
        self._table.update_item(
            Key={_PUBLIC_ADDRESS: public_address},
            UpdateExpression=(
                f"SET {_PREVENT_FURTHER_ALLOCATION}=:value, {_LAST_UPDATE_TIME}=:now"
            ),
            ExpressionAttributeValues={
                ":value": value,
                ":now": datetime.datetime.utcnow().isoformat(),
            },
        )
        return True

    async def launch_instances(
        self,
        resources_required_per_task: ResourcesInternal,
        num_concurrent_tasks: int,
        region_name: str,
    ) -> Sequence[CloudInstance]:
        if "nvidia" in resources_required_per_task.non_consumable:
            ami_type = "cuda"
        else:
            ami_type = "plain"

        if region_name not in _AMIS[ami_type]:
            raise ValueError(
                f"The meadowrun AMI is not available in {region_name}. Please ask the "
                "meadowrun maintainers to add support for this region: "
                "https://github.com/meadowdata/meadowrun/issues and try a supported "
                f"region for now: {', '.join(_AMIS[ami_type].keys())}"
            )
        ami = _AMIS[ami_type][region_name]

        return await launch_ec2_instances(
            resources_required_per_task,
            num_concurrent_tasks,
            ami,
            region_name=region_name,
            # TODO we should let users add their own security groups
            security_group_ids=[get_ssh_security_group_id(region_name)],
            # TODO we should let users set their own IAM role as long as it grants
            # access to the dynamodb table we need for deallocation
            iam_role_name=_EC2_ROLE_INSTANCE_PROFILE,
            # assumes that we've already called ensure_meadowrun_key_pair!
            key_name=MEADOWRUN_KEY_PAIR_NAME,
            tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
        )

    async def authorize_current_ip(self) -> None:
        await authorize_current_ip_for_meadowrun_ssh(self.get_region_name())

    async def open_ports(
        self,
        ports: Optional[Sequence[str]],
        allocated_existing_instances: Iterable[_TInstanceState],
        allocated_new_instances: Iterable[CloudInstance],
    ) -> None:
        if ports:
            security_group_ids = await asyncio.gather(
                *(ensure_port_security_group(p, self.get_region_name()) for p in ports)
            )
            ec2 = boto3.client("ec2", region_name=self.get_region_name())
            for instance_id in itertools.chain(
                (i.name for i in allocated_existing_instances),
                (i.name for i in allocated_new_instances),
            ):
                groups = {
                    group["GroupId"]
                    for group in ec2.describe_instance_attribute(
                        InstanceId=instance_id, Attribute="groupSet"
                    )["Groups"]
                }
                groups.update(security_group_ids)
                ec2.modify_instance_attribute(
                    InstanceId=instance_id, Groups=list(groups)
                )


async def run_job_ec2_instance_registrar(
    job: Job,
    resources_required: ResourcesInternal,
    region_name: Optional[str],
    wait_for_result: bool,
) -> JobCompletion[Any]:
    """Runs the specified job on EC2. Creates an EC2InstanceRegistrar"""
    region_name = region_name or await _get_default_region_name()
    pkey = get_meadowrun_ssh_key(region_name)

    async with EC2InstanceRegistrar(region_name, "create") as instance_registrar:
        hosts = await allocate_jobs_to_instances(
            instance_registrar,
            resources_required,
            1,
            region_name,
            job.ports,
        )

    if len(hosts) != 1:
        raise ValueError(f"Asked for one host, but got back {len(hosts)}")
    host, job_ids = list(hosts.items())[0]
    if len(job_ids) != 1:
        raise ValueError(f"Asked for one job allocation but got {len(job_ids)}")

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_ids[0]

    return await SshHost(host, SSH_USER, pkey, ("EC2", region_name)).run_job(
        resources_required, job, wait_for_result
    )
