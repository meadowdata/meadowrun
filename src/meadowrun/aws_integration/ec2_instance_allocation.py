from __future__ import annotations

import asyncio
import dataclasses
import datetime
import decimal
import functools
import itertools
import uuid
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import boto3
import aiobotocore.session

from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.aws_permissions_install import _EC2_ROLE_INSTANCE_PROFILE
from meadowrun.aws_integration.ec2 import (
    LaunchEC2InstanceSettings,
    authorize_current_ip_for_meadowrun_ssh,
    ensure_port_security_group,
    get_ssh_security_group_id,
    launch_ec2_instances,
)
from meadowrun.aws_integration.ec2_ssh_keys import (
    MEADOWRUN_KEY_PAIR_NAME,
    get_meadowrun_ssh_key,
)
from meadowrun.aws_integration.grid_tasks_sqs import (
    _add_worker_shutdown_messages,
    _add_tasks,
    _create_request_queue,
    receive_results,
    retry_task,
    worker_function,
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
    allocate_single_job_to_instance,
)
from meadowrun.instance_selection import ResourcesInternal, CloudInstance
from meadowrun.aws_integration.s3 import S3ObjectStorage
from meadowrun.run_job_core import (
    AllocVM,
    CloudProviderType,
    GridJobCloudInterface,
    JobCompletion,
    ObjectStorage,
    SshHost,
    WaitOption,
)

if TYPE_CHECKING:
    from types import TracebackType
    from typing_extensions import Literal
    from meadowrun.meadowrun_pb2 import Job, ProcessState

# SEE ALSO ec2_alloc_stub.py

_T = TypeVar("_T")
_U = TypeVar("_U")

# AMIs that have meadowrun pre-installed. These are all identical, we just need to
# replicate into each region.
_AMIS = {
    "plain": {
        "us-east-2": "ami-093bc850d42012c90",
        "us-east-1": "ami-0a519867d2df1d0db",
        "us-west-1": "ami-036f320911e0afa56",
        "us-west-2": "ami-09a6c93dc39a05f37",
        "eu-central-1": "ami-003339e724b9bfdfd",
        "eu-west-1": "ami-085fa49ba176751b4",
        "eu-west-2": "ami-08345d3288b4f44c7",
        "eu-west-3": "ami-07824146798815dfd",
        "eu-north-1": "ami-09769400e1855038c",
    },
    "cuda": {
        "us-east-2": "ami-05fc9761c08a10e8d",
        "us-east-1": "ami-07fb980d159f20b8a",
        "us-west-1": "ami-0a45b65eefcc590e3",
        "us-west-2": "ami-0762674ff454dc3a7",
        "eu-central-1": "ami-03d7b166252674d75",
        "eu-west-1": "ami-0a39a88acace4f75f",
        "eu-west-2": "ami-042bfbb3a9039d3ac",
        "eu-west-3": "ami-02500f90feb40d2e2",
        "eu-north-1": "ami-0b0f26cf57f54e286",
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
        instance_type_resources_required_per_task: ResourcesInternal,
        num_concurrent_tasks: int,
        alloc_cloud_instances: AllocVM,
        abort: Optional[asyncio.Event],
    ) -> Sequence[CloudInstance]:
        if not isinstance(alloc_cloud_instances, AllocEC2Instance):
            # TODO do this in the type checker somehow
            raise ValueError(
                "Programming error: EC2InstanceRegistrar can only be used with "
                "AllocEC2Instance"
            )

        region_name = alloc_cloud_instances._get_region_name()

        if alloc_cloud_instances.ami_id:
            ami = alloc_cloud_instances.ami_id
        else:
            if "nvidia" in instance_type_resources_required_per_task.non_consumable:
                ami_type = "cuda"
            else:
                ami_type = "plain"

            if region_name not in _AMIS[ami_type]:
                raise ValueError(
                    f"The meadowrun AMI is not available in {region_name}. Please ask "
                    f"the meadowrun maintainers to add support for this region: "
                    "https://github.com/meadowdata/meadowrun/issues and try a supported"
                    f" region for now: {', '.join(_AMIS[ami_type].keys())}"
                )
            ami = _AMIS[ami_type][region_name]

        if alloc_cloud_instances.subnet_id:
            subnet_id = alloc_cloud_instances.subnet_id
        else:
            subnet_id = None

        if alloc_cloud_instances.security_group_ids:
            if isinstance(alloc_cloud_instances.security_group_ids, str):
                security_group_ids: Sequence[str] = [
                    alloc_cloud_instances.security_group_ids
                ]
            else:
                security_group_ids = alloc_cloud_instances.security_group_ids
        else:
            security_group_ids = [get_ssh_security_group_id(region_name)]

        if alloc_cloud_instances.iam_role_instance_profile:
            iam_role_instance_profile = alloc_cloud_instances.iam_role_instance_profile
        else:
            iam_role_instance_profile = _EC2_ROLE_INSTANCE_PROFILE

        return await launch_ec2_instances(
            instance_type_resources_required_per_task,
            num_concurrent_tasks,
            LaunchEC2InstanceSettings(
                ami_id=ami,
                subnet_id=subnet_id,
                security_group_ids=security_group_ids,
                iam_role_instance_profile=iam_role_instance_profile,
            ),
            region_name=region_name,
            # assumes that we've already called ensure_meadowrun_key_pair!
            key_name=MEADOWRUN_KEY_PAIR_NAME,
            tags={_EC2_ALLOC_TAG: _EC2_ALLOC_TAG_VALUE},
            abort=abort,
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


@dataclasses.dataclass()
class AllocEC2Instance(AllocVM):
    """
    Specifies that the job should be run on a dynamically allocated EC2 instance. Any
    existing Meadowrun-managed EC2 instances will be reused if available. If none are
    available, Meadowrun will launch the cheapest instance type that meets the resource
    requirements for a job.

    `resources_required` must be provided with the AllocEC2Instance Host.

    Attributes:
        region_name: Specifies the region name for EC2, e.g. "us-east-2". None will use
            the default region_name.
        ami_id: An AMI ID for EC2, e.g. "ami-006426834f282c3d7". This image must be
            available in the specified region. The AMI specified must be built off of
            the Meadowrun AMIs as Meadowrun expects certain python environments and
            folders to be available. See [Use a custom AMI (machine image) on
            AWS](../../how_to/custom_ami)

            If this is not specified, Meadowrun will use the default Meadowrun-supplied
            images when launching new images. If there are existing instances, the job
            will run on any instance, regardless of what image was used to launch it.
        subnet_id: The subnet id for EC2, e.g. "subnet-02e4e2996d2fb96d9". The subnet
            must be in the specified region. If this is not specified, the default
            subnet in the default VPC will be used.

            If you specify a subnet, you'll need to think about how to connect to the
            machine in your subnet. One option is to [set your subnet to auto-assign IP
            addresses](https://docs.aws.amazon.com/vpc/latest/userguide/configure-subnets.html#subnet-settings).
            If an instance does not have a public IP address, Meadowrun will try to
            connect via the private IP address. This will only work if the machine
            launching the job can access the private IP address. The default subnet in a
            new AWS account will be set to auto-assign IP addresses.

            If your subnet does not have access to the internet, you'll need to make
            sure that any dependencies (e.g. pip packages, container images) that
            Meadowrun will try to download are available without internet access. The
            default subnet in a new AWS account will have the appropriate Route Tables
            and Internet Gateway configuration to be able to access the internet.
        security_group_ids: A list of security group ids, e.g. "sg-0690853a8374b9b6d".
            The security group must be in the same VPC as the specified subnet_id
            (security groups are specific to each VPC), and it must allow you to SSH
            over port 22 to the machine from your current IP address. If this is not
            specified, Meadowrun will use a security group created at install time in
            the default VPC. Meadowrun's install step sets up a security group and opens
            port 22 for that security group for the current IP whenever Meadowrun is
            used. If you specify a subnet that is not in the default VPC, this parameter
            is required, as the default security group will not be available. Please
            also see the "ports" argument on the run_* commands.
        iam_role_instance_profile: The name of an instance profile for an IAM role name
            (not to be confused with the IAM role itself!), e.g.
            "meadowrun_ec2_role_instance_profile". The EC2 instance will be launched
            under this IAM role. By default, Meadowrun will use an IAM role created at
            install time called meadowrun_ec2_role that has the permissions needed for a
            Meadowrun-managed EC2 instance. Any IAM role you specify must have a
            superset of the permissions granted by meadowrun_ec2_role. The easiest way
            to implement this is to attach the Meadowrun-generated
            "meadowrun_ec2_policy" to your IAM role (in addition to any custom policies
            you wish to add). Please also see [Access resources from Meadowrun
            jobs](../../how_to/access_resources)
    """

    region_name: Optional[str] = None
    ami_id: Union[str, None] = None
    subnet_id: Optional[str] = None
    security_group_ids: Union[str, Sequence[str], None] = None
    iam_role_instance_profile: Optional[str] = None

    def get_cloud_provider(self) -> CloudProviderType:
        return "EC2"

    async def set_defaults(self) -> None:
        # This does not need to set defaults for ami_id, subnet_id, etc., as those can
        # all be set in run_job. The only thing we need to set at this point is the
        # region_name, because an S3 code upload requires the region to be decided.
        if not self.region_name:
            self.region_name = await _get_default_region_name()

    def _get_region_name(self) -> str:
        # Wrapper around region_name that throws if it is None. Should only be used
        # internally.
        if self.region_name is None:
            raise ValueError(
                "Programming error: region_name is None but it should have been set by "
                "set_defaults earlier"
            )
        return self.region_name

    def get_runtime_resources(self) -> ResourcesInternal:
        non_consumable = {}
        if self.ami_id is not None:
            non_consumable[self.ami_id] = 1.0
        if self.subnet_id is not None:
            non_consumable[self.subnet_id] = 1.0
        if self.security_group_ids:
            if isinstance(self.security_group_ids, str):
                non_consumable[self.security_group_ids] = 1.0
            else:
                for security_group_id in self.security_group_ids:
                    non_consumable[security_group_id] = 1.0
        if self.iam_role_instance_profile:
            non_consumable[self.iam_role_instance_profile] = 1.0

        return ResourcesInternal({}, non_consumable)

    async def run_job(
        self,
        resources_required: Optional[ResourcesInternal],
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        if resources_required is None:
            raise ValueError(
                "Resources.logical_cpu and memory_gb must be specified for "
                "AllocEC2Instance"
            )

        return await run_job_ec2_instance_registrar(
            job, resources_required, self, wait_for_result
        )

    def _create_grid_job_cloud_interface(self) -> GridJobCloudInterface:
        return EC2GridJobInterface(self)

    async def get_object_storage(self) -> ObjectStorage:
        return S3ObjectStorage(self._get_region_name())


async def run_job_ec2_instance_registrar(
    job: Job,
    resources_required: ResourcesInternal,
    alloc_ec2_instance: AllocEC2Instance,
    wait_for_result: WaitOption,
) -> JobCompletion[Any]:
    """Runs the specified job on EC2. Creates an EC2InstanceRegistrar"""
    region_name = alloc_ec2_instance._get_region_name()
    pkey = get_meadowrun_ssh_key(region_name)

    async with EC2InstanceRegistrar(region_name, "create") as instance_registrar:
        host, job_id = await allocate_single_job_to_instance(
            instance_registrar,
            resources_required,
            alloc_ec2_instance,
            job.ports,
        )

    # Kind of weird that we're changing the job_id here, but okay as long as job_id
    # remains mostly an internal concept
    job.job_id = job_id

    return await SshHost(host, SSH_USER, pkey, ("EC2", region_name)).run_job(
        resources_required, job, wait_for_result
    )


class EC2GridJobInterface(GridJobCloudInterface):
    """
    This is a relatively thin wrapper around some functionality in grid_tasks_sqs. This
    class should be in grid_tasks_sqs, but it's here because of circular import issues.
    """

    def __init__(self, alloc_cloud_instance: AllocEC2Instance):
        self._cloud_provider = alloc_cloud_instance.get_cloud_provider()
        self._region_name = alloc_cloud_instance._get_region_name()

        self._ssh_private_key = get_meadowrun_ssh_key(self._region_name)

        self._request_queue_url: Optional[asyncio.Task[str]] = None
        self._task_argument_ranges: Optional[asyncio.Task[List[Tuple[int, int]]]] = None

        # this id is just used for creating the job's queues. It has no relationship to
        # any Job.job_ids
        self._job_id = str(uuid.uuid4())

    def create_instance_registrar(self) -> InstanceRegistrar:
        return EC2InstanceRegistrar(self._region_name, "create")

    async def setup_and_add_tasks(self, tasks: Sequence[_T]) -> None:
        # create SQS queues and add tasks to the request queue
        print(f"The current run_map's id is {self._job_id}")
        session = aiobotocore.session.get_session()
        async with session.create_client(
            "sqs", region_name=self._region_name
        ) as sqs, session.create_client("s3", region_name=self._region_name) as s3c:
            self._request_queue_url = asyncio.create_task(
                _create_request_queue(self._job_id, sqs)
            )
            self._task_argument_ranges = asyncio.create_task(
                _add_tasks(self._job_id, await self._request_queue_url, s3c, sqs, tasks)
            )
            await self._task_argument_ranges

    async def ssh_host_from_address(self, address: str) -> SshHost:
        return SshHost(
            address,
            SSH_USER,
            self._ssh_private_key,
            (self._cloud_provider, self._region_name),
        )

    async def shutdown_workers(self, num_workers: int) -> None:
        if self._request_queue_url is None:
            raise ValueError(
                "setup_and_add_tasks must be called before send_worker_done_messages"
            )
        await _add_worker_shutdown_messages(
            await self._request_queue_url, num_workers, self._region_name
        )

    async def get_worker_function(
        self, user_function: Callable[[_T], _U]
    ) -> Callable[[str, str], None]:
        if self._request_queue_url is None:
            raise ValueError(
                "setup_and_add_tasks must be called before get_worker_function"
            )

        return functools.partial(
            worker_function,
            user_function,
            await self._request_queue_url,
            self._job_id,
            self._region_name,
        )

    async def receive_task_results(
        self, *, stop_receiving: asyncio.Event, workers_done: asyncio.Event
    ) -> AsyncIterable[Tuple[int, int, ProcessState]]:
        return receive_results(
            self._job_id,
            self._region_name,
            stop_receiving=stop_receiving,
            all_workers_exited=workers_done,
        )

    async def retry_task(self, task_id: int, attempts_so_far: int) -> None:
        if self._request_queue_url is None or self._task_argument_ranges is None:
            raise ValueError("setup_and_add_tasks must be called before retry_task")

        await retry_task(
            await self._request_queue_url,
            task_id,
            attempts_so_far + 1,
            (await self._task_argument_ranges)[task_id],
            self._region_name,
        )
