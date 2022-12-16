from __future__ import annotations
import base64
import json
import os
import pickle

import aiobotocore.session
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Sequence,
    TypeVar,
)
from meadowrun.aws_integration.boto_utils import (
    retry_boto3_error_code,
    ignore_boto3_error_code_async,
)
import meadowrun.aws_integration.lambda_run_job

from meadowrun.config import EPHEMERAL_STORAGE_GB, EVICTION_RATE_INVERSE, MEMORY_GB
from meadowrun.meadowrun_pb2 import ProcessState
from meadowrun.run_job_core import (
    Host,
    JobCompletion,
    Job,
    TaskResult,
    WaitOption,
)

from meadowrun.aws_integration.aws_core import (
    _get_default_region_name,
    _get_account_number,
)
from meadowrun.storage_grid_job import get_aws_s3_bucket
from meadowrun.instance_selection import ResourcesInternal
from meadowrun.aws_integration.aws_permissions_install import _MANAGEMENT_LAMBDA_ROLE
from meadowrun.aws_integration.aws_mgmt_lambda_install import (
    _get_zipped_lambda_code,
    _LAMBDA_LAYER_ACCOUNT,
    _LAMBDA_LAYER_NAME,
    _LAMBDA_LAYER_REGION_TO_VERSION,
    _LAMBDA_LAYER_DEFAULT_VERSION,
)

if TYPE_CHECKING:
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket
    from types_aiobotocore_lambda.type_defs import InvocationResponseTypeDef

    # from types_aiobotocore_lambda import LambdaClient

_T = TypeVar("_T")
_U = TypeVar("_U")
_DEFAULT_LAMBDA_MEMORY_GB = 0.125
_DEFAULT_LAMBDA_EPHEMERAL_STORAGE_GB = 0.512


@dataclass()
class AllocLambda(Host):
    region_name: Optional[str] = None

    async def set_defaults(self) -> None:
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

    async def get_storage_bucket(self) -> AbstractStorageBucket:
        return get_aws_s3_bucket(self._get_region_name())

    def _fix_resources(
        self, resources_required: Optional[ResourcesInternal]
    ) -> ResourcesInternal:
        if resources_required is None:
            resources_required = ResourcesInternal.from_cpu_and_memory(
                logical_cpu=None,
                memory_gb=_DEFAULT_LAMBDA_MEMORY_GB,
                ephemeral_storage_gb=_DEFAULT_LAMBDA_EPHEMERAL_STORAGE_GB,
            )

        resources_required.consumable.setdefault(MEMORY_GB, _DEFAULT_LAMBDA_MEMORY_GB)
        resources_required.non_consumable.setdefault(
            EPHEMERAL_STORAGE_GB, _DEFAULT_LAMBDA_EPHEMERAL_STORAGE_GB
        )
        # TODO gets added by default higher up, just get rid of it
        del resources_required.non_consumable[EVICTION_RATE_INVERSE]
        return resources_required

    async def run_job(
        self,
        resources_required: ResourcesInternal,
        job: Job,
        wait_for_result: WaitOption,
    ) -> JobCompletion[Any]:
        resources_required = self._fix_resources(resources_required)

        if (
            len(resources_required.consumable) > 1
            or len(resources_required.non_consumable) > 1
        ):
            raise ValueError(
                "Only memory_gb and ephemeral_storage can be specified for lambdas"
            )

        if job.WhichOneof("job_spec") not in ("py_function", "py_command"):
            raise ValueError("Only run_function is supported for lambda at the moment")

        await self.set_defaults()

        session = aiobotocore.session.get_session()
        region_name = self._get_region_name()
        async with session.create_client(
            "lambda", region_name=region_name
        ) as lambda_client:

            lambda_memory_mb = max(
                128, round(resources_required.consumable[MEMORY_GB] * 1024)
            )
            lambda_ephemeral_storage_mb = max(
                512,
                round(resources_required.non_consumable[EPHEMERAL_STORAGE_GB] * 1000),
            )
            lambda_name = f"meadowrun_{lambda_memory_mb}_{lambda_ephemeral_storage_mb}"
            # create the lambda
            account_number = _get_account_number()
            lambda_handler = meadowrun.aws_integration.lambda_run_job.lambda_handler
            is_success, result, error_code = await ignore_boto3_error_code_async(
                lambda_client.create_function(
                    FunctionName=lambda_name,
                    Runtime="python3.9",
                    Role=f"arn:aws:iam::{account_number}"
                    f":role/{_MANAGEMENT_LAMBDA_ROLE}",
                    Handler=f"{lambda_handler.__module__}.{lambda_handler.__name__}",
                    Code={"ZipFile": _get_zipped_lambda_code(None)},
                    Layers=[
                        f"arn:aws:lambda:{region_name}:{_LAMBDA_LAYER_ACCOUNT}:layer:"
                        f"{_LAMBDA_LAYER_NAME}:"
                        f"{_LAMBDA_LAYER_REGION_TO_VERSION.get(region_name, _LAMBDA_LAYER_DEFAULT_VERSION)}"  # noqa
                    ],
                    Timeout=15 * 60,  # 15 minutes is the maximum
                    MemorySize=lambda_memory_mb,
                    EphemeralStorage={"Size": lambda_ephemeral_storage_mb},
                    Tags={"meadowrun": "true"},
                ),
                "ResourceConflictException",
            )

            if not is_success:
                # already exists - should update, pass for now
                pass

            # invoke the lambda
            payload_in = {
                "job": base64.b64encode(job.SerializeToString()).decode("ascii")
            }

            def invoke_lambda() -> Awaitable[InvocationResponseTypeDef]:
                return lambda_client.invoke(
                    FunctionName=lambda_name,
                    InvocationType="RequestResponse",
                    LogType="Tail",
                    Payload=json.dumps(payload_in),
                )

            # retry, as the lambda may not be ready yet
            response = await retry_boto3_error_code(
                invoke_lambda,
                "ResourceConflictException",
                message="Retrying lambda invoke",
            )
            payload_bs: bytes = await response["Payload"].read()
            payload_out = json.loads(payload_bs.decode("ascii"))
            print(payload_out)
            body = payload_out["body"]

            # TODO check payload for errors and react appropriately
            # print(body)

            process_state_bytes = base64.b64decode(body["process_state"])
            process_state = ProcessState()
            process_state.ParseFromString(process_state_bytes)

            if process_state.pickled_result:
                result = pickle.loads(process_state.pickled_result)
            else:
                result = None
            log_result = base64.b64decode(response["LogResult"]).decode("utf-8")
            print(f"Begin Lambda log{os.linesep}{log_result}{os.linesep}End Lambda log")
        return JobCompletion(
            result=result,
            process_state=process_state.state,
            log_file_name=log_result,
            return_code=response["StatusCode"],
            public_address="lambda",
        )

    def run_map_as_completed(
        self,
        function: Callable[[_T], _U],
        args: Sequence[_T],
        resources_required_per_task: Optional[ResourcesInternal],
        job_fields: Dict[str, Any],
        num_concurrent_tasks: int,
        pickle_protocol: int,
        wait_for_result: WaitOption,
        max_num_task_attempts: int,
        retry_with_more_memory: bool,
    ) -> AsyncIterable[TaskResult[_U]]:
        raise NotImplementedError("run_map on AWS lambda")
