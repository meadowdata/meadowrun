from __future__ import annotations

import datetime
import pprint
from typing import TYPE_CHECKING

import boto3
import meadowrun.aws_integration.aws_install_uninstall
import meadowrun.aws_integration.management_lambdas.adjust_ec2_instances as adjust_ec2_instances  # noqa: E501
import pytest
from instance_registrar_suite import (
    TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
    InstanceRegistrarProvider,
    InstanceRegistrarSuite,
)
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ec2_instance_allocation import (
    AllocEC2Instance,
    EC2InstanceRegistrar,
)
from meadowrun.aws_integration.ec2_pricing import _get_ec2_instance_types
from meadowrun.config import EVICTION_RATE_INVERSE, LOGICAL_CPU, MEMORY_GB
from meadowrun.instance_allocation import InstanceRegistrar
from meadowrun.instance_selection import (
    ResourcesInternal,
    choose_instance_types_for_job,
)

if TYPE_CHECKING:
    from meadowrun.run_job_core import Host


class EC2InstanceRegistrarProvider(InstanceRegistrarProvider[InstanceRegistrar]):
    async def get_instance_registrar(self) -> InstanceRegistrar:
        return EC2InstanceRegistrar(await _get_default_region_name(), "create")

    async def deregister_instance(
        self,
        instance_registrar: InstanceRegistrar,
        name: str,
        require_no_running_jobs: bool,
    ) -> bool:
        return adjust_ec2_instances._deregister_ec2_instance(
            name,
            require_no_running_jobs,
            instance_registrar.get_region_name(),
        )

    async def num_currently_running_instances(
        self, instance_registrar: InstanceRegistrar
    ) -> int:
        ec2 = boto3.resource("ec2", region_name=instance_registrar.get_region_name())
        return sum(1 for _ in adjust_ec2_instances._get_running_instances(ec2))

    async def run_adjust(self, instance_registrar: InstanceRegistrar) -> None:
        adjust_ec2_instances._deregister_and_terminate_instances(
            instance_registrar.get_region_name(),
            TERMINATE_INSTANCES_IF_IDLE_FOR_TEST,
            datetime.timedelta.min,
        )

    async def terminate_all_instances(
        self, instance_registrar: InstanceRegistrar
    ) -> None:
        meadowrun.aws_integration.aws_install_uninstall.terminate_all_instances(
            instance_registrar.get_region_name(), False
        )

    def get_host(self) -> Host:
        return AllocEC2Instance()


class TestEC2InstanceRegistrar(EC2InstanceRegistrarProvider, InstanceRegistrarSuite):
    pass


REGION = "us-east-2"


@pytest.mark.asyncio
async def test_get_ec2_instance_types() -> None:
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types(REGION)
    # the actual number of instance types will fluctuate based on AWS' whims.
    assert len(instance_types) > 600

    chosen_instance_types = list(
        choose_instance_types_for_job(
            ResourcesInternal.from_cpu_and_memory(3, 5, 10), 52, instance_types
        )
    )
    total_cpu = sum(
        instance_type.instance_type.resources.consumable[LOGICAL_CPU]
        * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_cpu >= 3 * 52
    total_memory_gb = sum(
        instance_type.instance_type.resources.consumable[MEMORY_GB]
        * instance_type.num_instances
        for instance_type in chosen_instance_types
    )
    assert total_memory_gb >= 5 * 52
    assert all(
        (
            100
            - instance_type.instance_type.resources.non_consumable[
                EVICTION_RATE_INVERSE
            ]
        )
        <= 10
        for instance_type in chosen_instance_types
    )
    pprint.pprint(chosen_instance_types)

    chosen_instance_types = list(
        choose_instance_types_for_job(
            ResourcesInternal.from_cpu_and_memory(1000, 24000, 10), 1, instance_types
        )
    )
    assert len(chosen_instance_types) == 0
