from __future__ import annotations

import dataclasses
import decimal
import math
import sys
from typing import Any, Dict, Optional, Tuple, Iterable, Union

from typing_extensions import Literal

from meadowrun.config import (
    AVX,
    GPU,
    GPU_MEMORY,
    EVICTION_RATE_INVERSE,
    LOGICAL_CPU,
    MEMORY_GB,
    avx_from_string,
)

ON_DEMAND_OR_SPOT_VALUES: Tuple[OnDemandOrSpotType, OnDemandOrSpotType] = (
    "on_demand",
    "spot",
)
OnDemandOrSpotType = Literal["on_demand", "spot"]


class ResourcesInternal:
    """
    Can represent both resources available (i.e. on a agent) as well as resources
    required (i.e. by a job).

    This is called ResourcesInternal to distinguish it from Resources. Resources is
    mostly used as a friendly way to ultimately construct a ResourcesInternal object,
    which is used internally. Only Resources should be part of public APIs.
    """

    def __init__(self, consumable: Dict[str, float], non_consumable: Dict[str, float]):
        self.consumable = consumable
        self.non_consumable = non_consumable

    def subtract(self, required: ResourcesInternal) -> Optional[ResourcesInternal]:
        """
        Interpreting self as "resources available" on an instance, subtracts "resources
        required" for a job on this instance

        Returns None if the required resources are not available in self.
        """

        for key, value in required.non_consumable.items():
            if key not in self.non_consumable:
                return None
            if self.non_consumable[key] < required.non_consumable[key]:
                return None

        for key, value in required.consumable.items():
            if key not in self.consumable:
                return None
            if self.consumable[key] < required.consumable[key]:
                return None

        return ResourcesInternal(
            {
                key: value - required.consumable.get(key, 0)
                for key, value in self.consumable.items()
            },
            self.non_consumable,
        )

    def add(self, returned: ResourcesInternal) -> ResourcesInternal:
        """
        Interpreting `self` as the available resources on an instance, adds back
        "resources required" for a job that has completed. returned.non_consumable is
        ignored.
        """
        return ResourcesInternal(
            {
                key: self.consumable.get(key, 0) + returned.consumable.get(key, 0)
                for key in set().union(self.consumable, returned.consumable)
            },
            self.non_consumable,
        )

    def combine(self, other: ResourcesInternal) -> ResourcesInternal:
        """
        combines two disjoint sets of resources. This is meant to combine instance type
        resources (like CPU and memory) with runtime resources (like AMI id and subnet
        id).

        I.e. the same resource cannot be present in self and other. This is different
        from add because add ignores non_consumable resources, and supports the case
        when self and other have the same consumable resources.
        """
        if other.consumable:
            new_consumable = self.consumable.copy()
            for key, value in other.consumable.items():
                if key in new_consumable:
                    raise ValueError(
                        f"Cannot combine resources, consumable resource {key} was in "
                        "both resources"
                    )
                new_consumable[key] = value
        else:
            new_consumable = self.consumable

        if other.non_consumable:
            new_non_consumable = self.non_consumable.copy()
            for key, value in other.non_consumable.items():
                if key in new_non_consumable:
                    raise ValueError(
                        f"Cannot combine resources, non-consumable resource {key} was "
                        "in both resources"
                    )
                new_non_consumable[key] = value
        else:
            new_non_consumable = self.non_consumable

        return ResourcesInternal(new_consumable, new_non_consumable)

    def divide_by(self, required: ResourcesInternal) -> int:
        """
        Interpreting `self` as available resources on an instance, returns how many
        tasks that require `required` resources could fit into this instance
        """
        # if a non_consumable required resource isn't available, then we won't be able
        # to fit any tasks
        for key, value in required.non_consumable.items():
            if key not in self.non_consumable:
                return 0
            if self.non_consumable[key] < required.non_consumable[key]:
                return 0

        # find the "binding" consumable that will determine how many tasks can fit on
        # this instance
        result = float(sys.maxsize)
        for key, value in required.consumable.items():
            if key not in self.consumable:
                return 0
            result = min(result, self.consumable[key] / required.consumable[key])

        return math.floor(result)

    def multiply(self, n: int) -> ResourcesInternal:
        """
        available.subtract(required.multiply(2)) should be equivalent to
        available.subtract(required).subtract(required)
        """
        return ResourcesInternal(
            {key: value * n for key, value in self.consumable.items()},
            self.non_consumable,
        )

    def non_standard_consumables(self) -> Iterable[Tuple[str, float]]:
        return (
            item
            for item in self.consumable.items()
            if item[0] != LOGICAL_CPU and item[0] != MEMORY_GB
        )

    def format_cpu_memory_gpu(self) -> str:
        if GPU in self.consumable:
            gpu_string = f", {self.consumable[GPU]} GPU"
        else:
            gpu_string = ""

        return (
            f"{self.consumable[LOGICAL_CPU]} CPU, {self.consumable[MEMORY_GB]} GB"
            + gpu_string
        )

    def format_eviction_rate(self) -> str:
        return f"{100 - self.non_consumable[EVICTION_RATE_INVERSE]}% eviction rate"

    def consumable_as_decimals(self) -> Dict[str, decimal.Decimal]:
        return {key: decimal.Decimal(value) for key, value in self.consumable.items()}

    def non_consumable_as_decimals(self) -> Dict[str, decimal.Decimal]:
        return {
            key: decimal.Decimal(value) for key, value in self.non_consumable.items()
        }

    def __str__(self) -> str:
        # TODO implement better
        other_consumables_str = ", ".join(
            f"{key}: {value}"
            for key, value in self.consumable.items()
            if key not in (LOGICAL_CPU, MEMORY_GB, GPU)
        )
        if other_consumables_str:
            other_consumables_str = ", " + other_consumables_str
        other_non_consumables_str = ", ".join(
            f"{key}: {value}"
            for key, value in self.non_consumable.items()
            if key != EVICTION_RATE_INVERSE
        )
        if other_non_consumables_str:
            other_non_consumables_str = ", " + other_non_consumables_str
        return (
            "Resources: consumables("
            f"{self.format_cpu_memory_gpu()}{other_consumables_str}), "
            f"non_consumables("
            f"{self.format_eviction_rate()}{other_non_consumables_str}"
            f")"
        )

    def copy(self) -> ResourcesInternal:
        return ResourcesInternal(self.consumable.copy(), self.non_consumable.copy())

    @classmethod
    def from_decimals(
        cls,
        consumable: Dict[str, decimal.Decimal],
        non_consumable: Dict[str, decimal.Decimal],
    ) -> ResourcesInternal:
        return cls(
            {key: float(value) for key, value in consumable.items()},
            {key: float(value) for key, value in non_consumable.items()},
        )

    @classmethod
    def from_cpu_and_memory(
        cls,
        logical_cpu: float,
        memory_gb: float,
        max_eviction_rate: Optional[float] = None,
        gpus: Optional[float] = None,
        gpu_memory: Optional[float] = None,
        flags_required: Union[Iterable[str], str, None] = None,
        *,
        other_consumables: Optional[Dict[str, float]] = None,
        other_non_consumables: Optional[Dict[str, float]] = None,
    ) -> ResourcesInternal:
        """A friendly constructor"""
        if other_consumables is None:
            consumables = {}
        else:
            consumables = other_consumables.copy()
        consumables[LOGICAL_CPU] = logical_cpu
        consumables[MEMORY_GB] = memory_gb
        if gpus is not None:
            consumables[GPU] = gpus
        if gpu_memory is not None:
            consumables[GPU_MEMORY] = gpu_memory

        if other_non_consumables is None:
            non_consumables = {}
        else:
            non_consumables = other_non_consumables.copy()

        if max_eviction_rate is not None:
            non_consumables[EVICTION_RATE_INVERSE] = 100.0 - max_eviction_rate

        if flags_required is not None:
            if isinstance(flags_required, str):
                flags_required = (flags_required,)
            for flag in flags_required:
                avx_version = avx_from_string(flag)
                if avx_version is not None:
                    non_consumables[AVX] = float(avx_version)
                else:
                    non_consumables[flag] = 1.0

        return cls(consumables, non_consumables)


def remaining_resources_sort_key(
    available_resources: ResourcesInternal, resources_required: ResourcesInternal
) -> Tuple[int, Optional[Tuple[float, float]]]:
    """
    This takes the available resources for an agent and returns (indicator,
    remaining_resources_sort_key).

    remaining_resources_sort_key indicates how "good" it is to run the job on that agent
    based on how many resources would be left if we created a worker for that job on
    that agent. The general idea is that having fewer resources left is better. E.g. if
    you have agent1 with 8GB+4cpu and agent2 with 2GB+1cpu, and you have a job that
    requires 2GB+1cpu, it's probably better to run it on agent2--that way if another job
    comes along that requires 8GB+4cpu, agent1 is still available to run that job. I.e.
    we prefer to create workers on agents that have a smaller
    remaining_resources_sort_key.

    indicator is either 0 or 1--0 means that it is possible to run the job on this
    agent, whereas 1 means that there are not enough resources to run the job on this
    agent. (We use the indicator this way because smaller is better for the
    remaining_resources_sort_key, so 0 is good and 1 is bad.)

    TODO we should probably be more thoughtful about the exact implementation of
     remaining_resources_sort_key.

    TODO it's possible we should "reserve" availability for future jobs that might come
     along. E.g. if we have many low-priority jobs that require 1 CPU, and then a
     high-priority job requiring 16 CPUs comes along we might wish that we had not used
     all of our resources to run the low-priority jobs. On the other hand, if you
     reserve resources and no future jobs come along, then you're just making the
     current job run slower for no reason.
    """
    remaining_resources = available_resources.subtract(resources_required)
    if remaining_resources is not None:
        # 0 is an indicator saying we can run this job
        return 0, (
            # TODO take non-consumables into account here?
            sum(value for key, value in remaining_resources.non_standard_consumables()),
            remaining_resources.consumable[MEMORY_GB]
            + 2 * remaining_resources.consumable[LOGICAL_CPU],
        )
    else:
        # 1 is an indicator saying we cannot run this job
        return 1, None


@dataclasses.dataclass(frozen=True)
class CloudInstanceType:
    name: str  # e.g. t2.micro
    on_demand_or_spot: OnDemandOrSpotType
    price: float  # 0.023 means 0.023 USD per hour to run the instance

    # Must include consumables:
    # - MEMORY_GB (e.g. 4 means 4 GiB)
    # - LOGICAL CPU (2 means 2 logical aka virtual cpus)
    # And non_consumables:
    # - EVICTION_RATE_INVERSE (e.g. 100 for on-demand instances, <100 for spot instances
    #   as (100 - percentage), so e.g. EVICTION_RATE_INVERSE==75 means 25% eviction rate
    #   aka probability of interruption)
    # TODO EVICTION_RATE_INVERSE should always use the latest data rather than always
    # using the number from when the instance was launched
    resources: ResourcesInternal

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "on_demand_or_spot": self.on_demand_or_spot,
            "price": self.price,
            "resources": {
                "consumable": self.resources.consumable,
                "non_consumable": self.resources.non_consumable,
            },
        }

    @classmethod
    def from_dict(cls, cit: Dict[str, Any]) -> CloudInstanceType:
        return cls(
            cit["name"],
            cit["on_demand_or_spot"],
            float(cit["price"]),
            ResourcesInternal(
                cit["resources"]["consumable"],
                cit["resources"]["non_consumable"],
            ),
        )


@dataclasses.dataclass
class ChosenCloudInstanceType:
    instance_type: CloudInstanceType
    # e.g. 5 means we should allocate 5 of these instances. Will always be >1
    num_instances: int

    # The maximum number of workers that you could put on this instance type
    workers_per_instance_full: int
    # price per worker if we fit as many workers as possible onto the instance
    price_per_worker_full: float

    # for "internal" use:
    workers_per_instance_current: int
    price_per_worker_current: float


@dataclasses.dataclass(frozen=True)
class CloudInstance:
    """Represents an instance launched by an InstanceRegistrar"""

    public_dns_name: str
    name: str

    instance_type: ChosenCloudInstanceType


def choose_instance_types_for_job(
    resources_required: ResourcesInternal,
    num_workers_to_allocate: int,
    original_instance_types: Iterable[CloudInstanceType],
) -> Iterable[ChosenCloudInstanceType]:
    """
    This chooses how many of which instance types we should launch for a job with 1 or
    more tasks where each task requires resources_required so that
    num_workers_to_allocate tasks can run in parallel. We choose the cheapest instances
    that have an eviction rate lower than or equal to the specified threshold. If you
    only want to use on-demand instances that have 0 eviction rate, you can set
    max_eviction_rate to 0. If there are multiple instances that are the cheapest, we
    choose the ones that have the lowest eviction rate. If there are still multiple
    instances, then we diversify among those instance types (it seems that evictions are
    more likely to happen at the same time on the same instance types).

    TODO we should maybe have an option where e.g. if you want to allocate 53 workers
     worth of capacity for a 100-task job, it makes more sense to allocate e.g. 55 or 60
     workers worth of capacity rather than allocating a little machine for the last 3
     workers of capacity
    """

    instance_types = []

    for orig_instance_type in original_instance_types:
        workers_per_instance_full = orig_instance_type.resources.divide_by(
            resources_required
        )
        # ignore instance types where we won't be able to fit even 1 worker
        if workers_per_instance_full >= 1:
            # if we get the maximum number of workers packed onto the instance type,
            # what is our effective price per worker
            price_per_worker_full = orig_instance_type.price / workers_per_instance_full

            instance_types.append(
                ChosenCloudInstanceType(
                    orig_instance_type,
                    0,
                    workers_per_instance_full,
                    price_per_worker_full,
                    workers_per_instance_full,
                    price_per_worker_full,
                )
            )

    # no instance types can run even one worker
    if len(instance_types) == 0:
        return []

    while num_workers_to_allocate > 0:
        # for larger instances, there might not be enough num_workers_to_allocate to
        # make it "worth it" to use that larger instance because we won't need enough
        # workers to fully pack the instance. So we recompute price_per_worker for those
        # instances assuming we only get to put num_workers_to_allocate on that
        # instance. We use ChosenCloudInstanceType.workers_per_instance_current and
        # price_per_worker_current to keep track of this.
        for instance_type in instance_types:
            if instance_type.workers_per_instance_full > num_workers_to_allocate:
                instance_type.price_per_worker_current = (
                    instance_type.instance_type.price / num_workers_to_allocate
                )
                instance_type.workers_per_instance_current = num_workers_to_allocate
            # as long as num_workers_to_allocate decreases on every iteration, we should
            # never need to "reset" price_per_worker_current_iteration or
            # workers_per_instance_current_iteration

        # Now find the instance types that have the lowest price per worker. If there
        # are multiple instance types that have the same price per worker (or are within
        # half a penny per hour), then take the ones that have the lowest eviction rate
        # (within 1%)
        # TODO maybe the rounding should be configurable?
        best_price_per_worker = min(
            instance_type.price_per_worker_current for instance_type in instance_types
        )
        best = [
            instance_type
            for instance_type in instance_types
            if instance_type.price_per_worker_current - best_price_per_worker < 0.005
        ]
        best_eviction_rate = max(
            instance_type.instance_type.resources.non_consumable[EVICTION_RATE_INVERSE]
            for instance_type in best
        )
        best = [
            instance_type
            for instance_type in best
            if best_eviction_rate
            - instance_type.instance_type.resources.non_consumable[
                EVICTION_RATE_INVERSE
            ]
            < 1
        ]

        # At this point, best is the set of instance types that are the cheapest and
        # least eviction-likely for our workload. Next, we'll make sure to take one, so
        # that we definitely make progress on allocating workers to instances. After
        # that, though, if we still have workers left to allocate, we want to allocate
        # them round-robin to the remaining instance types in best--the diversity in
        # instance types is good because instances of the same type are more likely to
        # get interrupted at the same time according to
        # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-fleet-allocation-strategy.html

        # iterating from largest to smallest will make things easier
        best.sort(
            key=lambda instance_type: instance_type.workers_per_instance_full,
            reverse=True,
        )

        # take the first one no matter what
        best[0].num_instances += 1
        num_workers_to_allocate -= best[0].workers_per_instance_current

        # Now that we've decreased num_workers_to_allocate, we need to make sure
        # price_per_worker is still accurate (i.e. num_workers_to_allocate could have
        # fallen below workers_per_instance). If there are any instance types that where
        # price_per_worker is still accurate, then we'll continue to allocate to those
        # in a round-robin-ish fashion. Once there are no more instance types where
        # price_per_worker is still accurate, we'll go through the loop again and
        # recompute price_per_worker.
        i = 0
        while True:
            best = [
                instance_type
                for instance_type in best
                if instance_type.workers_per_instance_current <= num_workers_to_allocate
            ]
            if len(best) == 0:
                break
            # this is...very inexact because best is changing as we iterate, but the
            # idea is to walk through the options in best one by one
            i = (i + 1) % len(best)
            best[i].num_instances += 1
            num_workers_to_allocate -= best[i].workers_per_instance_current

    return (
        instance_type
        for instance_type in instance_types
        if instance_type.num_instances > 0
    )
