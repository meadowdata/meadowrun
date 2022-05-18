from __future__ import annotations

import dataclasses
import math
from typing import Dict, Optional, Tuple, List
from typing_extensions import Literal

ON_DEMAND_OR_SPOT_VALUES: Tuple[OnDemandOrSpotType, OnDemandOrSpotType] = (
    "on_demand",
    "spot",
)
OnDemandOrSpotType = Literal["on_demand", "spot"]


@dataclasses.dataclass(frozen=True)
class Resources:
    """
    Can represent both resources available (i.e. on a agent) as well as resources
    required (i.e. by a job)
    """

    memory_gb: float
    logical_cpu: int
    custom: Dict[str, float]

    def subtract(self, required: Resources) -> Optional[Resources]:
        """
        Subtracts "resources required" for a job from self, which is interpreted as
        "resources available" on a agent.

        Returns None if the required resources are not available in self.
        """
        if self.memory_gb < required.memory_gb:
            return None
        if self.logical_cpu < required.logical_cpu:
            return None
        for key, value in required.custom.items():
            if key not in self.custom:
                return None
            if self.custom[key] < required.custom[key]:
                return None

        return Resources(
            self.memory_gb - required.memory_gb,
            self.logical_cpu - required.logical_cpu,
            {
                key: value - required.custom.get(key, 0)
                for key, value in self.custom.items()
            },
        )

    def add(self, returned: Resources) -> Resources:
        """
        Adds back "resources required" for a job to self, which is usually resources
        available on a agent.
        """
        return Resources(
            self.memory_gb + returned.memory_gb,
            self.logical_cpu + returned.logical_cpu,
            {
                key: self.custom.get(key, 0) + returned.custom.get(key, 0)
                for key in set().union(self.custom, returned.custom)
            },
        )


def remaining_resources_sort_key(
    available_resources: Resources, resources_required: Resources
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
            sum(remaining_resources.custom.values()),
            remaining_resources.memory_gb + 2 * remaining_resources.logical_cpu,
        )
    else:
        # 1 is an indicator saying we cannot run this job
        return 1, None


@dataclasses.dataclass(frozen=True)
class CloudInstanceType:
    name: str  # e.g. t2.micro
    memory_gb: float  # e.g. 4 means 4 GiB
    logical_cpu: int  # e.g. 2 means 2 logical (aka virtual) cpus
    price: float  # 0.023 means 0.023 USD per hour to run the instance
    # e.g. 0 for on-demand instances, >0 for spot instances, as a percentage, so values
    # range from 0 to 100.
    interruption_probability: float
    on_demand_or_spot: OnDemandOrSpotType


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

    # TODO instance_type.interruption_probability should always use the latest data
    # rather than always using the number from when the instance was launched
    instance_type: ChosenCloudInstanceType


def choose_instance_types_for_job(
    resources_required: Resources,
    num_workers_to_allocate: int,
    interruption_probability_threshold: float,
    original_instance_types: List[CloudInstanceType],
) -> List[ChosenCloudInstanceType]:
    """
    This chooses how many of which instance types we should launch for a job with 1 or
    more tasks where each task requires resources_required so that
    num_workers_to_allocate tasks can run in parallel. We choose the cheapest instances
    that have interruption probability lower than or equal to the specified threshold.
    If you only want to use on-demand instances that have 0 probability of interruption,
    you can set interruption_probability_threshold to 0. If there are multiple instances
    that are the cheapest, we choose the ones that have the lowest interruption
    probability. If there are still multiple instances, then we diversify among those
    instance types (it seems that interruptions are more likely to happen at the same
    time on the same instance types).

    TODO we should maybe have an option where e.g. if you want to allocate 53 workers
     worth of capacity for a 100-task job, it makes more sense to allocate e.g. 55 or 60
     workers worth of capacity rather than allocating a little machine for the last 3
     workers of capacity
    """

    instance_types = []

    for orig_instance_type in original_instance_types:
        # ignore anything with a higher interruption probability than what we want to
        # tolerate
        if (
            orig_instance_type.interruption_probability
            <= interruption_probability_threshold
        ):
            workers_per_instance_full = math.floor(
                min(
                    orig_instance_type.memory_gb / resources_required.memory_gb,
                    orig_instance_type.logical_cpu / resources_required.logical_cpu,
                )
            )
            # ignore instance types where we won't be able to fit even 1 worker
            if workers_per_instance_full >= 1:
                # if we get the maximum number of workers packed onto the instance type,
                # what is our effective price per worker
                price_per_worker_full = (
                    orig_instance_type.price / workers_per_instance_full
                )

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
        # half a penny per hour), then take the ones that have the lowest probability of
        # interruption (within 1%)
        # TODO maybe the rounding should be configurable?
        best_price_per_worker = min(
            instance_type.price_per_worker_current for instance_type in instance_types
        )
        best = [
            instance_type
            for instance_type in instance_types
            if instance_type.price_per_worker_current - best_price_per_worker < 0.005
        ]
        best_interruption_probability = min(
            instance_type.instance_type.interruption_probability
            for instance_type in best
        )
        best = [
            instance_type
            for instance_type in best
            if instance_type.instance_type.interruption_probability
            - best_interruption_probability
            < 1
        ]

        # At this point, best is the set of instance types that are the cheapest and
        # least interruption-likely for our workload. Next, we'll make sure to take one,
        # so that we definitely make progress on allocating workers to instances. After
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

    return [
        instance_type
        for instance_type in instance_types
        if instance_type.num_instances > 0
    ]
