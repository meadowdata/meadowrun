from __future__ import annotations

import dataclasses
from typing import Dict, Optional, Tuple


def _assert_is_not_none(resources: Optional[Resources]) -> Resources:
    """A helper for mypy"""
    assert resources is not None
    return resources


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


def _remaining_resources_sort_key(
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
