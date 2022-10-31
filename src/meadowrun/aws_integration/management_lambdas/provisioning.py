from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Dict, FrozenSet, Iterable, List, Tuple

from meadowrun.instance_selection import ResourcesInternal


if TYPE_CHECKING:
    from meadowrun.instance_selection import CloudInstanceType, OnDemandOrSpotType


@dataclasses.dataclass(frozen=True)
class Threshold:
    times: int = 1
    logical_cpu_count: float = 0.0
    memory_gb: float = 0.0
    gpu_count: float = 0.0
    gpu_memory_gb: float = 0.0
    flags: FrozenSet[str] = frozenset()

    def accepts(self, resources: ResourcesInternal) -> bool:
        """Returns True if the given resource can contribute to this threshold."""

        def check_flags() -> bool:
            required_flags = ResourcesInternal.from_cpu_and_memory(
                0.0, 0.0, flags_required=self.flags
            )
            for (
                required_flag_k,
                required_flag_v,
            ) in required_flags.non_consumable.items():
                if not (
                    required_flag_k in resources.non_consumable
                    and resources.non_consumable[required_flag_k] >= required_flag_v
                ):
                    return False
            return True

        return (
            # check that the resource is "chunky" enough
            resources.logical_cpu >= self.logical_cpu_count
            and resources.memory_gb >= self.memory_gb
            and resources.gpu >= self.gpu_count
            and resources.gpu_memory_gb >= self.gpu_memory_gb
            # machines with GPU requirements don't contribute to non-GPU thresholds,
            # because it's counter-intuitive, and expensive to keep a GPU machine alive
            # when all you want is some cheap CPUs.
            and (
                # not A or B === A => B
                not (self.gpu_count == 0.0 and self.gpu_memory_gb == 0.0)
                or (resources.gpu == 0.0 and resources.gpu_memory_gb == 0.0)
            )
            # all threshold flags must be present in the resource
            and check_flags()
        )

    def is_reached(self, resources: ResourcesInternal) -> bool:
        return (
            self.logical_cpu_count <= resources.logical_cpu
            and self.memory_gb <= resources.memory_gb
            and self.gpu_count <= resources.gpu
            and self.gpu_memory_gb <= resources.gpu_memory_gb
        )

    def total(self) -> Threshold:
        if self.times == 1:
            return self
        return Threshold(
            times=1,
            logical_cpu_count=self.times * self.logical_cpu_count,
            memory_gb=self.times * self.memory_gb,
            gpu_count=self.times * self.gpu_count,
            gpu_memory_gb=self.times * self.gpu_memory_gb,
            flags=self.flags,
        )


if TYPE_CHECKING:
    InstanceId = str
    InstanceTypeKey = Tuple[str, OnDemandOrSpotType]


@dataclasses.dataclass
class Assignment:
    threshold: Threshold
    instances: List[Tuple[InstanceId, CloudInstanceType]] = dataclasses.field(
        default_factory=list
    )

    def accepts(self, instance: Tuple[InstanceId, CloudInstanceType]) -> bool:
        return self.threshold.accepts(instance[1].resources)

    def assign(self, instance: Tuple[InstanceId, CloudInstanceType]) -> None:
        self.instances.append(instance)

    def is_threshold_reached(self) -> bool:
        total_threshold = self.threshold.total()
        total_allocated = ResourcesInternal({}, {})
        for instance in self.instances:
            total_allocated = total_allocated.add(instance[1].resources)
        return total_threshold.is_reached(total_allocated)


def shutdown_thresholds(
    thresholds: Iterable[Threshold],
    instances: Dict[InstanceId, InstanceTypeKey],
    type_to_info: Dict[InstanceTypeKey, CloudInstanceType],
) -> List[InstanceId]:

    assignments = [Assignment(threshold) for threshold in thresholds]

    sorted_instances = [
        (instance_id, type_to_info[instance_type_key])
        for instance_id, instance_type_key in instances.items()
    ]
    sorted_instances.sort(key=lambda i: i[1].price, reverse=True)

    # Pass 1.
    # Assign cheapest machines to thresholds first, until the threshold is reached.
    _greedy_assignment(assignments, sorted_instances)

    # Pass 2.
    # Remove from assignments any small superfluous machines.
    # After the first pass, it's possible that there is an assignment that has several
    # small machines and then one big machine, which puts it far over the threshold.
    # Here the small machines are removed. This favors a small amount of big machines
    # over many small machines - this is normally more cost-effective, and simpler to
    # manage.
    to_shut_down = sorted_instances
    _remove_small_machines(assignments, to_shut_down)

    return [inst[0] for inst in sorted_instances]


def _greedy_assignment(
    assignments: List[Assignment], sorted_instances: List[Any]
) -> None:
    for assignment in assignments:
        unassigned = []
        while len(sorted_instances) > 0 and not assignment.is_threshold_reached():
            candidate = sorted_instances.pop()
            if assignment.accepts(candidate):
                assignment.assign(candidate)
            else:
                unassigned.append(candidate)
        # put the unassigned instances back, to be considered for the next assignemnt.
        sorted_instances.extend(unassigned)


def _remove_small_machines(assignments: List[Assignment], instances: List[Any]) -> None:
    for assignment in assignments:
        original_len = len(assignment.instances)
        removed_instances = []
        for _ in range(original_len):
            instance = assignment.instances.pop(0)
            if assignment.is_threshold_reached():
                removed_instances.append(instance)
            else:
                assignment.instances.append(instance)
        instances.extend(removed_instances)
