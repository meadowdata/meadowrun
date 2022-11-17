from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance
from meadowrun.instance_selection import ResourcesInternal

if TYPE_CHECKING:
    from meadowrun.instance_selection import CloudInstanceType, OnDemandOrSpotType
    from meadowrun.run_job_core import Resources


@dataclasses.dataclass
class Threshold:
    """Specifies resources (EC2 instances) to run regardless of whether they're used to
    run jobs. Only used in configuration.

    Attributes:
        resources: A chunk of resources (cpus, memory)
        num_resources: How many resource chunks to run
        instance_type: Restrictions on the machines, such as custom ami and region
    """

    resources: Resources
    num_resources: int = 1
    instance_type: AllocEC2Instance = dataclasses.field(
        default_factory=AllocEC2Instance
    )

    threshold_as_resource: ResourcesInternal = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        res = self.resources.to_internal()
        alloc = self.instance_type.get_runtime_resources()
        self.threshold_as_resource = res.combine(alloc)

    def accepts(self, instance_resources: ResourcesInternal) -> bool:
        # Returns True if the given instance's resources can contribute to this
        # threshold.
        # special rule - since GPUs are typically expensive, don't keep GPU instances
        # around when threshold is not asking GPU resources.
        # (this also excludes when the threshold has GPUs but the instance does not)
        if (
            self.threshold_as_resource.has_gpu_consumables()
            != instance_resources.has_gpu_consumables()
        ):
            return False

        leftover = instance_resources.subtract(self.threshold_as_resource)
        if leftover is None:
            # the instance does not have the required resources, or too little of them
            return False

        return True

    def is_reached(self, resources: ResourcesInternal) -> bool:
        # Returns true if the given resources equal or exceed this threshold's.
        return self.total().consumables_le(resources)

    def total(self) -> ResourcesInternal:
        if self.num_resources == 1:
            return self.threshold_as_resource
        return self.threshold_as_resource.multiply(self.num_resources)


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

    def _total_allocated(self) -> ResourcesInternal:
        total_allocated = ResourcesInternal({}, {})
        for instance in self.instances:
            total_allocated = total_allocated.add(instance[1].resources)
        return total_allocated

    def is_threshold_reached(self) -> bool:
        return self.threshold.is_reached(self._total_allocated())

    def unmet_threshold(self) -> Optional[Tuple[Threshold, int]]:
        if self.is_threshold_reached():
            return None

        threshold_res = self.threshold.resources.to_internal()
        num_resources_met = 0
        for instance_id, instance_type in self.instances:
            num_resources_met += instance_type.resources.divide_by(threshold_res)
        num_unmet_resources = self.threshold.num_resources - num_resources_met
        assert num_unmet_resources > 0
        return self.threshold, num_unmet_resources


def _augmented_cloud_instance_type(
    instance_type: CloudInstanceType, instance_specifc: ResourcesInternal
) -> CloudInstanceType:
    return dataclasses.replace(
        instance_type, resources=instance_type.resources.combine(instance_specifc)
    )


def shutdown_thresholds(
    thresholds: Iterable[Threshold],
    instances: Dict[InstanceId, InstanceTypeKey],
    type_to_info: Dict[InstanceTypeKey, CloudInstanceType],
    instance_to_resources: Dict[InstanceId, ResourcesInternal],
) -> Tuple[List[InstanceId], List[Tuple[Threshold, int]]]:
    """Returns a list of instance ids that can be shut down, taking into account
    thresholds only. Also returns a lit of thresholds that are not met. The latter is
    used to start new instances."""

    assignments = [Assignment(threshold) for threshold in thresholds]

    instance_id_type = [
        (
            instance_id,
            _augmented_cloud_instance_type(
                type_to_info[instance_type_key], instance_to_resources[instance_id]
            ),
        )
        for instance_id, instance_type_key in instances.items()
    ]

    # Pass 1.
    # Assign cheapest machines to thresholds first, until the threshold is reached.
    _greedy_assignment(assignments, instance_id_type)

    # Pass 2.
    # Remove from assignments any small superfluous machines.
    # After the first pass, it's possible that there is an assignment that has several
    # small machines and then one big machine, which puts it far over the threshold.
    # Here the small machines are removed. This favors a small amount of big machines
    # over many small machines - this is normally more cost-effective, and simpler to
    # manage.
    _remove_small_machines(assignments, instance_id_type)

    excess_instances = [inst[0] for inst in instance_id_type]

    unmet_thresholds = []
    for assignment in assignments:
        unmet_threshold = assignment.unmet_threshold()
        if unmet_threshold:
            unmet_thresholds.append(unmet_threshold)

    return excess_instances, unmet_thresholds


def _greedy_assignment(
    assignments: List[Assignment],
    instances: List[Tuple[InstanceId, CloudInstanceType]],
) -> None:
    """Modifies assignemtns and instances!"""
    instances.sort(key=lambda i: i[1].price, reverse=True)
    for assignment in assignments:
        unassigned = []
        while len(instances) > 0 and not assignment.is_threshold_reached():
            candidate = instances.pop()
            if assignment.accepts(candidate):
                assignment.assign(candidate)
            else:
                unassigned.append(candidate)
        # put the unassigned instances back, to be considered for the next assignemnt.
        instances.extend(unassigned)


def _remove_small_machines(assignments: List[Assignment], instances: List[Any]) -> None:
    """Modifies assignements and instances!"""
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
