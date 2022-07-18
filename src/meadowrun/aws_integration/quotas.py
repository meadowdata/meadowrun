from __future__ import annotations

import re
from typing import TYPE_CHECKING, Dict, Optional, Tuple

import boto3

if TYPE_CHECKING:
    from meadowrun.instance_selection import OnDemandOrSpotType


# data manually copied from
# https://us-east-2.console.aws.amazon.com/servicequotas/home/services/ec2/quotas
_QUOTA_TO_INSTANCE_FAMILY: Dict[OnDemandOrSpotType, Dict[str, Tuple[str, ...]]] = {
    "spot": {
        "L-34B43A08": tuple("acdhimrtz"),
        "L-85EED4F7": ("dl",),
        "L-88CF9481": ("f",),
        "L-3819A6DF": ("g", "vt"),
        "L-B5D1601B": ("inf",),
        "L-7212CCBC": ("p",),
        "L-E3A00192": ("x",),
    },
    "on_demand": {
        "L-1216C47A": tuple("acdhimrtz"),
        "L-6E869C2A": ("dl",),
        "L-74FC7D96": ("f",),
        "L-DB2E81BA": ("g", "vt"),
        "L-1945791B": ("inf",),
        "L-417A185B": ("p",),
        "L-7295265B": ("x",),
        # TODO L-43DA4232 is for "high-memory" instances
    },
}

_INSTANCE_FAMILY_TO_QUOTA: Dict[OnDemandOrSpotType, Dict[str, str]] = {
    on_demand_or_spot: {
        family: quota
        for quota, families in quota_to_instance_family.items()
        for family in families
    }
    for on_demand_or_spot, quota_to_instance_family in _QUOTA_TO_INSTANCE_FAMILY.items()
}


class UnknownQuotaException(Exception):
    pass


def instance_family_from_type(instance_type: str) -> Optional[str]:
    instance_type = instance_type.lower()
    match = re.match(r"[a-z]+", instance_type)
    if match is None:
        return None
    return match.group()


_NOTE_THAT_TERMINATED_INSTANCES = (
    "(Note that terminated instances sometimes count against this limit: "
    "https://stackoverflow.com/a/54538652/908704 Also, quota increases are not granted "
    "immediately.)"
)


def get_quota_for_instance_type(
    instance_type: str, on_demand_or_spot: OnDemandOrSpotType, region_name: str
) -> Tuple[str, str, Tuple[str, ...]]:
    # this should be pretty rare, but we need to just throw an exception if we can't
    # figure out what kind of quota we're hitting
    unable_to_launch = (
        f"Unable to launch new {instance_type} {on_demand_or_spot} instances due to"
    )
    unknown_quota_exception = UnknownQuotaException(
        f"{unable_to_launch} a quota in your account. Please use the Service Quota tool"
        " in the AWS Console (https://.console.aws.amazon.com/servicequotas/home/"
        "services/ec2/quotas make sure the region is correct!) to find the appropriate"
        f" quota and increase it {_NOTE_THAT_TERMINATED_INSTANCES}."
    )

    instance_family = instance_family_from_type(instance_type)
    if instance_family is None:
        raise unknown_quota_exception
    quota_code = _INSTANCE_FAMILY_TO_QUOTA[on_demand_or_spot].get(instance_family)
    if quota_code is None:
        raise unknown_quota_exception

    quota_instance_families = _QUOTA_TO_INSTANCE_FAMILY[on_demand_or_spot][quota_code]

    current_quota_instance_families = ", ".join(quota_instance_families)
    quota_is_met_run_command_line = (
        f"This quota is currently met. Run `aws service-quotas "
        f"request-service-quota-increase --service-code ec2 --quota-code {quota_code} "
        "--desired-value X` to set the quota to X, where X is larger than the current "
        "quota."
    )

    try:
        current_quota_value = boto3.client(
            "service-quotas", region_name=region_name
        ).get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)["Quota"]["Value"]
    except Exception:
        message = (
            f"{unable_to_launch} the {quota_code} quota, which limits how many CPUs you"
            f" can have across all of your {on_demand_or_spot} instances from the "
            f"{current_quota_instance_families} instance families. "
            f"{quota_is_met_run_command_line} {_NOTE_THAT_TERMINATED_INSTANCES}"
        )
    else:
        message = (
            f"{unable_to_launch} the {quota_code} quota which is set to "
            f"{current_quota_value}. This means you cannot have more than "
            f"{current_quota_value} CPUs across all of your {on_demand_or_spot} "
            f"instances from the {current_quota_instance_families} instance families. "
            f"{quota_is_met_run_command_line} {_NOTE_THAT_TERMINATED_INSTANCES}"
        )

    return message, quota_code, quota_instance_families
