from typing import Dict, List

import boto3


_QUOTA_TO_INSTANCE_FAMILY = {
    "L-34B43A08": tuple("acdhimrtz"),
    "L-3819A6DF": ("g", "vt"),
    "L-7212CCBC": ("p",),
    "L-85EED4F7": ("dl",),
    "L-88CF9481": ("f",),
    "L-B5D1601B": ("inf",),
    "L-E3A00192": ("x",),
}


def _construct_instance_family_to_quota() -> List[Dict[str, str]]:
    families_by_length: List[Dict[str, str]] = []

    for quota, families in _QUOTA_TO_INSTANCE_FAMILY.items():
        for family in families:
            # e.g. a len 3 string will have l = 2
            index = len(family) - 1
            # e.g. if we've only seen len 1 strings, curr_max will be 1
            curr_max = len(families_by_length)
            # continuing example, we only have [{}] and we need [{}, {}, {}]
            for _ in range(curr_max, index + 1):
                families_by_length.append({})
            families_by_length[index][family] = quota

    return families_by_length


_INSTANCE_FAMILY_TO_QUOTA = _construct_instance_family_to_quota()


class SpotQuotaException(Exception):
    def __init__(self, instance_type: str, region_name: str):
        quota_code = None
        for i, families in reversed(list(enumerate(_INSTANCE_FAMILY_TO_QUOTA))):
            quota_code = families.get(instance_type[: i + 1].lower(), None)
            if quota_code is not None:
                break

        note_that_terminated_instances = (
            "(note that terminated instances sometimes count against this limit: "
            "https://stackoverflow.com/a/54538652/908704 Also, quota increases are not"
            "granted immediately.)"
        )
        if quota_code is None:
            message = (
                f"You are unable to launch new {instance_type} spot instances because "
                f"of an AWS spot limit quota {note_that_terminated_instances}. Please "
                "use the Service Quota tool in the AWS Console "
                "(https://us-east-2.console.aws.amazon.com/servicequotas/home/services/ec2/quotas)"  # noqa: E501
                " to find the appropriate quota and increase it."
            )
        else:
            current_quota_instance_families = ", ".join(
                _QUOTA_TO_INSTANCE_FAMILY[quota_code]
            )
            quota_is_met_run_command_line = (
                f"This quota is currently met {note_that_terminated_instances}. Run "
                "`aws service-quotas request-service-quota-increase --service-code ec2 "
                f"--quota-code {quota_code} --desired-value X` to set the quota to X, "
                "where X is larger than the current quota."
            )

            try:
                current_quota_value = boto3.client(
                    "service-quotas", region_name=region_name
                ).get_service_quota(ServiceCode="ec2", QuotaCode=quota_code)["Quota"][
                    "Value"
                ]
            except Exception:
                message = (
                    f"The spot quota {quota_code} limits how many CPUs you can have "
                    "across all of your spot instances from the "
                    f"{current_quota_instance_families} instance families. "
                    f"{quota_is_met_run_command_line}"
                )
            else:
                message = (
                    f"The spot quota {quota_code} is set to {current_quota_value} which"
                    f" means you cannot have more than {current_quota_value} CPUs "
                    "across all of your spot instances from the "
                    f"{current_quota_instance_families} instance families. "
                    f"{quota_is_met_run_command_line}"
                )

        super().__init__(message)
