from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable
import datetime as dt

if TYPE_CHECKING:
    from meadowrun.aws_integration.management_lambdas.provisioning import Threshold


@dataclass
class ManagementConfig:
    """Configuration used by management lambdas.

    Attributes:
        terminate_instances_if_idle_for: How long instances should be idle for, i.e. not
            running any jobs, before they are considered for termination.

        instance_thresholds: a list of thresholds, which determine instances to keep
            running regardless of idle time.
    """

    terminate_instances_if_idle_for: dt.timedelta = dt.timedelta(minutes=5)
    instance_thresholds: Iterable[Threshold] = ()


def get_config() -> ManagementConfig:
    try:
        from aws_custom_management_config import get_config as custom_config

        return custom_config()
    except ImportError:
        return ManagementConfig()
