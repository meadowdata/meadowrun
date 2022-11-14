from meadowrun.aws_integration.management_lambdas.config import ManagementConfig
import datetime as dt

from meadowrun import AllocEC2Instance, Resources
from meadowrun.aws_integration.management_lambdas.provisioning import Threshold


def get_config() -> ManagementConfig:
    defaults = ManagementConfig()

    return defaults
