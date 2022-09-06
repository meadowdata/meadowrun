from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from meadowrun.aws_integration.ec2_instance_allocation import AllocEC2Instance
from meadowrun.azure_integration.azure_instance_allocation import AllocAzureVM

if TYPE_CHECKING:
    from meadowrun.run_job_core import CloudProviderType, AllocVM


def AllocCloudInstance(
    cloud_provider: CloudProviderType, region_name: Optional[str] = None
) -> AllocVM:
    """
    This function is deprecated and will be removed in version 1.0.0. Please use
    [AllocEC2Instance][meadowrun.AllocEC2Instance] or
    [AllocAzureVM][meadowrun.AllocAzureVM] directly.
    """
    if cloud_provider == "EC2":
        return AllocEC2Instance(region_name)
    elif cloud_provider == "AzureVM":
        return AllocAzureVM(region_name)
    else:
        raise ValueError(f"Unexpected value for cloud_provider {cloud_provider}")
