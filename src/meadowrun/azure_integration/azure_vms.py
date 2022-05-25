from __future__ import annotations

import asyncio
import uuid
from typing import Optional, Tuple, Sequence

import azure.core.exceptions
from azure.mgmt.compute.aio import ComputeManagementClient
from azure.mgmt.compute.models import ResourceIdentityType
from azure.mgmt.network.aio import NetworkManagementClient

from meadowrun.azure_integration.azure_core import (
    MEADOWRUN_RESOURCE_GROUP_NAME,
    _ensure_managed_identity,
    ensure_meadowrun_resource_group,
    get_default_location,
    get_subscription_id,
)
from meadowrun.azure_integration.mgmt_functions.azure_instance_alloc_stub import (
    get_credential_aio,
)
from meadowrun.azure_integration.azure_vm_pricing import get_vm_types
from meadowrun.instance_selection import (
    CloudInstance,
    OnDemandOrSpotType,
    Resources,
    choose_instance_types_for_job,
)

_MEADOWRUN_VIRTUAL_NETWORK_NAME = "Meadowrun-vnet"
_MEADOWRUN_SUBNET_NAME = "Meadowrun-subnet"
_MEADOWRUN_USERNAME = "meadowrunuser"

# This id does not appear to be available on the Azure Portal. The easiest way to get it
# is to navigate to the image definition in the portal and then run `az sig
# image-definition list --resource-group meadowrun-dev --gallery-name
# meadowrun.dev.gallery`
_MEADOWRUN_IMAGE_DEFINITION_ID = (
    "/subscriptions/d740513f-4172-4792-bd29-5194e79d5881/resourceGroups/meadowrun-dev/"
    "providers/Microsoft.Compute/galleries/meadowrun.dev.gallery/images/meadowrun"
)
_MEADOWRUN_IMAGE_VERSION = "0.0.9"
# This is distinct from the resource group referenced in
# ensure_meadowrun_resource_group--that resource group is for the meadowrun-generated
# VMs in the user's subscription. This resource group is for the meadowrun-published
# community image
_MEADOWRUN_IMAGE_RESOURCE_GROUP = "meadowrun-dev"


async def _ensure_virtual_network_and_subnet(
    network_client: NetworkManagementClient, location: str
) -> str:
    """
    Returns the meadowrun subnet's id, creates it if it doesn't exist, and creates the
    meadowrun virtual network if that doesn't exist either.
    """
    resource_group_name = await ensure_meadowrun_resource_group(location)

    try:
        await network_client.virtual_networks.get(
            resource_group_name, _MEADOWRUN_VIRTUAL_NETWORK_NAME
        )
    except azure.core.exceptions.ResourceNotFoundError:
        print(
            f"The meadowrun virtual network ({_MEADOWRUN_VIRTUAL_NETWORK_NAME}) "
            f"doesn't exist, in the meadowrun resource group "
            f"({MEADOWRUN_RESOURCE_GROUP_NAME}), creating it now in {location}"
        )

        # Provision the virtual network and wait for completion
        poller = await network_client.virtual_networks.begin_create_or_update(
            resource_group_name,
            _MEADOWRUN_VIRTUAL_NETWORK_NAME,
            {
                "location": location,
                "address_space": {"address_prefixes": ["10.0.0.0/16"]},
            },
        )

        await poller.result()

    try:
        meadowrun_subnet = await network_client.subnets.get(
            resource_group_name,
            _MEADOWRUN_VIRTUAL_NETWORK_NAME,
            _MEADOWRUN_SUBNET_NAME,
        )
    except azure.core.exceptions.ResourceNotFoundError:
        print(
            f"The meadowrun subnet ({_MEADOWRUN_SUBNET_NAME}) in "
            f"({_MEADOWRUN_VIRTUAL_NETWORK_NAME}/{MEADOWRUN_RESOURCE_GROUP_NAME}),"
            f" doesn't exist, creating it now in {location}"
        )

        poller = await network_client.subnets.begin_create_or_update(
            resource_group_name,
            _MEADOWRUN_VIRTUAL_NETWORK_NAME,
            _MEADOWRUN_SUBNET_NAME,
            {"address_prefix": "10.0.0.0/24"},
        )
        meadowrun_subnet = await poller.result()

    return meadowrun_subnet.id


async def _provision_nic_with_public_ip(
    network_client: NetworkManagementClient,
    location: str,
    vm_name: str,
) -> Tuple[str, str]:
    """
    Creates a public IP address and corresponding NIC. Returns (NIC id, public address
    name)
    """

    subnet_id = await _ensure_virtual_network_and_subnet(network_client, location)

    # provision an IP address
    # TODO there should probably be a way to re-use IP addresses rather than
    # create/destroy them for every VM
    poller = await network_client.public_ip_addresses.begin_create_or_update(
        await ensure_meadowrun_resource_group(location),
        f"{vm_name}-ip",
        {
            "location": location,
            # critical that we use Basic here instead of Standard, otherwise this will
            # not be available via instance metadata:
            # https://github.com/MicrosoftDocs/azure-docs/issues/44314
            "sku": {"name": "Basic"},
            "public_ip_allocation_method": "Dynamic",
            "public_ip_address_version": "IPV4",
        },
    )
    ip_address_result = await poller.result()

    print(f"Provisioned public IP address {ip_address_result.name}")

    # provision a NIC
    poller = await network_client.network_interfaces.begin_create_or_update(
        await ensure_meadowrun_resource_group(location),
        f"{vm_name}-ip",
        {
            "location": location,
            "ip_configurations": [
                {
                    "name": f"{vm_name}-ip-config",
                    "subnet": {"id": subnet_id},
                    "public_ip_address": {
                        "id": ip_address_result.id,
                        "properties": {"deleteOption": "Delete"},
                    },
                }
            ],
        },
    )

    nic_result = await poller.result()
    print(f"Provisioned network interface client {nic_result.name}")

    return nic_result.id, ip_address_result.name


async def _provision_vm(
    location: str,
    vm_size: str,
    on_demand_or_spot: OnDemandOrSpotType,
    ssh_public_key_data: str,
) -> Tuple[str, str]:
    """
    Based on
    https://docs.microsoft.com/en-us/azure/developer/python/sdk/examples/azure-sdk-example-virtual-machines?tabs=cmd

    Returns the (public IP address of the VM, name of the VM).

    ssh_public_key_data should be the actual line that should get added to
    ~/.ssh/authorized_keys, e.g. "ssh-rsa: ...". Azure does seem to have a feature for
    storing SSH public keys for reuse, but there doesn't seem to be a way to reference
    them from the VM API.
    """

    resource_group_name = await ensure_meadowrun_resource_group(location)
    meadowrun_identity_id, _ = await _ensure_managed_identity(location)

    vm_name = str(uuid.uuid4())

    async with get_credential_aio() as credential, NetworkManagementClient(
        credential, await get_subscription_id()
    ) as network_client, ComputeManagementClient(
        credential, await get_subscription_id()
    ) as compute_client:

        # every VM needs its own NIC and IP address
        nic_id, ip_address_name = await _provision_nic_with_public_ip(
            network_client, location, vm_name
        )

        # https://docs.microsoft.com/en-us/rest/api/compute/virtual-machines/create-or-update
        # The documentation here is poor, the best way to understand how to use these
        # options is to create VMs using the portal and then run `az vm list` on the
        # command line to see how the portal sets these options.
        vm_create_properties = {
            "location": location,
            "hardware_profile": {
                "vm_size": vm_size,
            },
            "storage_profile": {
                "image_reference": {
                    # according to
                    # https://docs.microsoft.com/en-us/rest/api/compute/virtual-machines/create-or-update#imagereference
                    # for sharedGalleryImageId and communityGalleryImageId (and
                    # presumably for "id" as well), we should not use the version field
                    # and instead just give the id of the image definition rather than
                    # the id of a specific version.
                    # TODO until our community compute gallery is enabled by Microsoft
                    # this won't work for anyone unless we explicitly share it with them
                    "id": (
                        f"{_MEADOWRUN_IMAGE_DEFINITION_ID}/versions/"
                        f"{_MEADOWRUN_IMAGE_VERSION}"
                    ),
                    # this does not seem to have any effect, the "id" version takes
                    # precedence
                    "exactVersion": _MEADOWRUN_IMAGE_VERSION,
                    "resourceGroup": _MEADOWRUN_IMAGE_RESOURCE_GROUP,
                },
                "osDisk": {
                    "createOption": "FromImage",
                    "deleteOption": "Delete",
                },
            },
            "os_profile": {
                "computer_name": f"{vm_name}-vm",
                "admin_username": _MEADOWRUN_USERNAME,
                "linux_configuration": {
                    "disable_password_authentication": True,
                    "ssh": {
                        "publicKeys": [
                            {
                                "path": (
                                    f"/home/{_MEADOWRUN_USERNAME}/.ssh/authorized_keys"
                                ),
                                "keyData": ssh_public_key_data,
                            }
                        ]
                    },
                },
            },
            "network_profile": {
                "network_interfaces": [
                    {"id": nic_id, "properties": {"deleteOption": "Delete"}}
                ]
            },
            "identity": {
                "type": ResourceIdentityType.USER_ASSIGNED,
                "user_assigned_identities": {meadowrun_identity_id: {}},
            },
        }
        if on_demand_or_spot == "spot":
            vm_create_properties["priority"] = "Spot"

        poller = await compute_client.virtual_machines.begin_create_or_update(
            resource_group_name,
            f"{vm_name}-vm",
            vm_create_properties,
        )

        vm_result = await poller.result()

        # we have to re-get the ip address object to get the current ip address, as it
        # doesn't get assigned until the ip address is attached to a VM and the VM is
        # launched
        ip_address_result2 = await network_client.public_ip_addresses.get(
            resource_group_name, ip_address_name
        )

        print(f"Provisioned virtual machine {vm_result.name}")

        # TODO the "deleteOption: Delete" takes care of deleting disks, network
        # interfaces, and public IP addresses when the VM is deleted, but we also need
        # to worry about the case where we throw an exception and/or crash after those
        # resources are created but before the VM is created

        return ip_address_result2.ip_address, vm_result.name


async def launch_vms(
    logical_cpu_required_per_job: int,
    memory_gb_required_per_job: float,
    num_jobs: int,
    eviction_rate: float,
    ssh_public_key_data: str,
    location: Optional[str] = None,
) -> Sequence[CloudInstance]:
    """See docstring for launch_ec2_instances"""

    if location is None:
        location = get_default_location()

    chosen_instance_types = choose_instance_types_for_job(
        Resources(memory_gb_required_per_job, logical_cpu_required_per_job, {}),
        num_jobs,
        eviction_rate,
        await get_vm_types(location),
    )
    if len(chosen_instance_types) < 1:
        raise ValueError(
            f"There were no instance types that could be selected for "
            f"memory={memory_gb_required_per_job}, cpu={logical_cpu_required_per_job}"
        )

    vm_detail_tasks = []
    chosen_instance_types_repeated = []

    for instance_type in chosen_instance_types:
        for _ in range(instance_type.num_instances):
            vm_detail_tasks.append(
                _provision_vm(
                    location,
                    instance_type.instance_type.name,
                    instance_type.instance_type.on_demand_or_spot,
                    ssh_public_key_data,
                )
            )
            chosen_instance_types_repeated.append(instance_type)

    vm_details = await asyncio.gather(*vm_detail_tasks)

    return [
        CloudInstance(public_dns_name, vm_name, instance_type)
        for (public_dns_name, vm_name), instance_type in zip(
            vm_details, chosen_instance_types_repeated
        )
    ]
