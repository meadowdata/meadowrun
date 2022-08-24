from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Optional, Tuple, Sequence, Awaitable, Any, Dict

if TYPE_CHECKING:
    from typing_extensions import Literal

from meadowrun.azure_integration.azure_meadowrun_core import (
    _ensure_managed_identity,
    ensure_meadowrun_resource_group,
    get_default_location,
)
from meadowrun.azure_integration.azure_vm_pricing import get_vm_types
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_exceptions import (
    ResourceNotFoundError,
)
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    azure_rest_api,
    azure_rest_api_poll,
    wait_for_poll,
)
from meadowrun.instance_selection import (
    CloudInstance,
    OnDemandOrSpotType,
    ResourcesInternal,
    choose_instance_types_for_job,
)
from meadowrun.version import __version__


_MEADOWRUN_VIRTUAL_NETWORK_NAME = "Meadowrun-vnet"
_MEADOWRUN_SUBNET_NAME = "Meadowrun-subnet"
_MEADOWRUN_USERNAME = "meadowrunuser"


# To get this ID, build an image using build_scripts/build_azure_image.py. Once you have
# an image, you'll need to get the community image id (NOT the image's regular id!) and
# copy/paste that here, using the following command:
# https://docs.microsoft.com/en-us/cli/azure/sig/image-definition?view=azure-cli-latest#az-sig-image-definition-list-community
_MEADOWRUN_COMMUNITY_IMAGE_ID = (
    "/CommunityGalleries/meadowprodeastus-e8b60fd5-8978-467b-a1b0-5b83cbf5393d/Images/"
    "meadowrun"
)


async def _ensure_virtual_network_and_subnet(location: str) -> str:
    """
    Returns the meadowrun subnet's id, creates it if it doesn't exist, and creates the
    meadowrun virtual network if that doesn't exist either.
    """
    virtual_network_path = (
        f"{await ensure_meadowrun_resource_group(location)}/providers/"
        f"Microsoft.Network/virtualnetworks/{_MEADOWRUN_VIRTUAL_NETWORK_NAME}"
    )
    subnet_path = f"{virtual_network_path}/subnets/{_MEADOWRUN_SUBNET_NAME}"

    try:
        subnet = await azure_rest_api("GET", subnet_path, "2021-05-01")
    except ResourceNotFoundError:
        print(
            f"The meadowrun virtual network ({_MEADOWRUN_VIRTUAL_NETWORK_NAME}) and/or "
            f"the subnet in it ({_MEADOWRUN_SUBNET_NAME}) does not exist, creating it "
            f"now in {location}"
        )

        # Provision the virtual network and wait for completion, will just get updated
        # if it already exists.
        # https://docs.microsoft.com/en-us/rest/api/virtualnetwork/virtual-networks/create-or-update
        await wait_for_poll(
            await azure_rest_api_poll(
                "PUT",
                virtual_network_path,
                "2021-05-01",
                "AsyncOperationJsonStatus",
                json_content={
                    "location": location,
                    "properties": {
                        "addressSpace": {"addressPrefixes": ["10.0.0.0/16"]},
                    },
                },
            )
        )

        # Then create the subnet
        # https://docs.microsoft.com/en-us/rest/api/virtualnetwork/subnets/create-or-update
        subnet, _ = await wait_for_poll(
            await azure_rest_api_poll(
                "PUT",
                subnet_path,
                "2021-05-01",
                "AsyncOperationJsonStatus",
                json_content={"properties": {"addressPrefix": "10.0.0.0/24"}},
            )
        )

    return subnet["id"]


async def _provision_nic_with_public_ip(
    location: str,
    vm_name: str,
) -> Tuple[str, str, Optional[Awaitable[Any]], Optional[Awaitable[Any]]]:
    """
    Creates a public IP address and corresponding NIC. Returns (NIC id, public address
    path, NIC creation continuation, ip address creation continuation). The NIC id and
    public address path can be used to start provisioning a VM, but the continuations
    must be awaited before using the VM
    """

    subnet_id = await _ensure_virtual_network_and_subnet(location)

    # provision an IP address
    # TODO there should probably be a way to re-use IP addresses rather than
    # create/destroy them for every VM
    resource_group_path = await ensure_meadowrun_resource_group(location)
    public_ip_path = (
        f"{resource_group_path}/providers/Microsoft.Network/publicIPAddresses"
        f"/{vm_name}-ip"
    )

    # https://docs.microsoft.com/en-us/rest/api/virtualnetwork/public-ip-addresses/create-or-update
    ip_address_result, ip_address_continuation = await azure_rest_api_poll(
        "PUT",
        public_ip_path,
        "2021-05-01",
        "AsyncOperationJsonStatus",
        json_content={
            "location": location,
            # critical that we use Basic here instead of Standard, otherwise this will
            # not be available via instance metadata (IMDS):
            # https://github.com/MicrosoftDocs/azure-docs/issues/44314
            "sku": {"name": "Basic"},
            "properties": {
                "publicIPAllocationMethod": "Dynamic",
                "publicIPAddressVersion": "IPV4",
            },
        },
    )

    # provision a NIC
    nic_result, nic_continuation = await azure_rest_api_poll(
        "PUT",
        f"{resource_group_path}/providers/Microsoft.Network/networkInterfaces"
        f"/{vm_name}-nic",
        "2021-05-01",
        "AsyncOperationJsonStatus",
        json_content={
            "location": location,
            "properties": {
                "ipConfigurations": [
                    {
                        "name": f"{vm_name}-ip-config",
                        "properties": {
                            "subnet": {"id": subnet_id},
                            "publicIPAddress": {
                                "id": ip_address_result["id"],
                                "properties": {"deleteOption": "Delete"},
                            },
                        },
                    }
                ]
            },
        },
    )

    return nic_result["id"], public_ip_path, nic_continuation, ip_address_continuation


async def _provision_vm(
    location: str,
    vm_size: str,
    on_demand_or_spot: OnDemandOrSpotType,
    ssh_public_key_data: str,
    image_id: str,
    image_type: Literal["Community", "Regular"],
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

    resource_group_path = await ensure_meadowrun_resource_group(location)
    meadowrun_identity_id, _ = await _ensure_managed_identity(location)

    vm_name = str(uuid.uuid4())

    # every VM needs its own NIC and IP address
    (
        nic_id,
        ip_address_path,
        nic_continuation,
        ip_address_continuation,
    ) = await _provision_nic_with_public_ip(location, vm_name)

    # https://docs.microsoft.com/en-us/rest/api/compute/virtual-machines/create-or-update
    # The documentation here is poor, the best way to understand how to use these
    # options is to create VMs using the portal and then run `az vm list` on the
    # command line to see how the portal sets these options.
    vm_create_properties: Dict[str, Any] = {
        "location": location,
        "identity": {
            "type": "UserAssigned",
            "userAssignedIdentities": {meadowrun_identity_id: {}},
        },
        "properties": {
            "hardwareProfile": {
                "vmSize": vm_size,
            },
            "storageProfile": {
                "imageReference": {
                    # will be populated further down
                },
                "osDisk": {
                    "createOption": "FromImage",
                    "deleteOption": "Delete",
                },
            },
            "osProfile": {
                "computerName": f"{vm_name}-vm",
                "adminUsername": _MEADOWRUN_USERNAME,
                "linuxConfiguration": {
                    "disablePasswordAuthentication": True,
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
            "networkProfile": {
                "networkInterfaces": [
                    {"id": nic_id, "properties": {"deleteOption": "Delete"}}
                ]
            },
        },
    }
    if on_demand_or_spot == "spot":
        vm_create_properties["properties"]["priority"] = "Spot"

    if image_type == "Community":
        vm_create_properties["properties"]["storageProfile"]["imageReference"][
            "communityGalleryImageId"
        ] = image_id
    elif image_type == "Regular":
        vm_create_properties["properties"]["storageProfile"]["imageReference"][
            "id"
        ] = image_id
    else:
        raise ValueError(f"Unexpected value for image_type {image_type}")

    vm_result, vm_continuation = await azure_rest_api_poll(
        "PUT",
        f"{resource_group_path}/providers/Microsoft.Compute/virtualMachines/"
        f"{vm_name}-vm",
        "2021-11-01",
        "AsyncOperationJsonStatus",
        json_content=vm_create_properties,
    )

    continuations = [
        continuation
        for continuation in (nic_continuation, ip_address_continuation, vm_continuation)
        if continuation is not None
    ]
    if continuations:
        await asyncio.gather(*continuations)

    # we have to re-get the ip address object to get the current ip address, as it
    # doesn't get assigned until the ip address is attached to a VM and the VM is
    # launched
    ip_address_result2 = await azure_rest_api("GET", ip_address_path, "2021-05-01")
    ip_address = ip_address_result2["properties"]["ipAddress"]

    print(f"Provisioned virtual machine {vm_result['name']} ({ip_address})")

    # TODO the "deleteOption: Delete" takes care of deleting disks, network
    # interfaces, and public IP addresses when the VM is deleted, but we also need
    # to worry about the case where we throw an exception and/or crash after those
    # resources are created but before the VM is created

    return ip_address, vm_result["name"]


async def launch_vms(
    resources_required_per_job: ResourcesInternal,
    num_jobs: int,
    ssh_public_key_data: str,
    location: Optional[str] = None,
) -> Sequence[CloudInstance]:
    """See docstring for launch_ec2_instances"""

    if location is None:
        location = get_default_location()

    chosen_instance_types = choose_instance_types_for_job(
        resources_required_per_job,
        num_jobs,
        await get_vm_types(location),
    )

    vm_detail_tasks = []
    chosen_instance_types_repeated = []

    image_id = f"{_MEADOWRUN_COMMUNITY_IMAGE_ID}/versions/{__version__}"
    # for development, you can uncomment the following, and replace "Community" with
    # "Regular" below
    # image_id = (
    #     "/subscriptions/d740513f-4172-4792-bd29-5194e79d5881/resourceGroups/"
    #     "meadowrun-dev/providers/Microsoft.Compute/galleries/meadowrun.dev.gallery/"
    #     f"images/meadowrun/versions/{_MEADOWRUN_IMAGE_VERSION}"
    # )

    at_least_one_chosen_instance_type = False
    for instance_type in chosen_instance_types:
        at_least_one_chosen_instance_type = True
        for _ in range(instance_type.num_instances):
            vm_detail_tasks.append(
                _provision_vm(
                    location,
                    instance_type.instance_type.name,
                    instance_type.instance_type.on_demand_or_spot,
                    ssh_public_key_data,
                    image_id,
                    "Community",
                )
            )
            chosen_instance_types_repeated.append(instance_type)

    if not at_least_one_chosen_instance_type:
        raise ValueError(
            "There were no instance types that could be selected for "
            f"{resources_required_per_job}"
        )

    vm_details = await asyncio.gather(*vm_detail_tasks)

    return [
        CloudInstance(public_dns_name, vm_name, instance_type)
        for (public_dns_name, vm_name), instance_type in zip(
            vm_details, chosen_instance_types_repeated
        )
    ]
