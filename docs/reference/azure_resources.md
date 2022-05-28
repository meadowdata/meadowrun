# Azure resources created by meadowrun

This page lists all Azure resources created by using Meadowrun or running
`meadowrun-manage-azure-vm install`. All of these resources can be deleted automatically
by running `meadowrun-manage-azure-vm uninstall`.

All Meadowrun-created resources will be in the Meadowrun-rg Resource Group.

* Compute
    * VMs and associated resources (Disks, Network Interfaces, and Public IP Addresses)
    * Virtual network called "Meadowrun-vnet"
* Key Vault
    * Key Vault: mr<last 22 letters/numbers of your subscription id>
    * Secret: meadowrunSshPrivateKey
* Azure Functions
    * Function App: mr<subscription id>
    * Function: vm_adjust. Deletes idle VMs
    * Function: clean_up. Cleans up resources that are no longer neededd.
    * App Service plan: auto-created by Azure when the Function App is created.
* Storage
    * Storage Account: mr<last 22 letters/numbers of your subscription id>
    * Queues: names start with "mrgt" (for Meadowrun Grid Tasks). For communicating task
      requests and results in `run_map`
    * Table: meadowrunVmAlloc. Keeps track of which jobs are running on which VMs.
    * Table: meadowrunLastUsed. Keeps track of when various resources were last used so
      for cleanup
    * Containers: Azure Functions will generate containers
* Container Registries:
    * Container Registry: mr<subscription id>
    * Repository: meadowrun_generated
* Logs
    * Application insights component: meadowrun-mgmt-functions-logs
    * Log Analytics workspace: meadowrun-mgmt-functions-logs-workspace
* IAM
    * Managed Identity: meadowrun-managed-identity
