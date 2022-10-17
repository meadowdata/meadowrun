# Uninstall Meadowrun

If you decide to stop using Meadowrun, you can remove all meadowrun-related resources
easily:

=== "AWS"
    ```
    > meadowrun-manage-ec2 uninstall
    Deleting all meadowrun resources
    Deleted all meadowrun resources in 6.61 seconds
    ```
=== "Azure"
    ```
    > meadowrun-manage-azure-vm uninstall
    Deleting all meadowrun resources
    Deleted all meadowrun resources in 6.61 seconds
    ```
=== "Kubernetes"
    Kubernetes does not require an explicit uninstall.
