"""
This is a miniaturized/custom version of the Azure SDK for python
(https://github.com/Azure/azure-sdk-for-python). This should contain no
meadowrun-specific code.

We do this because the actual SDK requires dozens of packages that are not up to date on
conda-forge and take up ~500MB of disk space. Incidentally, we've also noticed
performance improvements by moving off of the Azure SDK.
"""
