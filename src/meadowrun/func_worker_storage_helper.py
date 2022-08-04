"""
This code belongs in either func_worker_storage.py. Unfortunately, if we put it in
func_worker_storage.py, then meadowrun.__init__ will import kubernetes_integration which
will import func_worker_storage, which means that running func_worker_storage will
produce annoying messages like RuntimeWarning: 'meadowrun.func_worker_storage' found in
sys.modules after import of package 'meadowrun', but prior to execution of
'meadowrun.func_worker_storage'; this may result in unpredictable behaviour
warn(RuntimeWarning(msg))
"""

from typing import Optional, Any

import boto3


MEADOWRUN_STORAGE_USERNAME = "MEADOWRUN_STORAGE_USERNAME"
MEADOWRUN_STORAGE_PASSWORD = "MEADOWRUN_STORAGE_PASSWORD"

# This is a global variable that will be updated with the storage client if it's
# available in func_worker_storage
STORAGE_CLIENT = None


def get_storage_client_from_args(
    storage_endpoint_url: Optional[str],
    storage_access_key_id: Optional[str],
    storage_secret_access_key: Optional[str],
) -> Any:
    session_kwargs = {}
    if storage_access_key_id is not None:
        session_kwargs["aws_access_key_id"] = storage_access_key_id
    if storage_secret_access_key is not None:
        session_kwargs["aws_secret_access_key"] = storage_secret_access_key
    client_kwargs = {}
    if storage_endpoint_url is not None:
        client_kwargs["endpoint_url"] = storage_endpoint_url
    if session_kwargs:
        session = boto3.Session(**session_kwargs)  # type: ignore
        return session.client("s3", **client_kwargs)  # type: ignore
    else:
        # TODO if all the parameters are None then we're implicitly falling back on AWS
        # S3, which we should make explicit
        return boto3.client("s3", **client_kwargs)  # type: ignore
