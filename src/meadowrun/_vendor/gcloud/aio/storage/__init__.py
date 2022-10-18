"""
Copied from https://github.com/talkiq/gcloud-aio/tree/storage-7.0.1/storage/gcloud/aio/storage
"""

__version__ = "7.0.1"

from meadowrun._vendor.gcloud.aio.storage.blob import Blob
from meadowrun._vendor.gcloud.aio.storage.bucket import Bucket
from meadowrun._vendor.gcloud.aio.storage.storage import SCOPES
from meadowrun._vendor.gcloud.aio.storage.storage import Storage
from meadowrun._vendor.gcloud.aio.storage.storage import StreamResponse


__all__ = ["__version__", "Blob", "Bucket", "SCOPES", "Storage", "StreamResponse"]
