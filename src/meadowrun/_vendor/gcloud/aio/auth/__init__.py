"""
Copied from https://github.com/talkiq/gcloud-aio/tree/auth-4.0.1/auth/gcloud/aio/auth

Applied https://github.com/talkiq/gcloud-aio/pull/518/
"""

__version__ = "4.0.1"

from meadowrun._vendor.gcloud.aio.auth.iam import IamClient
from meadowrun._vendor.gcloud.aio.auth.session import AioSession
from meadowrun._vendor.gcloud.aio.auth.token import Token
from meadowrun._vendor.gcloud.aio.auth.utils import decode
from meadowrun._vendor.gcloud.aio.auth.utils import encode
from meadowrun._vendor.gcloud.aio.auth.build_constants import BUILD_GCLOUD_REST


__all__ = [
    "__version__",
    "IamClient",
    "Token",
    "decode",
    "encode",
    "AioSession",
    "BUILD_GCLOUD_REST",
]
