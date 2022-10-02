from __future__ import annotations

import abc
import os
import shutil
import sys
import urllib.parse
import zipfile
from typing import TYPE_CHECKING, Tuple, Type

import filelock

if TYPE_CHECKING:
    from types import TracebackType


class ObjectStorage(abc.ABC):
    """An ObjectStorage is a place where you can upload files and download them"""

    async def __aenter__(self) -> ObjectStorage:
        return self

    async def __aexit__(
        self,
        exc_typ: Type[BaseException],
        exc_val: BaseException,
        exc_tb: TracebackType,
    ) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def get_url_scheme(cls) -> str:
        """
        Right now we're using the URL scheme to effectively serialize the ObjectStorage
        object to the job. This works as long as we don't need any additional parameters
        (region name, username/password), but we may need to make this more flexible in
        the future.
        """
        pass

    async def upload_from_file_url(self, file_url: str) -> str:
        """
        file_url will be a file:// url to a file on the local machine. This function
        should upload that file to the object storage, delete the local file, and return
        the URL of the remote file.
        """
        decoded_url = urllib.parse.urlparse(file_url)
        if decoded_url.scheme != "file":
            raise ValueError(f"Expected file URI: {file_url}")
        if sys.platform == "win32" and decoded_url.path.startswith("/"):
            # on Windows, file:///C:\foo turns into file_url.path = /C:\foo so we need
            # to remove the forward slash at the beginning
            file_path = decoded_url.path[1:]
        else:
            file_path = decoded_url.path
        bucket_name, object_name = await self._upload(file_path)
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
        return urllib.parse.urlunparse(
            (self.get_url_scheme(), bucket_name, object_name, "", "", "")
        )

    async def download_and_unzip(
        self, remote_url: str, local_copies_folder: str
    ) -> str:
        """
        remote_url will be the URL of a file in the object storage system as generated
        by upload_from_file_url. This function should download the file and extract it
        to local_copies_folder if it has not already been extracted.
        """
        decoded_url = urllib.parse.urlparse(remote_url)
        bucket_name = decoded_url.netloc
        object_name = decoded_url.path.lstrip("/")
        extracted_folder = os.path.join(
            local_copies_folder, os.path.basename(object_name)
        )

        with filelock.FileLock(f"{extracted_folder}.lock", timeout=120):
            if not os.path.exists(extracted_folder):
                zip_file_path = extracted_folder + ".zip"
                await self._download(bucket_name, object_name, zip_file_path)
                with zipfile.ZipFile(zip_file_path) as zip_file:
                    zip_file.extractall(extracted_folder)

        return extracted_folder

    @abc.abstractmethod
    async def _upload(self, file_path: str) -> Tuple[str, str]:
        """Ensure the file at the given file path is uploaded to storage. Returns where
        the file was uploaded as a tuple of bucket name and object name."""
        pass

    @abc.abstractmethod
    async def _download(
        self, bucket_name: str, object_name: str, file_name: str
    ) -> None:
        """Download the file at given bucket and object name locally to the given file
        name."""
        pass
