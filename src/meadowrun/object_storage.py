from __future__ import annotations

import abc
import os
import shutil
import sys
import urllib.parse
import zipfile
from typing import TYPE_CHECKING, Type, Optional

import filelock

if TYPE_CHECKING:
    from types import TracebackType


class ObjectStorage(abc.ABC):
    """An ObjectStorage is a place where you can upload files and download them"""

    async def __aenter__(self) -> ObjectStorage:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
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
        file_path = self._file_path_from_url(file_url)
        object_name = await self._upload(file_path)
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
        # the actual bucket name will already exist on the other end, "bucket" is just a
        # placeholder
        return urllib.parse.urlunparse(
            (self.get_url_scheme(), "bucket", object_name, "", "", "")
        )

    def _file_path_from_url(self, file_url: str) -> str:
        decoded_url = urllib.parse.urlparse(file_url)
        if decoded_url.scheme != "file":
            raise ValueError(f"Expected file URI: {file_url}")
        if sys.platform == "win32" and decoded_url.path.startswith("/"):
            # on Windows, file:///C:\foo turns into file_url.path = /C:\foo so we need
            # to remove the forward slash at the beginning
            file_path = decoded_url.path[1:]
        else:
            file_path = decoded_url.path
        return file_path

    async def download_and_unzip(
        self, remote_url: str, local_copies_folder: str
    ) -> str:
        """
        remote_url will be the URL of a file in the object storage system as generated
        by upload_from_file_url. This function should download the file and extract it
        to local_copies_folder if it has not already been extracted.
        """
        decoded_url = urllib.parse.urlparse(remote_url)
        object_name = decoded_url.path.lstrip("/")
        extracted_folder = os.path.join(
            local_copies_folder, os.path.basename(object_name)
        )

        with filelock.FileLock(f"{extracted_folder}.lock", timeout=120):
            if not os.path.exists(extracted_folder):
                zip_file_path = extracted_folder + ".zip"
                await self._download(object_name, zip_file_path)
                with zipfile.ZipFile(zip_file_path) as zip_file:
                    zip_file.extractall(extracted_folder)

        return extracted_folder

    @abc.abstractmethod
    async def _upload(self, file_path: str) -> str:
        """Ensure the file at the given file path is uploaded to storage. Returns where
        the file was uploaded"""
        pass

    @abc.abstractmethod
    async def _download(self, object_name: str, file_name: str) -> None:
        """Download the file locally to the given file name."""
        pass
