"""Adapted from https://github.com/pycontribs/tendo/blob/master/tendo/singleton.py"""

import os
import sys

if sys.platform != "win32":
    import fcntl


class ExclusiveFileLockException(BaseException):
    def __init__(self) -> None:
        super().__init__("Another process already holds this file lock")


def exclusive_file_lock(lock_file: str) -> None:
    """
    Gets an exclusive lock on the lock_file which will not be released until this
    process exits. Generally used to prevent more than one instance of a process from
    running on a single machine (as long as they can coordinate on a single lock file).

    This will throw a ExclusiveFileLockException if a different process has already
    acquired the lock on lock_file
    """

    if sys.platform == "win32":
        try:
            # file already exists, we try to remove (in case previous
            # execution was interrupted)
            if os.path.exists(lock_file):
                os.unlink(lock_file)
            os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except OSError as e:
            if e.errno == 13:
                raise ExclusiveFileLockException()
            raise
    else:  # non Windows
        fp = open(lock_file, "w")
        fp.flush()
        try:
            fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            raise ExclusiveFileLockException()
