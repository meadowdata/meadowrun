"""
The sole purpose of this script is to run until this pod has been idle for at least
_IDLE_TIMEOUT_SECS, and then exit at that point. Actual jobs will be started using
kubectl exec on this pod.

Not dissimilar to deallocate_jobs.py
"""

from __future__ import annotations

import os
import signal
import sys
import time
import traceback
from typing import TYPE_CHECKING, Any, Optional

import filelock

if TYPE_CHECKING:
    from types import FrameType

_LAST_JOB_TIMESTAMP_FILE = "/var/meadowrun/job_timestamp"

_IDLE_TIMEOUT_SECS = 60 * 5


def is_job_running() -> bool:
    try:
        with filelock.FileLock(_JOB_IS_RUNNING_FILE, 0):
            pass
        return False
    except filelock.Timeout:
        return True


_JOB_IS_RUNNING_FILE = "/var/meadowrun/job_is_running"
# this is exactly equivalent to is_job_running but is much faster if that's all we want
# to do because it doesn't require starting a python interpreter
IS_JOB_RUNNING_COMMAND = ["flock", "-w", "0", _JOB_IS_RUNNING_FILE, "-c", "true"]


def _sigchld_handler(signum: int, frame: Optional[FrameType]) -> Any:
    """
    Because of the way docker works, this process will eventually become the parent
    process of every killed process in the container started via exec. We need to handle
    SIGCHLD so that the killed processes don't become zombies.
    """
    try:
        if sys.platform != "win32":
            # waits for a child signal
            while True:
                os.wait()
        print("SIGCHLD handled successfully")
    except BaseException:
        print("Exception handling SIGCHLD:")
        traceback.print_exc()


def main() -> None:
    if sys.platform != "win32":
        signal.signal(signal.SIGCHLD, _sigchld_handler)

    last_activity = time.time()
    while True:
        if time.time() - last_activity > _IDLE_TIMEOUT_SECS:
            print(
                f"Last job was running at {last_activity}, which is more than "
                f"{_IDLE_TIMEOUT_SECS} ago, exiting"
            )
            break

        time.sleep(30)

        try:
            # this checks for jobs that are currently running
            if is_job_running():
                last_activity = time.time()
            else:
                # this checks for jobs that ran and exited quickly that we may not have
                # "noticed" were running
                with open(_LAST_JOB_TIMESTAMP_FILE, "r", encoding="utf-8") as f:
                    last_job_timestamp = float(f.read())
                if last_job_timestamp > last_activity:
                    last_activity = last_job_timestamp
        except BaseException:
            print("Error checking for running jobs: " + traceback.format_exc())


if __name__ == "__main__":
    main()
