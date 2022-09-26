"""
The sole purpose of this script is to run until this pod has been idle for at least
_IDLE_TIMEOUT_SECS, and then exit at that point. Actual jobs will be started using
kubectl exec on this pod.

Not dissimilar to deallocate_jobs.py
"""
import time

import filelock

_IDLE_TIMEOUT_SECS = 60 * 5
_JOB_IS_RUNNING_FILE = "/var/meadowrun/job_is_running"


def main() -> None:
    last_activity = time.time()
    while True:
        if time.time() - last_activity > _IDLE_TIMEOUT_SECS:
            print(
                f"Last job was running at {last_activity}, which is more than "
                f"{_IDLE_TIMEOUT_SECS} ago, exiting"
            )
            break

        time.sleep(5)

        try:
            with filelock.FileLock(_JOB_IS_RUNNING_FILE, 0):
                pass

            last_activity = time.time()
        except filelock.Timeout:
            pass


if __name__ == "__main__":
    main()
