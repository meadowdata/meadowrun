"""
The sole purpose of this script is to run until this pod has been idle for at least
_IDLE_TIMEOUT_SECS, and then exit at that point. Actual jobs will be started using
kubectl exec on this pod.

Not dissimilar to deallocate_jobs.py
"""
import time
import traceback

from meadowrun.k8s_integration.is_job_running import is_job_running

_LAST_JOB_TIMESTAMP_FILE = "/var/meadowrun/job_timestamp"

_IDLE_TIMEOUT_SECS = 60 * 5


def main() -> None:
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
