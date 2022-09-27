"""
Written to work with Kubernetes readiness probes. Returns 0 if there are no jobs running
(i.e. we are ready), 1 if there is a job running (we are not ready for another job)
"""

import sys

import filelock


_JOB_IS_RUNNING_FILE = "/var/meadowrun/job_is_running"


def is_job_running() -> bool:
    try:
        with filelock.FileLock(_JOB_IS_RUNNING_FILE, 0):
            pass
        return False
    except filelock.Timeout:
        return True


def main() -> None:
    if is_job_running():
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
