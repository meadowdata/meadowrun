import argparse
import asyncio
import asyncio.subprocess
import datetime
import logging
from typing import Optional, Dict, Any, ItemsView, Union, Iterable, Tuple

import psutil

from meadowgrid.agent import _get_default_working_folder
from meadowgrid.aws_integration import _get_ec2_metadata
from meadowgrid.ec2_alloc import (
    get_jobs_on_ec2_instance,
    deallocate_job_from_ec2_instance,
    _ALLOCATED_TIME,
)


# If a job is allocated but we never see a pid file for it, we assume after this amount
# of time that the client process crashed
_ALLOCATED_BUT_NOT_RUNNING_TIMEOUT = datetime.timedelta(minutes=7)


def _try_read_pid_file(working_folder: str, job_id: str) -> Optional[int]:
    """
    Tries to get the pid file that run_one_job_main.py will create for a particular
    job_id. If the pid file does not exist or does not contain a valid integer, returns
    None.
    """
    try:
        f = open(f"{working_folder}/io/{job_id}.pid", "r", encoding="utf-8")
    except FileNotFoundError:
        return None

    try:
        pid_str = f.read()
        try:
            return int(pid_str)
        except ValueError:
            print(
                f"Warning, pid file for {job_id} could not be interpreted as an int: "
                f"{pid_str}"
            )
            return None
    finally:
        f.close()


async def async_main(working_folder: Optional[str], job_id: Optional[str]) -> None:
    """
    Deallocates job(s) from the EC2 alloc table. public_address needs to correspond to
    the machine that we're currently running on. If job_id is not specified, we check
    the EC2 alloc table for all jobs currently allocated on this machine. If any of them
    are complete or never ran successfully, we deallocate them. If job_id is specified,
    we only check whether we need to deallocate the specified job.
    """
    public_address = await _get_ec2_metadata("public-hostname")
    if not public_address:
        raise ValueError(
            "Cannot deallocate jobs because we can't get the public address of the "
            "current EC2 instance (maybe we're not running on an EC2 instance?)"
        )

    now = datetime.datetime.utcnow()

    if not working_folder:
        working_folder = _get_default_working_folder()

    if job_id:
        job = (await get_jobs_on_ec2_instance(public_address)).get(job_id)
        if job is None:
            print(
                f"job_id {job_id} was specified, but it does not exist in the EC2 alloc"
                " table"
            )
            return
        jobs: Union[
            Iterable[Tuple[str, Dict[str, Any]]], ItemsView[str, Dict[str, Any]]
        ] = [(job_id, job)]
    else:
        jobs = (await get_jobs_on_ec2_instance(public_address)).items()

    for job_id, job in jobs:
        pid = _try_read_pid_file(working_folder, job_id)
        if pid is not None:
            if not psutil.pid_exists(pid):
                print(
                    f"Deallocating {job_id} as the process {pid} is no longer running"
                )
                await deallocate_job_from_ec2_instance(public_address, job_id, job)
            else:
                print(
                    f"Not deallocating {job_id} as the process {pid} is still running"
                )
        else:
            job_allocated_time = datetime.datetime.fromisoformat(job[_ALLOCATED_TIME])
            if now - job_allocated_time > _ALLOCATED_BUT_NOT_RUNNING_TIMEOUT:
                print(
                    f"Warning: deallocating job that looks like it never ran {job_id} "
                    f"was allocated at {job_allocated_time}"
                )
                await deallocate_job_from_ec2_instance(public_address, job_id, job)
            else:
                print(
                    f"Not deallocating {job_id}: we don't have a pid file but it was "
                    f"allocated recently at {job_allocated_time}"
                )


def main(working_folder: Optional[str], job_id: Optional[str]) -> None:
    asyncio.run(async_main(working_folder, job_id))


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--working-folder")
    parser.add_argument("--job-id")
    args = parser.parse_args()

    main(args.working_folder, args.job_id)


if __name__ == "__main__":
    command_line_main()
