"""
Runs a single job locally. Meant to be used on a "server" where the client is calling
e.g. run_function
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional

import meadowrun.run_job_local
from meadowrun.meadowrun_pb2 import ProcessState, Job
from meadowrun.run_job_core import CloudProvider, CloudProviderType


async def main_async(
    job_id: str,
    working_folder: str,
    deallocation: Optional[CloudProviderType],
    deallocation_region_name: Optional[str],
) -> None:
    job_io_prefix = f"{working_folder}/io/{job_id}"

    # write to a temp file and then rename to make deallocate_tasks doesn't see a
    # partial write
    with open(f"{job_io_prefix}.pid_temp", mode="w", encoding="utf-8") as f:
        f.write(str(os.getpid()))
    os.rename(f"{job_io_prefix}.pid_temp", f"{job_io_prefix}.pid")

    with open(f"{job_io_prefix}.job_to_run", mode="rb") as f:
        bytes_job_to_run = f.read()
    job = Job()
    job.ParseFromString(bytes_job_to_run)
    first_state, continuation = await meadowrun.run_job_local.run_local(
        job, working_folder
    )
    with open(f"{job_io_prefix}.initial_process_state", mode="wb") as f:
        f.write(first_state.SerializeToString())

    if (
        first_state.state != ProcessState.ProcessStateEnum.RUNNING
        or continuation is None
    ):
        with open(f"{job_io_prefix}.process_state", mode="wb") as f:
            f.write(first_state.SerializeToString())
    else:
        final_process_state = await continuation
        # if the result is large it's a little sad because we're duplicating it into
        # this .process_state file
        with open(f"{job_io_prefix}.process_state", mode="wb") as f:
            f.write(final_process_state.SerializeToString())

    if deallocation is not None and deallocation_region_name is not None:
        # we want to kick this off and then allow the current process to complete
        # without affecting the child process. This seems to work on Linux but not on
        # Windows (but we don't currently support Windows, so that's okay)
        await asyncio.subprocess.create_subprocess_exec(
            sys.executable,
            os.path.join(os.path.dirname(__file__), "deallocate_jobs.py"),
            "--cloud-provider",
            deallocation,  # e.g. EC2 or Azure
            "--instance-registrar-region-name",
            deallocation_region_name,
            "--working-folder",
            working_folder,
            "--job-id",
            job_id,
        )


def main(
    job_id: str,
    working_folder: str,
    deallocation: Optional[CloudProviderType],
    deallocation_region_name: Optional[str],
) -> None:
    asyncio.run(
        main_async(job_id, working_folder, deallocation, deallocation_region_name)
    )


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--working-folder", required=True)
    parser.add_argument("--deallocation", choices=CloudProvider)
    parser.add_argument("--deallocation-region-name")
    args = parser.parse_args()

    main(
        args.job_id,
        args.working_folder,
        args.deallocation,
        args.deallocation_region_name,
    )


if __name__ == "__main__":
    command_line_main()
