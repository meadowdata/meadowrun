"""
This is a command-line interface for agent.run_one_job. This isn't meant to be used
standalone, this is designed to be called by the functions functions in runner.py
"""

import argparse
import asyncio
import logging
from typing import Optional

import meadowgrid.agent
from meadowgrid.meadowgrid_pb2 import JobToRun, ProcessState


async def main_async(job_io_prefix: str, working_folder: Optional[str]) -> None:
    with open(f"{job_io_prefix}.job_to_run", mode="rb") as f:
        bytes_job_to_run = f.read()
    job_to_run = JobToRun()
    job_to_run.ParseFromString(bytes_job_to_run)
    first_state, continuation = await meadowgrid.agent.run_one_job(
        job_to_run, working_folder
    )
    with open(f"{job_io_prefix}.initial_process_state", mode="wb") as f:
        f.write(first_state.SerializeToString())

    if (
        first_state.process_state.state != ProcessState.ProcessStateEnum.RUNNING
        or continuation is None
    ):
        with open(f"{job_io_prefix}.process_state", mode="wb") as f:
            f.write(first_state.SerializeToString())
    else:
        final_process_state = (await continuation).process_state
        # if the result is large it's a little sad because we're duplicating it into
        # this .process_state file
        with open(f"{job_io_prefix}.process_state", mode="wb") as f:
            f.write(final_process_state.SerializeToString())


def main(job_io_prefix: str, working_folder: Optional[str]) -> None:
    asyncio.run(main_async(job_io_prefix, working_folder))


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--job-io-prefix", required=True)
    parser.add_argument("--working-folder")
    args = parser.parse_args()

    main(args.job_io_prefix, args.working_folder)


if __name__ == "__main__":
    command_line_main()
