"""
This is a command-line interface for agent.run_one_job. This isn't meant to be used
standalone, this is designed to be called by the functions functions in runner.py
"""

import argparse
import asyncio
import logging
from typing import Optional

import meadowgrid.agent
from meadowgrid.meadowgrid_pb2 import JobToRun


def main(serialized_job_to_run_path: str, working_folder: Optional[str]) -> None:
    with open(serialized_job_to_run_path, mode="rb") as f:
        bytes_job_to_run = f.read()
    job_to_run = JobToRun()
    job_to_run.ParseFromString(bytes_job_to_run)
    asyncio.run(meadowgrid.agent.run_one_job(job_to_run, working_folder))


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--serialized-job-to-run-path", required=True)
    parser.add_argument("--working-folder")
    args = parser.parse_args()

    main(args.serialized_job_to_run_path, args.working_folder)


if __name__ == "__main__":
    command_line_main()
