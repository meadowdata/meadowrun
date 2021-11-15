import argparse
import asyncio
import contextlib
import logging
import multiprocessing
import os
import os.path
from typing import Optional, ContextManager

import meadowgrid.job_worker
from meadowgrid.config import DEFAULT_COORDINATOR_HOST, DEFAULT_COORDINATOR_PORT


def main(
    working_folder: Optional[str] = None,
    coordinator_host: Optional[str] = None,
    coordinator_port: Optional[int] = None,
):
    if working_folder is None:
        # figure out the default working_folder based on the OS
        if os.name == "nt":
            working_folder = os.path.join(os.environ["USERPROFILE"], "meadowgrid")
        elif os.name == "posix":
            working_folder = os.path.join(os.environ["HOME"], "meadowgrid")
        else:
            raise ValueError(f"Unexpected os.name {os.name}")
        os.makedirs(working_folder, exist_ok=True)

    if coordinator_host is None:
        coordinator_host = DEFAULT_COORDINATOR_HOST
    if coordinator_port is None:
        coordinator_port = DEFAULT_COORDINATOR_PORT

    asyncio.run(
        meadowgrid.job_worker.job_worker_main_loop(
            working_folder, f"{coordinator_host}:{coordinator_port}"
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    working_folder: Optional[str] = None,
    coordinator_host: Optional[str] = None,
    coordinator_port: Optional[int] = None,
) -> ContextManager[None]:
    """
    Launch worker in a child process. Usually for unit tests. For debugging, it's better
    to just run job_worker_main.py manually as a standalone process so you can debug it,
    see logs, etc. If there's an existing worker already running, the child process will
    just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(
        target=main, args=(working_folder, coordinator_host, coordinator_port)
    )
    server_process.start()

    try:
        yield None
    finally:
        server_process.kill()


def command_line_main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--working-folder")
    parser.add_argument("--coordinator-host")
    parser.add_argument("--coordinator-port")

    args = parser.parse_args()

    main(args.working_folder, args.coordinator_host, args.coordinator_port)


if __name__ == "__main__":
    command_line_main()
