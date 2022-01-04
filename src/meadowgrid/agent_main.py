import argparse
import asyncio
import contextlib
import logging
import multiprocessing
import os
import os.path
from typing import Iterator, Optional, Dict

import meadowgrid.agent
from meadowgrid.config import DEFAULT_COORDINATOR_HOST, DEFAULT_COORDINATOR_PORT


def main(
    working_folder: Optional[str] = None,
    available_resources: Optional[Dict[str, float]] = None,
    coordinator_host: Optional[str] = None,
    coordinator_port: Optional[int] = None,
) -> None:
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

    if available_resources is None:
        available_resources = {}

    asyncio.run(
        meadowgrid.agent.agent_main_loop(
            working_folder,
            available_resources,
            f"{coordinator_host}:{coordinator_port}",
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    working_folder: Optional[str] = None,
    available_resources: Optional[Dict[str, float]] = None,
    coordinator_host: Optional[str] = None,
    coordinator_port: Optional[int] = None,
) -> Iterator[Optional[int]]:
    """
    Launch agent in a child process. Usually for unit tests. For debugging, it's better
    to just run agent_main.py manually as a standalone process so you can debug it, see
    logs, etc. If there's an existing agent already running, the child process will
    just die immediately without doing anything.
    """
    ctx = multiprocessing.get_context("spawn")
    server_process = ctx.Process(
        target=main,
        args=(working_folder, available_resources, coordinator_host, coordinator_port),
    )
    server_process.start()

    try:
        logging.info(f"Process started. Pid: {server_process.pid}")
        yield server_process.pid
    finally:
        server_process.terminate()
        logging.info("Process terminated. Waiting up to 5 seconds for exit...")
        server_process.join(5)
        logging.info(f"Process exited with code {server_process.exitcode}")
        if server_process.is_alive():
            logging.info("Process alive after termination, killing.")
            server_process.kill()


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--working-folder")
    parser.add_argument(
        "--available-resource", action="append", nargs=2, metavar=("name", "value")
    )
    parser.add_argument("--coordinator-host")
    parser.add_argument("--coordinator-port")

    args = parser.parse_args()

    available_resources: Dict[str, float] = {}
    if args.available_resource:
        for name, value in args.available_resource:
            try:
                value = float(value)
            except ValueError:
                raise ValueError(
                    "For --available-resource [name] [value], value must be a float"
                )

            available_resources[name] = value

    main(
        args.working_folder,
        available_resources,
        args.coordinator_host,
        args.coordinator_port,
    )


if __name__ == "__main__":
    command_line_main()
