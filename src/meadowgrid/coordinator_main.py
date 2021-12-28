"""A runnable script for running a meadowgrid server"""
import argparse
import logging
import multiprocessing
import asyncio
import contextlib
from typing import ContextManager, Optional

import meadowgrid.coordinator
from meadowgrid.config import DEFAULT_COORDINATOR_HOST, DEFAULT_COORDINATOR_PORT


def main(
    host: Optional[str] = None,
    port: Optional[int] = None,
    meadowflow_address: Optional[str] = None,
) -> None:
    """A function for running a meadowgrid coordinator"""

    if not host:
        host = DEFAULT_COORDINATOR_HOST
    if not port:
        port = DEFAULT_COORDINATOR_PORT

    asyncio.run(
        meadowgrid.coordinator.start_meadowgrid_coordinator(
            host, port, meadowflow_address
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    host: Optional[str] = None,
    port: Optional[int] = None,
    meadowflow_address: Optional[str] = None,
) -> ContextManager[None]:
    """
    Launch server in a child process. Usually for unit tests. For debugging, it's better
    to just run coordinator_main.py manually as a standalone process so you can debug
    it, see logs, etc. If there's an existing server already running, the child process
    will just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(
        target=main, args=(host, port, meadowflow_address)
    )
    server_process.start()

    try:
        yield server_process.pid
        server_process.terminate()
    finally:
        server_process.kill()


def command_line_main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--meadowflow-address")

    args = parser.parse_args()

    main(args.host, args.port, args.meadowflow_address)


if __name__ == "__main__":
    command_line_main()
