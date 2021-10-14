"""A runnable script for running a meadowrun server"""
import argparse
import logging
import multiprocessing
import pathlib
import asyncio
import contextlib
from typing import ContextManager, Optional

import meadowrun.server
from meadowrun.config import DEFAULT_HOST, DEFAULT_PORT


def main(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    meadowflow_address: Optional[str] = None,
) -> None:
    """A function for running a meadowrun server"""

    # TODO read config file and rather than using test defaults
    test_working_folder = str(
        (
            pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowrun"
        ).resolve()
    )
    asyncio.run(
        meadowrun.server.start_meadowrun_server(
            test_working_folder, host, port, meadowflow_address
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    meadowflow_address: Optional[str] = None,
) -> ContextManager[None]:
    """
    Launch server in a child process. Usually for unit tests. For debugging, it's better
    to just run server_main.py manually as a standalone process so you can debug it, see
    logs, etc. If there's an existing server already running, the child process will
    just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(
        target=main, args=(host, port, meadowflow_address)
    )
    server_process.start()

    try:
        yield None
    finally:
        server_process.kill()


def command_line_main():
    logging.basicConfig(level=logging.INFO)

    usage = (
        "If --meadowflow-address [host]:[port] is specified, then this service will "
        "try to register itself with meadowflow at that address"
    )
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--meadowflow-address")

    args = parser.parse_args()

    main(meadowflow_address=args.meadowflow_address)


if __name__ == "__main__":
    command_line_main()
