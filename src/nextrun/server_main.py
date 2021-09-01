"""A runnable script for running a nextrun server"""

import logging
import multiprocessing
import pathlib
import asyncio
import contextlib
from typing import ContextManager

import nextrun.server
from nextrun.config import DEFAULT_HOST, DEFAULT_PORT


def main(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    """A function for running a nextrun server"""

    # TODO read config file and rather than using test defaults
    test_io_folder = str(
        (
            pathlib.Path(__file__).parent.parent.parent / "test_data" / "nextrun"
        ).resolve()
    )
    asyncio.run(nextrun.server.start_nextrun_server(test_io_folder, host, port))


@contextlib.contextmanager
def main_in_child_process(
    host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
) -> ContextManager[None]:
    """
    Launch server in a child process. Usually for unit tests. For debugging, it's better
    to just run server_main.py manually as a standalone process so you can debug it, see
    logs, etc. If there's an existing server already running, the child process will
    just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(target=main, args=(host, port))
    server_process.start()

    try:
        yield None
    finally:
        server_process.kill()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
