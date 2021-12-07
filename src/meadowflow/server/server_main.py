"""A runnable script for running a meadowflow server"""

import itertools
import logging
import multiprocessing
import asyncio
import contextlib
from typing import Iterator, Optional

import meadowflow.server.server
from meadowflow.scheduler import Scheduler
from meadowflow.server.config import DEFAULT_HOST, DEFAULT_PORT


def main(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    job_runner_poll_delay_seconds: float = Scheduler._JOB_RUNNER_POLL_DELAY_SECONDS,
) -> None:
    """A function for running a meadowflow server"""

    event_loop = asyncio.new_event_loop()
    scheduler = Scheduler(event_loop, job_runner_poll_delay_seconds)
    event_loop.run_until_complete(
        asyncio.wait(
            itertools.chain(
                scheduler.get_main_loop_tasks(),
                [
                    meadowflow.server.server.start_meadowflow_server(
                        scheduler, host, port
                    )
                ],
            )
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    job_runner_poll_delay_seconds: float = Scheduler._JOB_RUNNER_POLL_DELAY_SECONDS,
) -> Iterator[Optional[int]]:
    """
    Launch server in a child process. Usually for unit tests. For debugging, it's better
    to just run server_main.py manually as a standalone process so you can debug it, see
    logs, etc. If there's an existing server already running, the child process will
    just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(
        target=main, args=(host, port, job_runner_poll_delay_seconds)
    )
    server_process.start()

    try:
        yield server_process.pid
    finally:
        server_process.kill()


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)
    main()


if __name__ == "__main__":
    command_line_main()
