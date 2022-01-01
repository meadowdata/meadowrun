"""A runnable script for running a meadowflow server"""

import asyncio
import contextlib
import logging
import multiprocessing
from typing import Iterator, Optional

import meadowflow.server.server
from meadowflow.scheduler import Scheduler
from meadowflow.server.config import DEFAULT_HOST, DEFAULT_PORT


async def start(host: str, port: int, job_runner_poll_delay_seconds: float) -> None:
    async with Scheduler(job_runner_poll_delay_seconds) as scheduler:
        await meadowflow.server.server.start_meadowflow_server(scheduler, host, port)


def main(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    job_runner_poll_delay_seconds: float = Scheduler._JOB_RUNNER_POLL_DELAY_SECONDS,
) -> None:
    """A function for running a meadowflow server"""

    asyncio.run(start(host, port, job_runner_poll_delay_seconds))


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
    ctx = multiprocessing.get_context("spawn")
    server_process = ctx.Process(
        target=main, args=(host, port, job_runner_poll_delay_seconds)
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
    main()


if __name__ == "__main__":
    command_line_main()
