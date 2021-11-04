import argparse
import asyncio
import contextlib
import logging
import multiprocessing
import pathlib
from typing import Optional, ContextManager

import meadowrun.job_worker
from meadowrun.config import DEFAULT_COORDINATOR_ADDRESS


def main(coordinator_address: Optional[str] = None):
    # TODO read config file and rather than using test defaults
    test_working_folder = str(
        (
            pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowrun"
        ).resolve()
    )

    if not coordinator_address:
        coordinator_address = DEFAULT_COORDINATOR_ADDRESS

    asyncio.run(
        meadowrun.job_worker.job_worker_main_loop(
            test_working_folder, coordinator_address
        )
    )


@contextlib.contextmanager
def main_in_child_process(
    coordinator_address: Optional[str] = None,
) -> ContextManager[None]:
    """
    Launch worker in a child process. Usually for unit tests. For debugging, it's better
    to just run job_worker_main.py manually as a standalone process so you can debug it,
    see logs, etc. If there's an existing worker already running, the child process will
    just die immediately without doing anything.
    """
    server_process = multiprocessing.Process(target=main, args=(coordinator_address,))
    server_process.start()

    try:
        yield None
    finally:
        server_process.kill()


def command_line_main():
    logging.basicConfig(level=logging.INFO)

    usage = (
        "If --coordinator-address [host]:[port] is not specified, we will connect to "
        "the default address"
    )
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("--coordinator-address")

    args = parser.parse_args()

    main(coordinator_address=args.coordinator_address)


if __name__ == "__main__":
    command_line_main()
