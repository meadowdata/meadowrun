from __future__ import annotations

import asyncio
from typing import (
    TYPE_CHECKING,
)

import asyncssh

if TYPE_CHECKING:
    from asyncssh import SSHClientConnection, SSHKey


@asyncssh.misc.async_context_manager
async def connect(host: str, username: str, private_key: SSHKey) -> SSHClientConnection:
    return await asyncssh.connect(
        host=host,
        username=username,
        known_hosts=None,
        config=None,
        client_keys=[private_key],
        # keepalive necessary otherwise client silently loses connection on long-running
        # jobs. The symptom is that e.g. run_and_print just hangs indefinitely.
        keepalive_interval=60,
    )


async def run_and_print(
    connection: asyncssh.SSHClientConnection, command: str, check: bool = True
) -> asyncssh.SSHCompletedProcess:
    """
    Runs the command, printing stdout and stderr from the remote process locally.
    result.stdout and result.stderr will be empty.
    Interrupts the remote process when the task is cancelled, by sending SIGINT signal.
    """
    # async with connection.create_process(command, stderr=asyncssh.STDOUT) as process:
    # doesn't work as well because that doesn't actively terminate the remote process.
    # In testing, calling terminate explicitly makes the remote process exit more
    # quickly.
    try:
        process = await connection.create_process(command, stderr=asyncssh.STDOUT)
        # TODO should be able to interleave stdout and stderr but this seems somewhat
        # non-trivial:
        # https://stackoverflow.com/questions/55299564/join-multiple-async-generators-in-python
        async for line in process.stdout:
            print(line, end="")

        return await process.wait(check)
    except (KeyboardInterrupt, asyncio.CancelledError):
        process.send_signal("INT")
        async for line in process.stdout:
            print(line, end="")
        # escalation: sends TERM and KILL respectively
        # process.terminate()
        # process.kill()
        # however those won't allow the remote process to react - asyncio.run in
        # particular makes sure on SIGINT to cancel and finish all tasks.
        raise
    finally:
        # what process.__aexit__ does:
        process.close()
        await process._chan.wait_closed()


async def run_and_capture(
    connection: asyncssh.SSHClientConnection, command: str, check: bool = True
) -> asyncssh.SSHCompletedProcess:
    """
    Runs the command, does not print any stdout/stderr. The output is also available as
    result.stdout and result.stderr.
    """
    return await connection.run(command, check=check)


async def write_to_file(
    connection: asyncssh.SSHClientConnection, bys: bytes, remote_path: str
) -> None:
    async with connection.start_sftp_client() as sftp:
        async with sftp.open(remote_path, "wb") as remote_file:
            await remote_file.write(bys)


async def write_text_to_file(
    connection: asyncssh.SSHClientConnection, text: str, remote_path: str
) -> None:
    await write_to_file(connection, text.encode("utf-8"), remote_path)


async def upload_file(
    connection: asyncssh.SSHClientConnection, local_path: str, remote_path: str
) -> None:
    await asyncssh.scp(
        local_path,
        (connection, remote_path),
    )


async def read_from_file(
    connection: asyncssh.SSHClientConnection, remote_path: str
) -> bytes:
    async with connection.start_sftp_client() as sftp:
        async with sftp.open(remote_path, "rb") as remote_file:
            return await remote_file.read()


async def read_text_from_file(
    connection: asyncssh.SSHClientConnection, remote_path: str
) -> str:
    return (await read_from_file(connection, remote_path)).decode("utf-8")
