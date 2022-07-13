import os
import tempfile

import asyncssh


@asyncssh.misc.async_context_manager
async def connect(
    host: str, username: str, private_key: asyncssh.SSHKey
) -> asyncssh.SSHClientConnection:
    return await asyncssh.connect(
        host,
        username=username,
        known_hosts=None,
        config=None,
        client_keys=[private_key],
    )


async def run_and_print(
    connection: asyncssh.SSHClientConnection, command: str, check: bool = True
) -> asyncssh.SSHCompletedProcess:
    """
    Runs the command, printing stdout and stderr from the remote process locally.
    result.stdout and result.stderr will be empty.
    """
    async with connection.create_process(command, stderr=asyncssh.STDOUT) as process:
        # TODO should be able to interleave stdout and stderr but this seems somewhat
        # non-trivial:
        # https://stackoverflow.com/questions/55299564/join-multiple-async-generators-in-python
        async for line in process.stdout:
            print(line, end="")

        return await process.wait(check)


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
    # this code can be made simpler if/when https://github.com/ronf/asyncssh/issues/497
    # is addressed
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
            tmp.write(bys)
        await asyncssh.scp(tmp_path, (connection, remote_path))
    finally:
        if tmp_path is not None and os.path.exists(tmp_path):
            os.remove(tmp_path)


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
    # this code can be made simpler if/when https://github.com/ronf/asyncssh/issues/497
    # is addressed
    local_copy_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False) as local_copy:
            local_copy_path = local_copy.name
        await asyncssh.scp((connection, remote_path), local_copy.name)
        with open(local_copy_path, "rb") as local_copy_reopened:
            return local_copy_reopened.read()
    finally:
        if local_copy_path is not None and os.path.exists(local_copy_path):
            os.remove(local_copy_path)


async def read_text_from_file(
    connection: asyncssh.SSHClientConnection, remote_path: str
) -> str:
    return (await read_from_file(connection, remote_path)).decode("utf-8")
