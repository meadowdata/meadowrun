import sys
import asyncssh
import tempfile


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
    # print(f"Running: {command}")
    result = await connection.run(command, check=check)
    print(result.stdout, end="")
    print(result.stderr, file=sys.stderr, end="")
    return result


async def write_to_file(
    connection: asyncssh.SSHClientConnection, bys: bytes, remote_path: str
) -> None:
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(bys)
        tmp.flush()
        await asyncssh.scp(tmp.name, (connection, remote_path))


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
    with tempfile.NamedTemporaryFile() as local_copy:
        await asyncssh.scp((connection, remote_path), local_copy.name)
        local_copy.seek(0)
        return local_copy.read()


async def read_text_from_file(
    connection: asyncssh.SSHClientConnection, remote_path: str
) -> str:
    return (await read_from_file(connection, remote_path)).decode("utf-8")
