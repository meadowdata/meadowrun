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
