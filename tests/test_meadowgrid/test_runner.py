from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.meadowgrid_pb2 import ServerAvailableInterpreter
from meadowgrid.runner import (
    run_command,
    run_function,
    SshHost,
    Deployment,
    LocalHost,
)


async def test_run_function_local():
    result = await run_function(
        lambda x: x * 2,
        LocalHost(),
        Deployment(ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER)),
        args=[5],
    )

    assert result == 10


# these parameters must be changed!

# this must be a linux host with meadowgrid installed as per build_meadowgrid_amis.sh
_REMOTE_HOST = SshHost("localhost", {})


async def manual_test_run_function_remote():
    result = await run_function(
        lambda x: x * 2,
        _REMOTE_HOST,
        args=[5],
    )

    assert result == 10


async def manual_test_run_command_remote():
    await run_command(
        "pip --version",
        _REMOTE_HOST,
    )
    # right now we don't get the stdout back, so the only way to check this is to look
    # at the log file on the remote host
