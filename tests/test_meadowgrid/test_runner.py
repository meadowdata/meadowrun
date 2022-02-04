from typing import Any, Dict

from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.meadowgrid_pb2 import ServerAvailableInterpreter
from meadowgrid.runner import (
    run_command_remote,
    run_function_async,
    run_function_remote,
)


async def test_run_function_local():
    result = await run_function_async(
        lambda x: x * 2,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        args=[5],
    )
    assert result == 10


# these parameters must be changed!

# this must be a linux host with meadowgrid installed as per
# build_meadowgrid_amis.sh
_HOST = "localhost"
# whatever parameters are needed to connect to the host via fabric
_FABRIC_KWARGS: Dict[str, Any] = {}


def manual_test_run_function_remote():

    result = run_function_remote(
        lambda x: x * 2,
        _HOST,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        args=[5],
        fabric_kwargs=_FABRIC_KWARGS,
    )

    assert result == 10


def manual_test_run_command_remote():
    run_command_remote(
        "pip --version",
        _HOST,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        fabric_kwargs=_FABRIC_KWARGS,
    )
    # right now we don't get the stdout back, so the only way to check this is to look
    # at the log file on the remote host
