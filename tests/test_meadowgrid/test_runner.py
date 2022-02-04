from typing import Any, Dict

from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.meadowgrid_pb2 import ServerAvailableInterpreter
from meadowgrid.runner import run_function_async, run_function_remote


async def test_run_function_local():
    result = await run_function_async(
        lambda x: x * 2,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        args=[5],
    )
    assert result == 10


def manual_test_run_function_remote():
    # these parameters must be changed!

    # this must be a linux host with meadowgrid installed as per
    # build_meadowgrid_amis.sh
    host = "localhost"
    # whatever parameters are needed to connect to the host via fabric
    fabric_kwargs: Dict[str, Any] = {}

    result = run_function_remote(
        lambda x: x * 2,
        host,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        args=[5],
        fabric_kwargs=fabric_kwargs,
    )

    assert result == 10
