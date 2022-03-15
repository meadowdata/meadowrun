import sys

import pytest

from meadowrun import run_function, LocalHost
from meadowrun.meadowrun_pb2 import ServerAvailableContainer, ProcessState
from meadowrun.run_job import Deployment, MeadowrunException


@pytest.mark.asyncio
async def test_run_request_failed():
    with pytest.raises(MeadowrunException) as exc_info:
        await run_function(
            lambda: "hello",
            LocalHost(),
            Deployment(ServerAvailableContainer(image_name="does-not-exist")),
        )

    assert (
        exc_info.value.process_state.state
        == ProcessState.ProcessStateEnum.RUN_REQUEST_FAILED
    )


@pytest.mark.asyncio
async def test_non_zero_return_code():
    def exit_immediately():
        sys.exit(101)

    with pytest.raises(MeadowrunException) as exc_info:
        await run_function(exit_immediately, LocalHost())

    assert (
        exc_info.value.process_state.state
        == ProcessState.ProcessStateEnum.NON_ZERO_RETURN_CODE
    )
    assert exc_info.value.process_state.return_code == 101
