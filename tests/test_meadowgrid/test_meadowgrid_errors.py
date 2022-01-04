import sys

import pytest

import meadowgrid.agent_main
import meadowgrid.coordinator_main
from meadowgrid.config import MEADOWGRID_INTERPRETER, MEMORY_GB, LOGICAL_CPU
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientAsync
from meadowgrid.grid import grid_map_async
from meadowgrid.meadowgrid_pb2 import (
    ServerAvailableContainer,
    ServerAvailableInterpreter,
)
from test_meadowgrid.test_meadowgrid_basics import (
    TEST_WORKING_FOLDER,
    wait_for_agents_async,
)

_interpreter = ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER)


@pytest.mark.asyncio
async def test_run_request_failed():
    def test_function(x: int) -> int:
        return x * 2

    with meadowgrid.coordinator_main.main_in_child_process():
        with meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER):
            async with MeadowGridCoordinatorClientAsync() as coordinator_client:
                await wait_for_agents_async(coordinator_client, 1)

            tasks = await grid_map_async(
                test_function,
                [0, 1, 2, 3, 4],
                ServerAvailableContainer(image_name="does-not-exist"),
            )
            for i, task in enumerate(tasks):
                with pytest.raises(ValueError, match=".*RUN_REQUEST_FAILED.*"):
                    await task


@pytest.mark.asyncio
async def test_non_zero_return_code():
    def test_function(x: int) -> int:
        if x == 2:
            sys.exit(1)

        return x * 2

    with meadowgrid.coordinator_main.main_in_child_process():
        with meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER):
            async with MeadowGridCoordinatorClientAsync() as coordinator_client:
                await wait_for_agents_async(coordinator_client, 1)

            tasks = await grid_map_async(test_function, [0, 1, 2, 3, 4], _interpreter)
            for i, task in enumerate(tasks):
                if i == 2:
                    with pytest.raises(ValueError, match=".*NON_ZERO_RETURN_CODE.*"):
                        await task
                else:
                    await task


@pytest.mark.asyncio
async def test_not_enough_resources():
    def test_function(x: int) -> int:
        return x * 2

    with meadowgrid.coordinator_main.main_in_child_process():
        async with MeadowGridCoordinatorClientAsync() as coordinator_client:
            assert len(await coordinator_client.get_agent_states()) == 0

            # a small job fails because there are no agents
            tasks = await grid_map_async(
                test_function,
                [1],
                _interpreter,
                resources_required_per_task={MEMORY_GB: 4, LOGICAL_CPU: 2},
            )
            with pytest.raises(ValueError, match=".*RESOURCES_NOT_AVAILABLE.*"):
                # TODO this exception should be more specific
                await tasks[0]

            with meadowgrid.agent_main.main_in_child_process(
                TEST_WORKING_FOLDER, {MEMORY_GB: 20, LOGICAL_CPU: 10}
            ):
                await wait_for_agents_async(coordinator_client, 1)

                # with an agent, a small job succeeds
                tasks = await grid_map_async(
                    test_function,
                    [1],
                    _interpreter,
                    resources_required_per_task={MEMORY_GB: 4, LOGICAL_CPU: 2},
                )
                await tasks[0]

                # but a bigger job fails
                tasks = await grid_map_async(
                    test_function,
                    [1],
                    _interpreter,
                    resources_required_per_task={MEMORY_GB: 24, LOGICAL_CPU: 82},
                )
                with pytest.raises(ValueError, match=".*RESOURCES_NOT_AVAILABLE.*"):
                    await tasks[0]
