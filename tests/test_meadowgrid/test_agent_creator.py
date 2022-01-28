import time

import pytest
import meadowgrid.coordinator_main
from meadowgrid import grid_map, ServerAvailableInterpreter
from meadowgrid.config import MEMORY_GB, LOGICAL_CPU, MEADOWGRID_INTERPRETER


@pytest.mark.asyncio
async def test_local_agent_creator():
    # TODO this test should probably be more extensive, for now we just assume that if
    # grid_map completes without any issues that everything went well.

    def test_function(x: float) -> float:
        time.sleep(x)
        return x

    with meadowgrid.coordinator_main.main_in_child_process(agent_creator="local"):
        grid_map(
            test_function,
            [1, 1, 1, 1],
            ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
            resources_required_per_task={MEMORY_GB: 1, LOGICAL_CPU: 0.5},
        )
