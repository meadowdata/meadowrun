import time

from meadowgrid import grid_map, ServerAvailableInterpreter
from meadowgrid.agent_creator import choose_instance_types_for_job
from meadowgrid.aws_integration import (
    _get_ec2_instance_types,
    launch_meadowgrid_coordinator,
)
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.resource_allocation import Resources


async def manual_test_aws_coordinator():
    """
    This is a manual test--it creates instances in EC2 that need to be cleaned up
    manually. Also, they
    """
    coordinator_host = await launch_meadowgrid_coordinator("us-east-2")

    def test_function(x: float) -> float:
        time.sleep(x)
        return x

    grid_map(
        test_function,
        [1, 1, 1, 1],
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        memory_gb_required_per_task=1,
        logical_cpu_required_per_task=0.5,
        coordinator_host=coordinator_host,
    )


async def manual_test_get_ec2_instance_types():
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types("us-east-2")
    print(instance_types)

    chosen_instance_types = choose_instance_types_for_job(
        Resources(5, 3, {}), 52, 10, instance_types
    )
    chosen_instance_types.to_clipboard()
    print(chosen_instance_types)
