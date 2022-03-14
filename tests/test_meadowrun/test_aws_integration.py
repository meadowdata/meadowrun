from meadowrun.agent_creator import choose_instance_types_for_job
from meadowrun.aws_integration import (
    _get_ec2_instance_types,
)
from meadowrun.resource_allocation import Resources


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
