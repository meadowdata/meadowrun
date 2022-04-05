import pprint

from meadowrun.aws_integration.aws_core import (
    _get_ec2_instance_types,
)
from meadowrun.instance_selection import Resources, choose_instance_types_for_job


async def manual_test_get_ec2_instance_types():
    # This function makes a lot of assumptions about the format of the data we get from
    # various AWS endpoints, good to check that everything works. Look for unexpected
    # warnings!
    instance_types = await _get_ec2_instance_types("us-east-2")
    pprint.pprint(instance_types)

    chosen_instance_types = choose_instance_types_for_job(
        Resources(5, 3, {}), 52, 10, instance_types
    )
    pprint.pprint(chosen_instance_types)

    chosen_instance_types = choose_instance_types_for_job(
        Resources(24000, 1000, {}), 1, 10, instance_types
    )
    assert len(chosen_instance_types) == 0
