import asyncio
import os
import platform
from pprint import pprint
from statistics import median
import sys
from time import monotonic
from typing import Tuple

from meadowrun import Resources
from meadowrun.run_job import AllocCloudInstance, run_function


async def main() -> None:

    nb_runs = 10

    # hack to find the automated module.
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from automated.test_aws_automated import EC2InstanceRegistrarProvider

    def remote_function() -> Tuple[int, str]:
        return os.getpid(), platform.node()

    irp = EC2InstanceRegistrarProvider()
    times = []
    async with await irp.get_instance_registrar() as instance_registrar:
        for i in range(nb_runs):
            await irp.clear_instance_registrar(instance_registrar)

            start_time = monotonic()
            pid1, host1 = await run_function(
                remote_function,
                AllocCloudInstance(irp.cloud_provider()),
                Resources(1, 0.5, 80),
            )
            times.append(monotonic() - start_time)
    print("Times in secs:")
    pprint(times)
    print(f"Min: {min(times):.2f}")
    print(f"Median: {median(times):.2f}")
    print(f"Max: {max(times):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
