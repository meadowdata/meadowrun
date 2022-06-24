import asyncio
import os
import platform
from pprint import pprint
from statistics import mean, median, stdev
from time import monotonic

from automated.test_aws_automated import EC2InstanceRegistrarProvider
from meadowrun.run_job import AllocCloudInstance, run_function

# May need to be run with PYTHONPATH=tests to find the automated module.


async def main():
    nb_runs = 10
    irp = EC2InstanceRegistrarProvider()
    times = []
    async with await irp.get_instance_registrar() as instance_registrar:
        for i in range(nb_runs):
            await irp.clear_instance_registrar(instance_registrar)

            def remote_function():
                return os.getpid(), platform.node()

            start_time = monotonic()
            pid1, host1 = await run_function(
                remote_function, AllocCloudInstance(1, 0.5, 15, irp.cloud_provider())
            )
            times.append(monotonic() - start_time)
    print("Times in secs:")
    pprint(times)
    print(f"Min: {min(times):.2f}")
    print(f"Median: {median(times):.2f}")
    print(f"Max: {max(times):.2f}")

    print(f"Average: {mean(times):.2f}")
    print(f"St Dev: {stdev(times):.2f}")


if __name__ == "__main__":

    asyncio.run(main())