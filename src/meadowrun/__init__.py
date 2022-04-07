import asyncio
import sys

# The default event loop type was changed from the selector event loop to the proactor
# event loop in python 3.8. To make this work on python 3.7, we need to use proactor
# event loops--aiohttp and asyncio.subprocess don't seem to with the selector event loop
if sys.platform == "win32":
    if sys.version_info < (3, 8):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


from meadowrun.run_job import (
    Deployment,
    EC2AllocHost,
    EC2AllocHosts,
    LocalHost,
    SshHost,
    run_command,
    run_function,
    run_map,
)

from meadowrun.meadowrun_pb2 import (
    AwsSecret,
    ContainerAtDigest,
    ContainerAtTag,
    GitRepoBranch,
    GitRepoCommit,
    ServerAvailableContainer,
    ServerAvailableFile,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)
