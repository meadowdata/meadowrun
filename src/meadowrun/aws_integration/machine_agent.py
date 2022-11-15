import asyncio
import dataclasses
import json
import os
import signal
import subprocess
import sys
import time
import traceback
from typing import Optional, Dict, Any

import aiobotocore.session

from meadowrun.aws_integration.aws_core import _get_ec2_metadata
from meadowrun.aws_integration.ec2_instance_allocation import (
    EC2InstanceRegistrar,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    MACHINE_AGENT_QUEUE_PREFIX,
    _MEADOWRUN_TAG,
    _MEADOWRUN_TAG_VALUE,
)
from meadowrun.run_job_core import get_log_path
from meadowrun.storage_grid_job import get_aws_s3_bucket
from meadowrun.storage_keys import storage_key_job_to_run


@dataclasses.dataclass
class ProcessWrapper:
    process: Optional[subprocess.Popen]
    killed: bool
    last_updated: float

    def get_status(self) -> str:
        if self.process is None:
            if not self.killed:
                return "unexpected status: process is None and killed = True"
            else:
                return "kill was requested before process was launched"
        else:
            return_code = self.process.poll()
            if return_code is None:
                return f"{self.process.pid} is running"
            else:
                return f"{self.process.pid} exited with return code {return_code}"


@dataclasses.dataclass
class CachedJobObject:
    job_id: str
    last_used: float


async def main() -> None:
    """
    There should be one of these processes running per EC2 instance. This will listen to
    the machine-specific SQS queue for messages that tell it to launch/kill jobs on this
    instance.
    """
    instance_id = await _get_ec2_metadata("instance-id")
    if not instance_id:
        raise ValueError("Cannot get instance-id from IMDS endpoint")
    region_name = await _get_ec2_metadata("placement/region")
    if not region_name:
        raise ValueError("Cannot get placement/region from IMDS endpoint")

    print(f"Running with instance id {instance_id} in {region_name}")

    environment = os.environ.copy()
    # shouldn't be needed if the process is launched correctly
    environment["PYTHONUNBUFFERED"] = "1"

    session = aiobotocore.session.get_session()
    async with session.create_client(
        "sqs", region_name=region_name
    ) as sqs_client, EC2InstanceRegistrar(region_name, "raise") as instance_registrar:
        queue_url = (
            await sqs_client.create_queue(
                QueueName=f"{MACHINE_AGENT_QUEUE_PREFIX}{instance_id}",
                tags={_MEADOWRUN_TAG: _MEADOWRUN_TAG_VALUE},
            )
        )["QueueUrl"]

        print(f"Listening to queue {queue_url}")

        # maps from (job_id_override OR job_id) -> ProcessWrapper
        processes: Dict[str, ProcessWrapper] = {}
        job_objects: Dict[str, CachedJobObject] = {}

        while True:
            try:
                messages_result = await sqs_client.receive_message(
                    QueueUrl=queue_url, WaitTimeSeconds=20, MaxNumberOfMessages=10
                )
                if "Messages" not in messages_result:
                    await _clean_up(processes, job_objects)
                    continue

                messages = messages_result["Messages"]
            except (asyncio.CancelledError, KeyboardInterrupt):
                raise
            except BaseException:
                print("Error receiving messages:\n" + traceback.format_exc())
                continue

            for message in messages:
                message_content = None
                try:
                    message_content = json.loads(message["Body"])
                    action = message_content.get("action", None)

                    if action == "launch":
                        await _launch_jobs(
                            message_content,
                            processes,
                            job_objects,
                            region_name,
                            environment,
                        )
                    elif action == "kill":
                        await _kill_jobs(
                            message_content, instance_id, instance_registrar, processes
                        )
                    else:
                        print(
                            f"Unexpected action {action} in message {message_content}"
                        )
                except (asyncio.CancelledError, KeyboardInterrupt):
                    raise
                except BaseException:
                    print(
                        f"Error acting on message {message_content}:\n"
                        + traceback.format_exc()
                    )
                finally:
                    # TODO it would be nice to retry instead of just giving up after one
                    # failure
                    if message_content is not None and "ReceiptHandle" in message:
                        await sqs_client.delete_message(
                            QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                        )


def _local_job_to_run_file(job_object_id: str) -> str:
    return f"/var/meadowrun/io/{job_object_id}.job_to_run"


async def _launch_jobs(
    message_content: Dict[str, Any],
    processes: Dict[str, ProcessWrapper],
    job_objects: Dict[str, CachedJobObject],
    region_name: str,
    environment: Dict[str, str],
) -> None:
    """Modifies processes in place!"""

    t0 = time.time()

    if "job_object_id" not in message_content or "job_ids" not in message_content:
        print(f"Warning: ignoring incomplete launch message: {message_content}")
        return

    job_object_id = message_content["job_object_id"]
    if job_object_id not in job_objects:
        async with get_aws_s3_bucket(region_name) as storage_bucket:
            await storage_bucket.get_file(
                storage_key_job_to_run(job_object_id),
                _local_job_to_run_file(job_object_id),
            )
        job_objects[job_object_id] = CachedJobObject(job_object_id, t0)
    else:
        job_objects[job_object_id].last_used = t0

    if "public_address" in message_content:
        public_address = f"--public-address {message_content['public_address']}"
    else:
        public_address = ""

    for job_id in message_content["job_ids"]:
        if job_id not in processes:
            if sys.platform != "win32":
                process = subprocess.Popen(
                    "/var/meadowrun/env/bin/python -m meadowrun.run_job_local_main "
                    f"--job-id {job_id} --cloud EC2 --cloud-region-name {region_name} "
                    f"--job-object-id {job_object_id} {public_address} "
                    f"--results-to-storage > {get_log_path(job_id)} 2>&1",
                    shell=True,
                    env=environment,
                    # see https://stackoverflow.com/a/4791612/908704
                    preexec_fn=os.setsid,
                )
                processes[job_id] = ProcessWrapper(process, False, t0)
                print(f"Started job {job_id}")
            else:
                # just for mypy
                raise ValueError("os.setsid not available on Windows")
        else:
            print(
                f"Unexpected: duplicate request to run {job_id}: "
                f"{processes[job_id].get_status()}"
            )


async def _kill_jobs(
    message_content: Dict[str, Any],
    instance_id: str,
    instance_registrar: EC2InstanceRegistrar,
    processes: Dict[str, ProcessWrapper],
) -> None:

    t0 = time.time()

    if "job_ids" not in message_content:
        print(f"Warning: ignoring incomplete kill message {message_content}")
        return

    registered_instance = await instance_registrar.get_registered_instance(instance_id)

    for job_id in message_content["job_ids"]:
        if job_id in processes:
            process_wrapper = processes[job_id]
            process = process_wrapper.process
            if process is not None:
                if process.poll() is None:  # i.e. process is still running
                    if sys.platform != "win32":
                        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                    else:
                        # this is just for mypy
                        raise ValueError(
                            "os.killpg and os.getpgid not implemented on Windows"
                        )

                    if job_id in registered_instance.get_running_jobs():
                        await instance_registrar.deallocate_job_from_instance(
                            registered_instance, job_id
                        )

                    print(f"Killed and deallocated job {job_id}")

                    process_wrapper.last_updated = t0
                    process_wrapper.killed = True
        else:
            processes[job_id] = ProcessWrapper(None, True, t0)
            print(f"Unexpected: requested to kill a job that is not running {job_id}")


async def _clean_up(
    processes: Dict[str, ProcessWrapper], job_objects: Dict[str, CachedJobObject]
) -> None:
    """
    Modifies processes in place, removes the ones that have exited (or never started)
    more than 120 seconds ago.

    Modifies job_objects in place, deletes the file and entry for objects that haven't
    been used in the last 30 minutes.
    """
    t0 = time.time()
    old_job_ids = []
    for job_id, process_wrapper in processes.items():
        # we need to poll even if we don't care about the return code of the child
        # process otherwise the child processes will become zombies
        if (
            process_wrapper.process is None
            or process_wrapper.process.poll() is not None  # i.e. process has exited
        ) and process_wrapper.last_updated < t0 - 120:
            old_job_ids.append(job_id)
    for job_id in old_job_ids:
        del processes[job_id]

    old_job_object_ids = []
    for job_object_id, job_object in job_objects.items():
        if job_object.last_used < t0 - 60 * 30:
            try:
                os.remove(_local_job_to_run_file(job_object_id))
                old_job_object_ids.append(job_object_id)
            except asyncio.CancelledError:
                raise
            except BaseException:
                print(
                    f"Warning, error deleting {_local_job_to_run_file(job_object_id)}:"
                    "\n" + traceback.format_exc()
                )
    for job_object_id in old_job_object_ids:
        del job_objects[job_object_id]


if __name__ == "__main__":
    asyncio.run(main())
