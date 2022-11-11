"""
Runs a single job locally. Meant to be used on a "server" where the client is calling
e.g. run_function


A long note about interrupting running jobs and deallocation, ssh, asyncio, signal
handlers, atexit handlers and cleanup.

When a job is running on a machine, via this script, the client may get interrupted or
otherwise exit. In that case, we'd like this script to exit as soon as possible, as it's
now using resources unnecessarily. Additionally, just exiting the process is not enough
- the job must also be deallocated: the table that has an overview of all running jobs
needs to be updated.

This script is always started via an ssh command, see run_job_core.py. There are now
roughly three possibilities:

1. The job runs to completion normally. In this case this script starts a process to
   deallocate, without waiting for it to finish. This way, users don't have to wait for
   deallocation but it happens asynchronously from their perspective.

2. The user interrupts the job from the client side. In this case we propagate the
   interrupt signal via ssh to server process, which reacts to it by cancelling whatever
   it is currently doing and finally starting the deallocation process.

3. The user kills the job from the client side. The difference with 2 is that in this
   case the client doesn't get an opportunity to send the interrupt signal to the server
   (this script). In this case sshd will eventually realize there's no ssh client
   connected to the session, and send SIGHUP to the shell session, which will eventually
   terminate the server process. To deal with such unexpected ends, each machine runs a
   cron job that deallocates processes that are no longer running perdiodically.

Some details about point 2:

- To be able to propagate the interruption from client to server, we obviously need to
  be able to react to a keyboard interrupt or cancellation.

- Interrupting an asyncio process is trickier than interrupting a non-asyncio process,
  because in the single-threaded, synchronous case, Python raises a
  KeyboardInterruptError which you can react to in an exception handler. In the asyncio
  case this error is still raised, but that doesn't necessarily mean that any suspended
  tasks get a chance to run. The program just exits with a bunch of unfinished tasks on
  the event loop. Since we use asyncssh, see ssh.py, we connect to remote hosts in an
  async coroutine. On a KeyboardInterrupt, we need those tasks to run so the
  asyncssh.Process can be sent a SIGINT in turn. Luckily, if you run your event loop
  through asyncio.run, this takes care of cancelling outstanding tasks and waiting for
  them automatically, so we can react to CancelledError and propagate the SIGINT. See
  ssh.py::run_and_print.

- Also, the asyncio coroutines should not swallow CancelledError (as some used to in
  run_job_local.py), as that means any coroutines that contain them do not get
  cancelled.

- On the server side, the SIGINT again gets interpreted as a KeyboardInterruptError, so
  the same mechanism via asyncio.run applies: all outstanding tasks get cancelled and
  get a chance to run. We take advantage of this mechanism below by starting the
  deallocate process in a finally block from the main task.

- Note that none of this works if either client or server gets SIGTERM (terminate) or
  SIGKILL (kill -9) signals - tasks will not be cleanly cancelled and run to completion,
  and the deallocate process will not run.

Some details about point 3:

- If a client abruptly disappears, it'd be better for the process to stop running and
  get deallocated as soon as possible. This will indeed eventually happen, but is
  unlikely to happen straight away for typical distributed systems reasons (i.e. you
  don't want processes exiting because of a temporary drop in connectivity). Here's a
  good link about the process:
  https://serverfault.com/questions/463366/does-getting-disconnected-from-an-ssh-session-kill-your-programs
  Via testing I found that outputting something to stdout is the sure way to get a
  "orphaned" ssh process to exit. This is not implemented, but we could print an
  invisible character (or a spinning ascii wheel) periodically as a poor man's
  keepalive. In testing, I've not been able to get sshd's options for keepalive checking
  to work:
  https://unix.stackexchange.com/questions/3026/what-do-options-serveraliveinterval-and-clientaliveinterval-in-sshd-config-d
  That said, it's entirely possible I made some basic mistake.

"""

from __future__ import annotations

import argparse
import asyncio
import functools
import logging
import os
import sys
import traceback
from typing import Optional, Tuple, TYPE_CHECKING, Union

import meadowrun.func_worker_storage_helper
import meadowrun.run_job_local
from meadowrun.azure_integration.blob_storage import get_azure_blob_container
from meadowrun.meadowrun_pb2 import ProcessState, Job
from meadowrun.run_job_core import CloudProvider, CloudProviderType, get_log_path
from meadowrun.storage_grid_job import get_aws_s3_bucket
from meadowrun.storage_keys import job_id_to_storage_key_process_state

if TYPE_CHECKING:
    from meadowrun.deployment_manager import StorageBucketFactoryType
    from meadowrun.abstract_storage_bucket import AbstractStorageBucket


async def main_async(
    job_id: str,
    job_object_id: Optional[str],
    public_address: Optional[str],
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> None:
    job_io_prefix = f"/var/meadowrun/io/{job_id}"
    if job_object_id:
        job_object_path = f"/var/meadowrun/io/{job_object_id}.job_to_run"
    else:
        job_object_path = f"{job_io_prefix}.job_to_run"

    storage_bucket: Optional[AbstractStorageBucket] = None
    storage_bucket_factory: Union[
        StorageBucketFactoryType, AbstractStorageBucket, None
    ] = None

    try:
        # write to a temp file and then rename to make sure deallocate_tasks doesn't see
        # a partial write
        with open(f"{job_io_prefix}.pid_temp", mode="w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        os.rename(f"{job_io_prefix}.pid_temp", f"{job_io_prefix}.pid")

        # get the job
        with open(job_object_path, mode="rb") as f:
            bytes_job_to_run = f.read()
        job = Job()
        job.ParseFromString(bytes_job_to_run)

        # set storage_bucket, storage_bucket_factory
        if cloud is not None:
            if cloud[0] == "EC2":
                # with EC2, we need the storage bucket to download the job object
                storage_bucket = await get_aws_s3_bucket(cloud[1]).__aenter__()
                storage_bucket_factory = storage_bucket
            elif cloud[0] == "AzureVM":
                # with Azure, the storage bucket is optional, so storage_bucket_factory
                # is a function rather than an actual storage_bucket
                storage_bucket_factory = functools.partial(
                    get_azure_blob_container, cloud[1]
                )
            else:
                print(f"Warning: unknown value for cloud {cloud}")
                storage_bucket_factory = None

        if public_address:
            # this is a little silly as we could just use the IMDS endpoints to figure
            # this out for ourselves. It shouldn't even really be necessary because the
            # caller should have this information already, but it's convenient to be
            # able to return a ProcessState with a populated log_file_name that has
            # everything in it you would want to know.
            log_file_name = f"{public_address}:{get_log_path(job_id)}"
        else:
            log_file_name = get_log_path(job_id)

        first_state, continuation = await meadowrun.run_job_local.run_local(
            job,
            job_id,
            log_file_name,
            cloud,
            storage_bucket_factory,
            compile_environment_in_container=True,
        )

        if (
            first_state.state != ProcessState.ProcessStateEnum.RUNNING
            or continuation is None
        ):
            final_process_state = first_state
        else:
            final_process_state = await continuation

        if storage_bucket is not None:
            await storage_bucket.write_bytes(
                final_process_state.SerializeToString(),
                job_id_to_storage_key_process_state(job_id),
            )
        else:
            # if the result is large it's a little sad because we're duplicating it into
            # this .process_state file
            with open(f"{job_io_prefix}.process_state", mode="wb") as f:
                f.write(final_process_state.SerializeToString())
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Job was killed by SIGINT")
    except:  # noqa: E722
        # so we know what's wrong
        traceback.print_exc()
        raise
    finally:
        if storage_bucket is not None:
            await storage_bucket.__aexit__(None, None, None)
        if cloud is not None:
            # we want to kick this off and then allow the current process to complete
            # without affecting the child process. This seems to work on Linux but not
            # on Windows (but we don't currently support Windows, so that's okay)
            await asyncio.subprocess.create_subprocess_exec(
                sys.executable,
                os.path.join(os.path.dirname(__file__), "deallocate_jobs.py"),
                "--cloud",
                cloud[0],  # e.g. EC2 or Azure
                "--cloud-region-name",
                cloud[1],
                "--job-id",
                job_id,
            )


def main(
    job_id: str,
    job_object_id: Optional[str],
    public_address: Optional[str],
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> None:
    try:
        asyncio.run(main_async(job_id, job_object_id, public_address, cloud))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Job was killed by SIGINT")


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--public-address")
    parser.add_argument("--cloud", choices=CloudProvider)
    parser.add_argument("--cloud-region-name")
    parser.add_argument("--job-object-id")
    args = parser.parse_args()

    if bool(args.cloud is None) ^ bool(args.cloud_region_name is None):
        raise ValueError(
            "--cloud and --cloud-region-name must both be provided or both not be "
            "provided"
        )

    if args.cloud is None:
        cloud = None
    else:
        cloud = args.cloud, args.cloud_region_name

    main(args.job_id, args.job_object_id, args.public_address, cloud)


if __name__ == "__main__":
    command_line_main()
