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

import argparse
import asyncio
import logging
import os
import sys
import traceback
from typing import Optional, Tuple

import meadowrun.run_job_local
from meadowrun.meadowrun_pb2 import ProcessState, Job
from meadowrun.run_job_core import CloudProvider, CloudProviderType


async def main_async(
    job_id: str,
    working_folder: str,
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> None:
    job_io_prefix = f"{working_folder}/io/{job_id}"
    try:
        # write to a temp file and then rename to make sure deallocate_tasks doesn't see
        # a partial write
        with open(f"{job_io_prefix}.pid_temp", mode="w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        os.rename(f"{job_io_prefix}.pid_temp", f"{job_io_prefix}.pid")

        with open(f"{job_io_prefix}.job_to_run", mode="rb") as f:
            bytes_job_to_run = f.read()
        job = Job()
        job.ParseFromString(bytes_job_to_run)
        first_state, continuation = await meadowrun.run_job_local.run_local(
            job, working_folder, cloud, True
        )
        with open(f"{job_io_prefix}.initial_process_state", mode="wb") as f:
            f.write(first_state.SerializeToString())

        if (
            first_state.state != ProcessState.ProcessStateEnum.RUNNING
            or continuation is None
        ):
            with open(f"{job_io_prefix}.process_state", mode="wb") as f:
                f.write(first_state.SerializeToString())
        else:
            final_process_state = await continuation
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
                "--working-folder",
                working_folder,
                "--job-id",
                job_id,
            )


def main(
    job_id: str,
    working_folder: str,
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> None:
    try:
        asyncio.run(main_async(job_id, working_folder, cloud))
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("Job was killed by SIGINT")


def command_line_main() -> None:
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--working-folder", required=True)
    parser.add_argument("--cloud", choices=CloudProvider)
    parser.add_argument("--cloud-region-name")
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

    main(args.job_id, args.working_folder, cloud)


if __name__ == "__main__":
    command_line_main()
