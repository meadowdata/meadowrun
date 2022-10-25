"""
This is very similar to run_job_local_main.py but instead of communicating with the
client via the local file system, it communicates via an S3 compatible storage bucket.
"""

import argparse
import asyncio
import logging
import os
import time
import traceback

import filelock

import meadowrun.run_job_local
from meadowrun.k8s_integration.storage_spec import (
    add_storage_spec_arguments_to_parser,
    storage_bucket_from_parsed_args,
)
from meadowrun.storage_keys import storage_key_job_to_run, storage_key_process_state
from meadowrun.meadowrun_pb2 import ProcessState, Job
from meadowrun.k8s_integration.k8s_main import (
    _JOB_IS_RUNNING_FILE,
    _LAST_JOB_TIMESTAMP_FILE,
)


async def main() -> None:
    with filelock.FileLock(_JOB_IS_RUNNING_FILE, 0):
        with open(_LAST_JOB_TIMESTAMP_FILE, "w", encoding="utf-8") as f:
            f.write(str(time.time()))

        logging.basicConfig(level=logging.INFO)

        # parse arguments

        parser = argparse.ArgumentParser()

        # arguments for finding the storage bucket (i.e. S3-compatible store) where we
        # can find inputs and put outputs
        parser.add_argument("--job-id", required=True)
        add_storage_spec_arguments_to_parser(parser)

        args = parser.parse_args()

        job_id: str = args.job_id
        suffix = os.environ.get(
            "MEADOWRUN_WORKER_INDEX", os.environ["JOB_COMPLETION_INDEX"]
        )

        # prepare storage bucket, filenames and pickle protocol for the result
        storage_bucket = await storage_bucket_from_parsed_args(args)
        async with storage_bucket:
            meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET = (
                storage_bucket
            )

            # run the job
            try:
                job = Job.FromString(
                    await storage_bucket.get_bytes(storage_key_job_to_run(job_id))
                )
                # hypothetically we should make sure MEADOWRUN_STORAGE_USERNAME and
                # MEADOWRUN_STORAGE_PASSWORD should get added to the Job object because
                # if we are running _indexed_map_worker we will need those variables.
                # However, this only happens in the non-container job case, in which
                # case the environment variables should just get inherited by the child
                # process that we start.
                first_state, continuation = await meadowrun.run_job_local.run_local(
                    job, None, storage_bucket, compile_environment_in_container=False
                )
                # TODO to be analogous to run_job_local_main.py we should write
                # f"{storage_file_prefix}.initial_process_state to the storage bucket
                # here

                if (
                    first_state.state != ProcessState.ProcessStateEnum.RUNNING
                    or continuation is None
                ):
                    await storage_bucket.write_bytes(
                        first_state.SerializeToString(),
                        storage_key_process_state(job_id, suffix),
                    )
                else:
                    final_process_state = await continuation

                    await storage_bucket.write_bytes(
                        final_process_state.SerializeToString(),
                        storage_key_process_state(job_id, suffix),
                    )
            except:  # noqa: E722
                # so we know what's wrong
                traceback.print_exc()
                raise


if __name__ == "__main__":
    asyncio.run(main())
