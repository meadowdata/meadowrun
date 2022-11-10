from __future__ import annotations

from typing import Tuple


STORAGE_ENV_CACHE_PREFIX = "env_cache/"
STORAGE_CODE_CACHE_PREFIX = "code_cache/"


def storage_key_task_args(job_id: str) -> str:
    return f"inputs/{job_id}.task_args"


def storage_key_ranges(job_id: str) -> str:
    return f"inputs/{job_id}.ranges"


def storage_key_job_to_run(job_id: str) -> str:
    return f"inputs/{job_id}.job_to_run"


def storage_prefix_outputs(job_id: str) -> str:
    return f"outputs/{job_id}/"


def storage_key_task_result(job_id: str, task_id: int, attempt: int) -> str:
    # A million tasks and 1000 attempts should be enough for everybody. Formatting the
    # task is important because when we task download results from S3, we use the
    # StartFrom argument to S3's ListObjects to exclude most tasks we've already
    # downloaded.
    return (
        f"{storage_prefix_outputs(job_id)}{task_id:06d}.{attempt:03d}"
        f"{STORAGE_KEY_TASK_RESULT_SUFFIX}"
    )


STORAGE_KEY_TASK_RESULT_SUFFIX = ".taskresult"


def parse_storage_key_task_result(key: str, results_prefix: str) -> Tuple[int, int]:
    """Returns task_id, attempt based on the task result key"""
    [task_id, attempt, _] = key.replace(results_prefix, "").split(".")
    return int(task_id), int(attempt)


STORAGE_KEY_PROCESS_STATE_SUFFIX = ".process_state"


def storage_key_process_state(job_id: str, worker_index: str) -> str:
    return (
        f"{storage_prefix_outputs(job_id)}{worker_index}"
        f"{STORAGE_KEY_PROCESS_STATE_SUFFIX}"
    )


def parse_storage_key_process_state(key: str, results_prefix: str) -> str:
    return key.replace(results_prefix, "").replace(STORAGE_KEY_PROCESS_STATE_SUFFIX, "")


def construct_job_id(base_job_id: str, worker_suffix: int) -> str:
    """
    For run_map jobs, base_job_id is used in contexts where it doesn't matter which
    worker is involved. E.g. the task arguments or results. job_id (which should be
    renamed to something like worker_id but is hard to rename because of the allocation
    table logic) is used to identify each worker within the overall run_map job. E.g. we
    don't want two workers for the same job on the same machine writing to the same log
    file.
    """
    return f"{base_job_id}-worker{worker_suffix}"
