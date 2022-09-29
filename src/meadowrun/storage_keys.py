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
