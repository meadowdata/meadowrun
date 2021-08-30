"""
This all thematically belongs in jobs.py but we broke it out to avoid circular imports
"""

from dataclasses import dataclass
from typing import Callable, Any, Optional, Iterable, Dict, Literal


@dataclass(frozen=True)
class JobRunSpec:
    """A JobRunSpec indicates how to run the job"""

    fn: Callable[..., Any]
    args: Optional[Iterable[Any]] = None
    kwargs: Optional[Dict[str, Any]] = None


JobState = Literal[
    # Nothing is currently happening with the job
    "WAITING",
    # A run of the job has been requested on a job runner
    "RUN_REQUESTED",
    # The job is currently running. JobPayload.pid will be populated
    "RUNNING",
    # The job has completed normally. JobPayload.result_value and pid will be populated
    "SUCCEEDED",
    # The job was cancelled by the user. JobPayload.pid will be populated
    "CANCELLED",
    # The job failed. JobPayload.raised_exception and pid will be populated
    "FAILED",
]


@dataclass(frozen=True)
class JobPayload:
    """The Event.payload for Job-related events. See JobStateType docstring."""

    request_id: Optional[str]
    state: JobState
    pid: Optional[int] = None
    result_value: Any = None
    raised_exception: Optional[BaseException] = None
