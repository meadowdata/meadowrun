"""
This all thematically belongs in jobs.py but we broke it out to avoid circular imports
"""

import abc
from dataclasses import dataclass
from typing import Any, Optional, Iterable, Union, Literal

from nextbeat.event_log import Event
from nextrun.job_run_spec import JobRunSpec


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
    # The job failed. JobPayload.failure_type and pid will be populated. If failure_type
    # is PYTHON_EXCEPTION, raised_exception will be populated, if failure_type is
    # NON_ZERO_RETURN_CODE, return_code will be populated.
    "FAILED",
]


@dataclass(frozen=True)
class RaisedException:
    """Represents a python exception raised by a remote process"""

    exception_type: str
    exception_message: str
    exception_traceback: str


@dataclass(frozen=True)
class JobPayload:
    """The Event.payload for Job-related events. See JobStateType docstring."""

    request_id: Optional[str]
    state: JobState
    failure_type: Optional[Literal["PYTHON_EXCEPTION", "NON_ZERO_RETURN_CODE"]] = None
    pid: Optional[int] = None
    result_value: Any = None
    raised_exception: Union[RaisedException, BaseException, None] = None
    return_code: Optional[int] = None


class JobRunner(abc.ABC):
    """An interface for job runner clients"""

    @abc.abstractmethod
    async def run(
        self, job_name: str, run_request_id: str, job_run_spec: JobRunSpec
    ) -> None:
        pass

    @abc.abstractmethod
    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """
        last_events is the last event we've recorded for the jobs that we are interested
        in. poll_jobs will add new events to the EventLog for these jobs if there's been
        any change in their state.
        """
        pass
