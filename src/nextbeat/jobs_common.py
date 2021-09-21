"""
This all thematically belongs in jobs.py but we broke it out to avoid circular imports
"""

import abc
import dataclasses
from dataclasses import dataclass
from typing import Any, Optional, Iterable, Union, Literal, Callable, Sequence, Dict

from nextbeat.event_log import Event
from nextrun.deployed_function import NextRunDeployedFunction

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
    # is PYTHON_EXCEPTION or RUN_REQUEST_FAILED, raised_exception will be populated, if
    # failure_type is NON_ZERO_RETURN_CODE, return_code will be populated
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
    failure_type: Optional[
        Literal["PYTHON_EXCEPTION", "NON_ZERO_RETURN_CODE", "RUN_REQUEST_FAILED"]
    ] = None
    pid: Optional[int] = None
    result_value: Any = None
    raised_exception: Union[RaisedException, BaseException, None] = None
    return_code: Optional[int] = None


@dataclasses.dataclass(frozen=True)
class LocalFunction:
    """
    A function pointer in the current codebase with arguments for calling the
    function
    """

    function_pointer: Callable[..., Any]
    function_args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    function_kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


# A JobRunnerFunction is a function/executable/script that one or more JobRunners will
# know how to run along with the arguments for that function/executable/script
JobRunnerFunctionTypes = (LocalFunction, NextRunDeployedFunction)
JobRunnerFunction = Union[JobRunnerFunctionTypes]


class JobRunner(abc.ABC):
    """An interface for job runner clients"""

    @abc.abstractmethod
    async def run(
        self, job_name: str, run_request_id: str, job_runner_function: JobRunnerFunction
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


class VersionedJobRunnerFunction(abc.ABC):
    """
    Similar to a JobRunnerFunction, but instead of a single version of the code (e.g. a
    specific commit in a git repo), specifies a versioned codebase (e.g. a git repo),
    along with a function/executable/script in that repo (and also including the
    arguments to call that function/executable/script).

    TODO this is not yet fully fleshed out and the interface will probably need to
     change
    """

    @abc.abstractmethod
    def get_job_runner_function(self) -> JobRunnerFunction:
        pass
