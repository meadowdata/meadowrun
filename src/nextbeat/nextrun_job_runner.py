import pickle
import dataclasses
from typing import Iterable, Sequence, Union

from nextbeat.event_log import Event, EventLog
from nextbeat.topic_names import TopicName
from nextbeat.jobs import (
    RaisedException,
    JobPayload,
    LocalFunction,
    JobRunnerFunction,
    JobRunner,
    VersionedJobRunnerFunction,
)
from nextrun.client import NextRunClientAsync, ProcessStateEnum
from nextrun.config import DEFAULT_ADDRESS
from nextrun.deployed_function import (
    NextRunFunction,
    NextRunDeployedFunction,
    convert_local_to_deployed_function,
    NextRunDeployedCommand,
)
from nextrun.nextrun_pb2 import GitRepoCommit


class NextRunJobRunner(JobRunner):
    """Integrates nextrun with nextbeat. Runs jobs on a nextrun server."""

    def __init__(self, event_log: EventLog, address: str = DEFAULT_ADDRESS):
        self._client = NextRunClientAsync(address)
        self._event_log = event_log

    async def _run_deployed_function(
        self,
        job_name: TopicName,
        run_request_id: str,
        deployed_function: Union[NextRunDeployedCommand, NextRunDeployedFunction],
    ) -> None:
        self._event_log.append_event(
            job_name, JobPayload(run_request_id, "RUN_REQUESTED")
        )

        if isinstance(deployed_function, NextRunDeployedCommand):
            result = await self._client.run_py_command(
                run_request_id, deployed_function
            )
        elif isinstance(deployed_function, NextRunDeployedFunction):
            result = await self._client.run_py_func(run_request_id, deployed_function)
        else:
            raise ValueError(
                f"Unexpected type of deployed_function {type(deployed_function)}"
            )

        if result.state == ProcessStateEnum.REQUEST_IS_DUPLICATE:
            # TODO handle this case and test it
            raise NotImplementedError()
        elif result.state == ProcessStateEnum.RUNNING:
            # TODO there is a very bad race condition here--the sequence of events could
            #  be:
            #  - run records RUN_REQUESTED
            #  - the nextrun server runs the job and it completes
            #  - poll_jobs runs and records SUCCEEDED
            #  - the post-await continuation of run happens and records RUNNING
            self._event_log.append_event(
                job_name,
                JobPayload(run_request_id, "RUNNING", pid=result.pid),
            )
        elif result.state == ProcessStateEnum.RUN_REQUEST_FAILED:
            # TODO handle this case and test it
            raise NotImplementedError()
        else:
            raise ValueError(f"Did not expect ProcessStateEnum {result.state}")

    async def run(
        self,
        job_name: TopicName,
        run_request_id: str,
        job_runner_function: JobRunnerFunction,
    ) -> None:
        """
        Dispatches to _run_deployed_function which calls nextrun
        """
        if isinstance(
            job_runner_function, (NextRunDeployedCommand, NextRunDeployedFunction)
        ):
            await self._run_deployed_function(
                job_name, run_request_id, job_runner_function
            )
        elif isinstance(job_runner_function, LocalFunction):
            await self._run_deployed_function(
                job_name,
                run_request_id,
                convert_local_to_deployed_function(
                    job_runner_function.function_pointer,
                    job_runner_function.function_args,
                    job_runner_function.function_kwargs,
                ),
            )
        else:
            raise ValueError(
                f"job_runner_function of type {type(job_runner_function)} is not "
                f"supported by NextRunJobRunner"
            )

    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """
        See docstring on base class. This code basically translates the nextrun
        ProcessState into a JobPayload
        """

        last_events = list(last_events)
        process_states = await self._client.get_process_states(
            [e.payload.request_id for e in last_events]
        )

        if len(last_events) != len(process_states):
            raise ValueError(
                "get_process_states returned a different number of requests than "
                f"expected, sent {len(last_events)}, got back {len(process_states)} "
                "responses"
            )

        timestamp = self._event_log.curr_timestamp

        for last_event, process_state in zip(last_events, process_states):
            request_id = last_event.payload.request_id
            topic_name = last_event.topic_name
            if process_state.state == ProcessStateEnum.RUN_REQUESTED:
                # this should never actually get written because we should always be
                # creating a RUN_REQUESTED event in the run function before we poll
                new_payload = JobPayload(
                    request_id, "RUN_REQUESTED", pid=process_state.pid
                )
            elif process_state.state == ProcessStateEnum.RUNNING:
                new_payload = JobPayload(request_id, "RUNNING", pid=process_state.pid)
            elif process_state.state == ProcessStateEnum.SUCCEEDED:
                result_value, effects = pickle.loads(process_state.pickled_result)
                new_payload = JobPayload(
                    request_id,
                    "SUCCEEDED",
                    pid=process_state.pid,
                    # TODO probably handle unpickling errors specially
                    result_value=result_value,
                    effects=effects,
                )
            elif process_state.state == ProcessStateEnum.RUN_REQUEST_FAILED:
                new_payload = JobPayload(
                    request_id,
                    "FAILED",
                    failure_type="RUN_REQUEST_FAILED",
                    raised_exception=RaisedException(
                        *pickle.loads(process_state.pickled_result)
                    ),
                )
            elif process_state.state == ProcessStateEnum.PYTHON_EXCEPTION:
                new_payload = JobPayload(
                    request_id,
                    "FAILED",
                    failure_type="PYTHON_EXCEPTION",
                    pid=process_state.pid,
                    # TODO probably handle unpickling errors specially
                    raised_exception=RaisedException(
                        *pickle.loads(process_state.pickled_result)
                    ),
                )
            elif process_state.state == ProcessStateEnum.NON_ZERO_RETURN_CODE:
                # TODO Test this case
                new_payload = JobPayload(
                    request_id,
                    "FAILED",
                    failure_type="NON_ZERO_RETURN_CODE",
                    pid=process_state.pid,
                    return_code=process_state.return_code,
                )
            elif process_state.state == ProcessStateEnum.CANCELLED:
                # TODO handle this and test it
                raise NotImplementedError("TBD")
            elif (
                process_state.state == ProcessStateEnum.UNKNOWN
                or process_state.state == ProcessStateEnum.ERROR_GETTING_STATE
            ):
                # TODO handle this case and test it
                raise NotImplementedError(
                    f"Not sure what to do here? Got {process_state.state} for job="
                    f"{topic_name} request_id={request_id}"
                )
            else:
                raise ValueError(
                    f"Did not expect ProcessStateEnum {process_state.state} for job="
                    f"{topic_name} request_id={request_id}"
                )

            # get the most recent updated_last_event. Because there's an await earlier
            # in this function, new events could have been added
            updated_last_event = self._event_log.last_event(topic_name, timestamp)

            if updated_last_event.payload.state != new_payload.state:
                if (
                    updated_last_event.payload.state == "RUN_REQUESTED"
                    and new_payload.state != "RUNNING"
                ):
                    self._event_log.append_event(
                        topic_name,
                        JobPayload(request_id, "RUNNING", pid=new_payload.pid),
                    )
                self._event_log.append_event(topic_name, new_payload)

    def can_run_function(self, job_runner_function: JobRunnerFunction) -> bool:
        return isinstance(
            job_runner_function,
            (NextRunDeployedCommand, NextRunDeployedFunction, LocalFunction),
        )

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._client.__aexit__(exc_type, exc_val, exc_tb)


@dataclasses.dataclass(frozen=True)
class GitRepo:
    """Represents a git repo"""

    # specifies the url, will be provided to git clone, see
    # https://git-scm.com/docs/git-clone
    repo_url: str

    default_branch: str

    # TODO this should actually be the name of a file in the repository that specifies
    #  what interpreter/libraries we should use. Currently it is just the path to the
    #  interpreter on the local machine
    interpreter_path: str

    def get_commit(self):
        # TODO this seems kind of silly right now, but we should move the logic for
        #  converting from a branch name to a specific commit hash to this function from
        #  _get_git_repo_commit_interpreter_and_code so we can show the user what commit
        #  we actually ran with.
        # TODO also we will add the ability to specify overrides (e.g. a specific commit
        #  or an alternate branch)
        return GitRepoCommit(
            repo_url=self.repo_url,
            # TODO this is very sketchy. Need to invest time in all of the possible
            #  revision specifications
            commit="origin/" + self.default_branch,
            interpreter_path=self.interpreter_path,
        )


@dataclasses.dataclass(frozen=True)
class NextRunCommandGitRepo(VersionedJobRunnerFunction):
    """Represents a NextRunCommand in a git repo"""

    git_repo: GitRepo
    command_line: Sequence[str]

    def get_job_runner_function(self) -> NextRunDeployedCommand:
        return NextRunDeployedCommand(self.git_repo.get_commit(), self.command_line)


@dataclasses.dataclass(frozen=True)
class NextRunFunctionGitRepo(VersionedJobRunnerFunction):
    """Represents a NextRunFunction in a git repo"""

    git_repo: GitRepo
    next_run_function: NextRunFunction

    def get_job_runner_function(self) -> NextRunDeployedFunction:
        return NextRunDeployedFunction(
            self.git_repo.get_commit(), self.next_run_function
        )
