from __future__ import annotations

import pickle
from types import TracebackType
from typing import Iterable, Optional, Type

from meadowflow.event_log import Event, EventLog
from meadowflow.jobs import (
    RaisedException,
    JobPayload,
    LocalFunction,
    JobRunnerFunction,
    JobRunner,
)
from meadowflow.topic_names import TopicName
from meadowgrid.config import DEFAULT_COORDINATOR_ADDRESS
from meadowgrid.coordinator_client import (
    MeadowGridCoordinatorClientAsync,
    ProcessStateEnum,
)
from meadowgrid.deployed_function import (
    convert_local_to_deployed_function,
    MeadowGridDeployedRunnable,
)


class MeadowGridJobRunner(JobRunner):
    """Integrates meadowgrid with meadowflow. Runs jobs on a meadowgrid server."""

    def __init__(self, event_log: EventLog, address: str = DEFAULT_COORDINATOR_ADDRESS):
        self._client = MeadowGridCoordinatorClientAsync(address)
        self._event_log = event_log

    async def _run_deployed_runnable(
        self,
        job_name: TopicName,
        run_request_id: str,
        deployed_runnable: MeadowGridDeployedRunnable,
    ) -> None:
        result = await self._client.add_py_runnable_job(
            run_request_id, job_name.as_file_name(), deployed_runnable
        )

        if result == "IS_DUPLICATE":
            # TODO handle this case and test it
            raise NotImplementedError()
        elif result == "ADDED":
            pass  # success
        else:
            raise ValueError(f"Did not expect AddJobState {result}")

    async def run(
        self,
        job_name: TopicName,
        run_request_id: str,
        job_runner_function: JobRunnerFunction,
    ) -> None:
        """Dispatches to _run_deployed_function which calls meadowgrid"""
        if isinstance(job_runner_function, MeadowGridDeployedRunnable):
            await self._run_deployed_runnable(
                job_name, run_request_id, job_runner_function
            )
        elif isinstance(job_runner_function, LocalFunction):
            await self._run_deployed_runnable(
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
                "supported by MeadowGridJobRunner"
            )

    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """
        See docstring on base class. This code basically translates the meadowgrid
        ProcessState into a JobPayload
        """

        last_events = list(last_events)

        process_states = await self._client.get_simple_job_states(
            [e.payload.request_id for e in last_events]  # type: ignore[misc]
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
            elif process_state.state == ProcessStateEnum.ASSIGNED:
                new_payload = JobPayload(request_id, "RUNNING")
            elif process_state.state == ProcessStateEnum.RUNNING:
                # TODO if we already created a RUNNING event without a pid based on a
                #  ProcessStateEnum.ASSIGNED, this update will be ignored and we'll
                #  never get the pid of the process. This could be okay, or we could fix
                #  this.
                new_payload = JobPayload(request_id, "RUNNING", pid=process_state.pid)
            elif process_state.state == ProcessStateEnum.SUCCEEDED:
                if process_state.pickled_result:
                    result_value, effects = pickle.loads(process_state.pickled_result)
                else:
                    result_value, effects = None, None

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

            if updated_last_event.payload.state != new_payload.state:  # type: ignore[union-attr] # noqa E501
                if (
                    updated_last_event.payload.state == "RUN_REQUESTED"  # type: ignore[union-attr] # noqa E501
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
            (MeadowGridDeployedRunnable, LocalFunction),
        )

    async def __aenter__(self) -> MeadowGridJobRunner:
        await self._client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return await self._client.__aexit__(exc_type, exc_value, traceback)
