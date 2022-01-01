"""
TODO capabilities
TODO checking for and restarting requested but not running jobs
"""
import multiprocessing
import traceback
from concurrent.futures.process import BrokenProcessPool
from typing import Dict, Iterable, Callable, Any, Tuple, Sequence, Optional
from concurrent.futures import ProcessPoolExecutor, Future, CancelledError

from meadowflow.event_log import Event, EventLog
from meadowflow.topic_names import TopicName
from meadowflow.jobs import (
    JobPayload,
    JobRunner,
    LocalFunction,
    JobRunnerFunction,
    RaisedException,
)
import meadowflow.effects


def _function_wrapper(
    func: Callable[..., Any],
    args: Optional[Sequence[Any]],
    kwargs: Optional[Dict[str, Any]],
) -> Tuple[Any, meadowflow.effects.Effects]:
    """
    This is roughly the LocalJobRunner equivalent to __meadowgrid_func_worker.py, it
    will run in the ProcessPoolExecutor child processes
    """

    # we need to reset effects as ProcessPoolExecutor will reuse the same processes
    meadowflow.effects.reset_effects()
    result = func(*(args or ()), **(kwargs or {}))
    effects = meadowflow.effects.get_effects()
    return result, effects


class LocalJobRunner(JobRunner):
    """Runs jobs on the current machine using a ProcessPoolExecutor"""

    def __init__(self, event_log: EventLog):
        self._running: Dict[str, Future] = {}
        ctx = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(max_workers=5, mp_context=ctx)
        self._event_log = event_log

    async def run(
        self,
        job_name: TopicName,
        run_request_id: str,
        job_runner_function: JobRunnerFunction,
    ) -> None:
        if run_request_id in self._running:
            return

        if isinstance(job_runner_function, LocalFunction):
            self._running[run_request_id] = self._executor.submit(
                _function_wrapper,
                job_runner_function.function_pointer,
                job_runner_function.function_args,
                job_runner_function.function_kwargs,
            )
        else:
            # TODO add support for other JobRunSpecs
            raise ValueError(
                f"LocalJobRunner does not support {type(job_runner_function)}"
            )

    async def poll_jobs(self, last_events: Iterable[Event[JobPayload]]) -> None:
        """See docstring on base class"""

        # TODO can we have more than one run_request_id going for the same job?

        for last_event in last_events:
            request_id = last_event.payload.request_id
            if request_id in self._running:
                fut = self._running[request_id]
                if fut.done():
                    try:
                        fut_result, effects = fut.result()
                        # TODO add pid to all of these?
                        new_payload = JobPayload(
                            request_id,
                            "SUCCEEDED",
                            result_value=fut_result,
                            effects=effects,
                        )
                    except BrokenProcessPool:
                        # TODO this means the process pool needs to be restarted
                        raise
                    except CancelledError as e:
                        new_payload = JobPayload(
                            last_event.payload.request_id,
                            "CANCELLED",
                            raised_exception=e,
                        )
                    except Exception as e:
                        raised_exception = RaisedException(
                            str(type(e)), str(e), traceback.format_exc()
                        )
                        new_payload = JobPayload(
                            request_id, "FAILED", raised_exception=raised_exception
                        )
                else:
                    # TODO this isn't technically correct, we could still be in
                    #  RUN_REQUESTED state
                    new_payload = JobPayload(request_id, "RUNNING")

                if last_event.payload.state != new_payload.state:
                    # if we went straight from RUN_REQUESTED to one of the "done"
                    # states, then "make up" the RUNNING state that we didn't see, but
                    # we know it must have happened
                    if (
                        last_event.payload.state == "RUN_REQUESTED"
                        and new_payload.state != "RUNNING"
                    ):
                        self._event_log.append_event(
                            last_event.topic_name,
                            JobPayload(request_id, "RUNNING", pid=new_payload.pid),
                        )
                    self._event_log.append_event(last_event.topic_name, new_payload)
            else:
                # TODO we should probably be doing something with the run_request_ids
                #  that we don't recognize
                pass

    def can_run_function(self, job_runner_function: JobRunnerFunction) -> bool:
        return isinstance(job_runner_function, LocalFunction)
