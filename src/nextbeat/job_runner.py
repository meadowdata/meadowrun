"""
This module is more of a shim than anything else, will change significantly

TODO remote execution
TODO capabilities
TODO two phases when starting processes - first launched then running
"""

from typing import Any, Callable, Dict, Iterable, Literal, Optional
from uuid import UUID
from dataclasses import dataclass
from concurrent.futures import ProcessPoolExecutor, Future, CancelledError


@dataclass(frozen=True)
class JobResult:
    job_id: UUID
    outcome: Literal["running", "succeeded", "failed", "cancelled"]
    result: Optional[Any] = None

    def is_done(self) -> bool:
        return self.outcome != "running"


class LocalJobRunner:
    def __init__(self) -> None:
        self._running: Dict[UUID, Future] = {}
        self._executor = ProcessPoolExecutor(max_workers=5)

    def run(self, launch_id: UUID, fn: Callable, *args: Any, **kwargs: Any) -> None:
        if launch_id in self._running:
            return
        self._running[launch_id] = self._executor.submit(fn, *args, **kwargs)

    def poll_jobs(self, job_ids: Iterable[UUID]) -> Dict[UUID, JobResult]:
        result = {}
        for job_id in job_ids:
            if job_id in self._running:
                fut = self._running[job_id]
                if fut.done():
                    try:
                        fut_result = fut.result()
                        result[job_id] = JobResult(
                            job_id, outcome="succeeded", result=fut_result
                        )
                    except CancelledError as e:
                        result[job_id] = JobResult(
                            job_id, outcome="cancelled", result=e
                        )
                    except Exception as e:
                        result[job_id] = JobResult(job_id, "failed", result=e)
                else:
                    result[job_id] = JobResult(job_id, outcome="running")
        return result
