"""
The base JobRunnerPredicate class is in jobs.py, but the implementations are here
partially for organization and partially because it's too hard to deal with the circular
imports.
"""

from typing import Dict, Type, Any

import meadowflow.local_job_runner
import meadowflow.meadowgrid_job_runner
from meadowflow.jobs import JobRunnerPredicate, JobRunner


class AndPredicate(JobRunnerPredicate):
    def __init__(self, a: JobRunnerPredicate, b: JobRunnerPredicate):
        self._a = a
        self._b = b

    def apply(self, job_runner: JobRunner) -> bool:
        return self._a.apply(job_runner) and self._b.apply(job_runner)


class OrPredicate(JobRunnerPredicate):
    def __init__(self, a: JobRunnerPredicate, b: JobRunnerPredicate):
        self._a = a
        self._b = b

    def apply(self, job_runner: JobRunner) -> bool:
        return self._a.apply(job_runner) or self._b.apply(job_runner)


class ValueInPropertyPredicate(JobRunnerPredicate):
    """E.g. ValueInPropertyPredicate("capabilities", "chrome")"""

    def __init__(self, property_name: str, value: Any):
        self._property_name = property_name
        self._value = value

    def apply(self, job_runner: JobRunner) -> bool:
        return self._value in getattr(job_runner, self._property_name)


_JOB_RUNNER_TYPES: Dict[str, Type] = {
    "local": meadowflow.local_job_runner.LocalJobRunner,
    "meadowgrid": meadowflow.meadowgrid_job_runner.MeadowGridJobRunner,
}


class JobRunnerTypePredicate(JobRunnerPredicate):
    def __init__(self, job_runner_type: str):
        self._job_runner_type = _JOB_RUNNER_TYPES[job_runner_type]

    def apply(self, job_runner: JobRunner) -> bool:
        # noinspection PyTypeHints
        return isinstance(job_runner, self._job_runner_type)
