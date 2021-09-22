"""
These functions should be called from inside of nextbeat jobs to indicate that an effect
has happened in that job.
"""

from nextbeat.jobs import Job
from nextbeat.jobs_common import Effects

# effects are processed in Scheduler._process_effects
_effects = Effects([])


def add_job(job: Job) -> None:
    """
    This is equivalent to calling NextBeatClientAsync.add_job on the nextbeat server
    that spawned this job.
    """
    _effects.add_jobs.append(job)
