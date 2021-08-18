from typing import Dict, List, Literal, Tuple
import uuid
from nextbeat.job_runner import LocalJobRunner
from nextbeat.jobs import Actions, Job, JobPayload
from nextbeat.event_log import Event, EventLog, Timestamp


class Scheduler:
    """
    A scheduler gets set up with jobs, and then executes actions on jobs as per the
    triggers defined on those jobs.
    """

    def __init__(self) -> None:
        self._event_log = EventLog()
        self._jobs: Dict[str, Job] = {}
        self._outstanding_subscriptions: List[Job] = []
        self._launched: Dict[uuid.UUID, str] = {}
        self._job_runner = LocalJobRunner()

    # adding jobs and updating subscriptions is done in two phases to avoid order
    # dependence (otherwise can't add a job that triggers based on another without
    # adding the other first), and allows even circular dependencies. I.e. add_job
    # should be called (repeatedly), then update_subscriptions should be called.

    def add_job(self, job: Job) -> None:
        if job.name in self._jobs:
            raise ValueError(f"Job with name {job.name} already exists.")
        self._jobs[job.name] = job
        self._outstanding_subscriptions.append(job)
        self._event_log.append_event(job.name, JobPayload("waiting"))

    def update_subscriptions(self) -> None:
        """Should be called after all jobs are added. See comment above add_job."""

        # TODO: this should also check the new jobs' preconditions against the existing
        #  state. Perhaps they should already trigger.
        # TODO: should make sure we don't try to proceed without calling
        #  update_subscriptions first
        for job in self._outstanding_subscriptions:
            for trigger, action in job.trigger_actions:

                def subscriber(
                    low_timestamp: Timestamp, high_timestamp: Timestamp
                ) -> None:
                    events: Dict[str, Tuple[Event, ...]] = {}
                    for name in trigger.topic_names_to_subscribe():
                        events[name] = tuple(
                            self._event_log.events_and_state(
                                name, low_timestamp, high_timestamp
                            )
                        )
                    if trigger.is_active(events):
                        action.execute(
                            job, self._job_runner, self._event_log, high_timestamp
                        )

                self._event_log.subscribe(
                    trigger.topic_names_to_subscribe(), subscriber
                )
        self._outstanding_subscriptions.clear()

    def manual_run(self, job_name: str) -> None:
        """Execute the Run Action on the specified job"""
        if job_name not in self._jobs:
            raise ValueError(f"Unknown job: {job_name}")
        job = self._jobs[job_name]
        Actions.run.execute(
            job,
            self._job_runner,
            self._event_log,
            self._event_log.curr_timestamp,
        )

    def _get_launched_and_running_jobs(
        self, timestamp: Timestamp
    ) -> Dict[uuid.UUID, Tuple[str, Literal["launched", "running"]]]:
        """Returns launch_id -> (name, JobState)"""
        result = {}
        for name in self._jobs.keys():
            ev = self._event_log.last_event(name, timestamp)
            if ev and ev.payload.state in ["launched", "running"]:
                result[ev.payload.extra_info["launch_id"]] = name, ev.payload.state
        return result

    def step(self) -> None:
        """Runs the scheduler for one step"""

        # first, call all subscribers for events that have happened so far
        timestamp = self._event_log.call_subscribers()

        # second, poll launched/running jobs and create events if they've changed state
        jobs_to_poll = self._get_launched_and_running_jobs(timestamp)
        poll_result = self._job_runner.poll_jobs(jobs_to_poll.keys())
        for launch_id, job_result in poll_result.items():
            if job_result.is_done():
                job_name = jobs_to_poll[launch_id][0]
                if jobs_to_poll[launch_id][1] == "launched":
                    self._event_log.append_event(
                        job_name, JobPayload("running", dict(launch_id=launch_id))
                    )
                self._event_log.append_event(
                    job_name,
                    JobPayload(
                        job_result.outcome, extra_info=dict(result=job_result.result)
                    ),
                )

    def is_done(self) -> bool:
        return not self._event_log.all_subscribers_called() and (
            len(self._get_launched_and_running_jobs(self._event_log.curr_timestamp))
            == 0
        )

    def events_of(self, job_name: str) -> List[Event]:
        """For unit tests/debugging"""
        return list(
            self._event_log.events_and_state(
                job_name, 0, self._event_log.curr_timestamp
            )
        )
