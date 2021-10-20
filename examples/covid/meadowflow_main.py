"""
A typical use case would have a meadowflow_main.py file which has three functions:
- meadowflow_main: This is a job function which would get added to meadowflow manually via a
  UI/command line (here simulated by initial_setup). This function will add two jobs,
  instantiate_scopes and add_daily_jobs.
- instantiate_scopes: This is a simple job function which just instantiates today's
  scope whenever it is run.
- add_daily_jobs: This is a job function that is set up to be run any time a date-based
  scope is instantiated, and it will add the jobs to that day's scope. This is what we
  would think of as the "job definition function" because most jobs will be daily jobs.
"""
import datetime
import os
import pathlib
import sys
import types
from typing import Optional, Sequence, Any, Dict, Callable

import pytz
from meadowflow.meadowrun_job_runner import (
    MeadowRunFunctionGitRepo,
    GitRepo,
    MeadowRunCommandGitRepo,
)

import covid_data
import covid_data.cdc_covid_data
import covid_data.mdb.schema
from meadowflow.effects import UntilMeadowdbWritten, MeadowdbDynamicDependency
from meadowflow.event_log import Event
from meadowflow.events_arg import LatestEventsArg
from meadowflow.jobs import (
    Job,
    Actions,
    ScopeValues,
    add_scope_jobs_decorator,
)
from meadowflow.scopes import ScopeInstantiated
from meadowflow.server.client import MeadowFlowClientSync
from meadowflow.time_event_publisher import TimeOfDay, Periodic, PointInTimePredicate
from meadowflow.topic import (
    EventFilter,
    StatePredicate,
    TruePredicate,
    TriggerAction,
    AllPredicate,
)
from meadowflow.topic_names import pname, FrozenDict, TopicName
from meadowrun.deployed_function import (
    MeadowRunFunction,
    MeadowRunDeployedFunction,
    MeadowRunDeployedCommand,
)
from meadowrun.meadowrun_pb2 import ServerAvailableFolder

_GIT_REPO_ROOT = covid_data.ROOT_DIR
_GIT_BRANCH = "main"
_PATH_IN_REPO = "examples/covid"
_PYTHON_INTERPRETER = sys.executable


def initial_setup():
    # Pretend that we add the meadowflow_main function to meadowflow manually, either
    # via a UI or the command line. Inputs are:
    git_repo_root = _GIT_REPO_ROOT
    git_branch = _GIT_BRANCH
    path_in_repo = _PATH_IN_REPO
    module = "meadowflow_main"
    function = "meadowflow_main"

    # UI will execute:
    client = MeadowFlowClientSync()
    client.add_jobs(
        [
            Job(
                pname(function),
                MeadowRunFunctionGitRepo(
                    GitRepo(
                        git_repo_root, git_branch, _PYTHON_INTERPRETER, path_in_repo
                    ),
                    MeadowRunFunction(module, function),
                ),
                (),
            )
        ]
    )

    # Then pretend this is also invoked via the UI/command line
    client.manual_run(pname(function), wait_for_completion=True)


def _function(
    function_pointer: Callable,
    function_args: Optional[Sequence[Any]] = None,
    function_kwargs: Optional[Dict[str, Any]] = None,
    *,
    run_on: Optional[EventFilter] = None,
    run_state_predicate: StatePredicate = TruePredicate(),
    job_name: Optional[str] = None,
):
    """
    This is a helper function to reduce the ceremony in defining jobs. For this example,
    we're assuming here that the meadowflow/meadowrun servers are running locally, so
    when we use "ServerAvailableFolder" this is effectively a local folder. job_name
    defaults to the function_name

    TODO consider creating something like this in the core code.
    """

    # TODO this should have a lot more checks around it to make sure this will
    #  round-trip correctly.
    if not isinstance(function_pointer, types.FunctionType):
        raise ValueError("TODO")
    module_name = function_pointer.__module__
    function_name = function_pointer.__qualname__

    if job_name is None:
        job_name = function_name

    if run_on is None:
        run_on = ()
    elif isinstance(run_on, EventFilter):
        run_on = [run_on]

    return Job(
        pname(job_name),
        MeadowRunFunctionGitRepo(
            GitRepo(_GIT_REPO_ROOT, _GIT_BRANCH, _PYTHON_INTERPRETER, _PATH_IN_REPO),
            MeadowRunFunction(
                module_name, function_name, function_args, function_kwargs
            ),
        ),
        [TriggerAction(Actions.run, run_on, run_state_predicate)],
    )


REPORTS_DIR = covid_data.ROOT_DIR / "test_data" / "reports"


def _notebook(
    notebook_path: str,
    context_variables: Optional[Dict[str, Any]] = None,
    output_dir: str = REPORTS_DIR,
    output_name: str = None,
    cell_timeout_seconds: int = 1200,
    run_on: Optional[EventFilter] = None,
    run_state_predicate: StatePredicate = TruePredicate(),
    job_name: Optional[str] = None,
):
    """
    This is a helper function to make it easier to define jobs that run notebooks and
    turn them into html. See _function for more details. job_name defaults to the name
    of the notebook specified in notebook_path. output_name also defaults to the name of
    the notebook.
    """
    notebook_name = os.path.splitext(os.path.basename(notebook_path))[0]

    if job_name is None:
        job_name = notebook_name

    if run_on is None:
        run_on = ()
    elif isinstance(run_on, EventFilter):
        run_on = [run_on]

    command_line = [
        "jupyter",
        "nbconvert",
        notebook_path,
        "--to",
        "html",
        "--execute",
        f"--ExecutePreprocessor.timeout={cell_timeout_seconds}",
        "--TemplateExporter.exclude_input=True",
        "--TemplateExporter.exclude_output_prompt=True",
        f"--output-dir={output_dir}",
    ]
    if output_name:
        command_line.append(f"--output={output_name}")

    return Job(
        pname(job_name),
        MeadowRunCommandGitRepo(
            GitRepo(_GIT_REPO_ROOT, _GIT_BRANCH, _PYTHON_INTERPRETER, _PATH_IN_REPO),
            command_line,
            context_variables,
        ),
        [TriggerAction(Actions.run, run_on, run_state_predicate)],
    )


def meadowflow_main():
    return [
        # Defines the following "infrastructure jobs":
        # Instantiate the date-based scope at 12 noon on the previous day
        _function(
            function_pointer=instantiate_scopes,
            function_args=[None, LatestEventsArg.construct()],
            run_on=TimeOfDay(
                datetime.timedelta(hours=-12), pytz.timezone("America/New_York")
            ),
        ),
        # Whenever a date scope is instantiated, add the daily jobs to that date scope
        _function(
            function_pointer=add_daily_jobs,
            function_args=[LatestEventsArg.construct()],
            run_on=ScopeInstantiated(frozenset(["date"])),
        ),
        # These are just "regular jobs"
        # On-demand, define/re-define schemas for meadowdb tables
        _function(function_pointer=covid_data.mdb.schema.define_schemas),
    ]


def instantiate_scopes(
    date: Optional[datetime.date], events: Optional[FrozenDict[TopicName, Event]]
):
    """
    In "normal operation", we'll read the date off of events which should be populated
    via LatestEventsArg. However, for manual runs we include the date parameter which
    will be an override.
    """
    # If date is supplied, then we take that regardless of whether events is there.
    # If both are None, then we have a problem
    if date is None:
        if events is None:
            raise ValueError(
                "Both date and events are None, please specify at least one"
            )
        if len(events) != 1:
            raise ValueError("There should only be one dependency for this job")

        date = list(events.values())[0].payload.date  # This will be a TimeOfDay Payload

    # instantiate a scope for the specified date
    return ScopeValues(date=date)


@add_scope_jobs_decorator
def add_daily_jobs(scope):
    date = scope["date"]

    def tomorrow(hour: int = 0, minute: int = 0, second: int = 0) -> datetime.datetime:
        return datetime.datetime.combine(
            date + datetime.timedelta(days=1),
            datetime.time(hour=hour, minute=minute, second=second),
            tzinfo=pytz.timezone("America/New_York"),
        )

    # Define the following jobs:
    return [
        # Gets covid data from the CDC API. Run every 2 minutes between 8:30am and
        # 5:30pm "tomorrow", which is when the data is usually posted. Only run while we
        # have not managed to write any data successfully yet.
        # TODO this should fail/alert if it's 5:30pm and we haven't gotten any data
        _function(
            function_pointer=covid_data.cdc_covid_data.cdc_covid_data,
            function_args=[date],
            run_on=Periodic(datetime.timedelta(minutes=2)),
            run_state_predicate=AllPredicate(
                [
                    PointInTimePredicate.between(tomorrow(8, 30), tomorrow(17, 30)),
                    UntilMeadowdbWritten.any(),
                ]
            ),
        ),
        # Run the smoothing job whenever its data inputs change
        # TODO this doesn't actually work right now--meadowflow needs to figure out what
        #  its initial dependencies are--either by running it once as soon as it's
        #  defined and hoping it doesn't have any side effects, doing some sort of
        #  static analysis, or getting some sort of hint from the user.
        _function(
            function_pointer=covid_data.cdc_covid_data.cdc_covid_data_smoothed,
            function_args=[date],
            run_on=MeadowdbDynamicDependency(scope),
        ),
        # Run the report whenever its data inputs change
        # TODO this should not kick off when cdc_covid_data completes, we should wait
        #  until cdc_covid_data_smoothed has run
        _notebook(
            notebook_path="covid_data/cdc_covid_report.ipynb",
            context_variables={"date": date},
            output_name=f"cdc_covid_report_{date:%Y-%m-%d}",
            run_on=MeadowdbDynamicDependency(scope),
        ),
    ]
