import sys
from typing import Union

import meadowrun.server_main
from meadowflow.git_repo import GitRepo
from meadowflow.jobs import Job, JobRunOverrides
from meadowflow.meadowrun_job_runner import MeadowRunJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.topic_names import pname
from meadowrun.deployed_function import (
    MeadowRunDeployedFunction,
    MeadowRunFunction,
    Deployment,
)
from meadowrun.meadowrun_pb2 import ServerAvailableFolder, GitRepoCommit
from test_meadowflow.test_scheduler import _wait_for_scheduler
from test_meadowrun import EXAMPLE_CODE, MEADOWDATA_CODE, TEST_REPO


def test_deployment_override() -> None:
    """Tests using JobRunOverride.deployment"""
    with meadowrun.server_main.main_in_child_process():
        with Scheduler(job_runner_poll_delay_seconds=0.05) as s:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(MeadowRunJobRunner)

            s.add_jobs(
                [
                    Job(
                        pname("A"),
                        MeadowRunDeployedFunction(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                                interpreter_path=sys.executable,
                            ),
                            MeadowRunFunction(
                                module_name="example_package.example",
                                function_name="unique_per_deployment",
                            ),
                        ),
                        (),
                    )
                ]
            )

            s.main_loop()

            expected_num_events = 1

            def result_with_deployment(deployment: Union[GitRepo, Deployment]) -> str:
                """
                Runs the A job with the specified deployment override and returns the
                result_value
                """
                nonlocal expected_num_events

                s.manual_run(pname("A"), JobRunOverrides(deployment=deployment))
                _wait_for_scheduler(s)
                events = s.events_of(pname("A"))
                expected_num_events += 3
                assert len(events) == expected_num_events
                assert events[0].payload.state == "SUCCEEDED"
                return events[0].payload.result_value

            # same as original
            result = result_with_deployment(
                ServerAvailableFolder(
                    code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                    interpreter_path=sys.executable,
                )
            )
            assert result == "embedded in main repo"

            # specific commit
            result = result_with_deployment(
                GitRepoCommit(
                    repo_url=TEST_REPO,
                    commit="2fcaca67ea40c35a96de39716e32e4c74cb7f221",
                    interpreter_path=sys.executable,
                )
            )
            assert result == "in test_repo older commit"

            # branch
            result = result_with_deployment(
                GitRepo(TEST_REPO, "test_branch", sys.executable)
            )
            assert result == "in test_repo test_branch"

            # a different branch
            result = result_with_deployment(GitRepo(TEST_REPO, "main", sys.executable))
            assert result == "in test_repo newer commit"
