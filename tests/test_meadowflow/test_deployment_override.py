from typing import Union, cast

from meadowflow.jobs import Job, JobRunOverrides
from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.topic_names import pname
import meadowgrid.coordinator_main
import meadowgrid.agent_main
import pytest
from meadowgrid.config import MEADOWGRID_INTERPRETER
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientAsync
from meadowgrid.deployed_function import (
    CodeDeployment,
    MeadowGridDeployedRunnable,
    MeadowGridFunction,
    VersionedCodeDeployment,
)
from meadowgrid.meadowgrid_pb2 import (
    GitRepoBranch,
    GitRepoCommit,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)
from test_meadowflow.test_scheduler import _wait_for_scheduler
from test_meadowgrid.test_meadowgrid_basics import (
    EXAMPLE_CODE,
    MEADOWDATA_CODE,
    TEST_REPO,
    TEST_WORKING_FOLDER,
    wait_for_agents_async,
)


@pytest.mark.asyncio
async def test_deployment_override() -> None:
    """
    Tests using JobRunOverride.deployment

    Requires cloning https://github.com/meadowdata/test_repo in the folder "next to" the
    meadowdata repo, and also pulling the test_branch branch in test_repo
    """
    with (
        meadowgrid.coordinator_main.main_in_child_process(),
        meadowgrid.agent_main.main_in_child_process(TEST_WORKING_FOLDER),
    ):
        async with Scheduler(
            job_runner_poll_delay_seconds=0.05
        ) as s, MeadowGridCoordinatorClientAsync() as coordinator_client:
            # TODO this line is sketchy as it's not necessarily guaranteed to run before
            #  anything in the next function
            s.register_job_runner(MeadowGridJobRunner)
            await wait_for_agents_async(coordinator_client, 1)

            s.add_jobs(
                [
                    Job(
                        pname("A"),
                        MeadowGridDeployedRunnable(
                            ServerAvailableFolder(
                                code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE],
                            ),
                            ServerAvailableInterpreter(
                                interpreter_path=MEADOWGRID_INTERPRETER
                            ),
                            MeadowGridFunction.from_name(
                                "example_package.example", "unique_per_deployment"
                            ),
                        ),
                        (),
                    )
                ]
            )

            expected_num_events = 1

            async def result_with_deployment(
                deployment: Union[CodeDeployment, VersionedCodeDeployment]
            ) -> str:
                """
                Runs the A job with the specified deployment override and returns the
                result_value
                """
                nonlocal expected_num_events

                s.manual_run(pname("A"), JobRunOverrides(code_deployment=deployment))
                await _wait_for_scheduler(s)
                events = s.events_of(pname("A"))
                expected_num_events += 3
                assert len(events) == expected_num_events
                assert events[0].payload.state == "SUCCEEDED"
                return cast(str, events[0].payload.result_value)

            # same as original
            result = await result_with_deployment(
                ServerAvailableFolder(code_paths=[EXAMPLE_CODE, MEADOWDATA_CODE])
            )
            assert result == "embedded in main repo"

            # specific commit
            result = await result_with_deployment(
                GitRepoCommit(
                    repo_url=TEST_REPO,
                    commit="2fcaca67ea40c35a96de39716e32e4c74cb7f221",
                )
            )
            assert result == "in test_repo older commit"

            # branch
            result = await result_with_deployment(
                GitRepoBranch(repo_url=TEST_REPO, branch="test_branch")
            )
            assert result == "in test_repo test_branch"

            # a different branch
            result = await result_with_deployment(
                GitRepoBranch(repo_url=TEST_REPO, branch="main")
            )
            assert result == "in test_repo newer commit"
