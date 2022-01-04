import asyncio
import itertools
import os
import time
from typing import Tuple, Dict, Iterable, List

import pytest

import meadowgrid.agent_main
import meadowgrid.coordinator_main
from meadowgrid import ServerAvailableInterpreter, ServerAvailableFolder
from meadowgrid.config import (
    LOGICAL_CPU,
    MEADOWGRID_AGENT_PID,
    MEADOWGRID_INTERPRETER,
    MEMORY_GB,
)
from meadowgrid.coordinator_client import MeadowGridCoordinatorClientAsync
from meadowgrid.grid import grid_map_async
from test_meadowgrid.test_meadowgrid_basics import (
    TEST_WORKING_FOLDER,
    MEADOWDATA_CODE,
    wait_for_agents_async,
)


async def run_job(
    task_arguments: List[float],
    t0: float,
    resources_required_per_task: Dict[str, float],
) -> List[Tuple[float, int, int]]:
    """
    Run one job that runs a task for each element in task_arguments. The task_argument
    specifies how long that task will sleep for, then the task will return the time the
    task started (relative to t0), the pid of the agent that launched the task/job, and
    the pid of the task/job process itself
    """

    def get_time_and_pid(x: float) -> Tuple[float, int, int]:
        # make this a local function so that it just gets cloudpickled and we don't have
        # to worry about getting the code over to the executor
        t0 = time.time()
        time.sleep(x)
        results = t0, int(os.environ[MEADOWGRID_AGENT_PID]), os.getpid()
        return results

    tasks = await grid_map_async(
        get_time_and_pid,
        task_arguments,
        ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
        ServerAvailableFolder(code_paths=[MEADOWDATA_CODE]),
        resources_required_per_task=resources_required_per_task,
    )

    results = []
    for task in tasks:
        t1, agent_pid, task_pid = await task
        results.append((t1 - t0, agent_pid, task_pid))
    return results


@pytest.mark.asyncio
async def test_basic_resources():
    with meadowgrid.coordinator_main.main_in_child_process():
        # create 2 agents that are slightly differently sized
        with meadowgrid.agent_main.main_in_child_process(
            TEST_WORKING_FOLDER, {MEMORY_GB: 20, LOGICAL_CPU: 10}
        ) as agent1_pid, meadowgrid.agent_main.main_in_child_process(
            TEST_WORKING_FOLDER + "2", {MEMORY_GB: 18, LOGICAL_CPU: 9}
        ) as agent2_pid:

            t0 = time.time()

            async with MeadowGridCoordinatorClientAsync() as coordinator_client:
                await wait_for_agents_async(coordinator_client, 2)

            # all of the jobs we're running will require the same resources:
            resources_required: Dict[str, float] = {MEMORY_GB: 4, LOGICAL_CPU: 2}

            # now run a bunch of jobs

            job1_future = asyncio.create_task(run_job([1] * 4, t0, resources_required))
            await asyncio.sleep(0.25)
            job2_future = asyncio.create_task(
                run_job([0.75] * 5, t0, resources_required)
            )
            await asyncio.sleep(0.25)
            job3_future = asyncio.create_task(run_job([1] * 9, t0, resources_required))

            # expected results:

            # job1 ran first, so all of its tasks should have gone to agent2, because
            # that's the smaller agent (see rationale in _remaining_resources_sort_key),
            # and they all should have started at the same time
            job1_results = await job1_future
            assert all(r[1] == agent2_pid for r in job1_results)
            # TODO we should be able to have a much lower tolerance on these checks
            #  (maybe 0.1s?), but we have some performance issues
            assert (
                max(r[0] for r in job1_results) - min(r[0] for r in job1_results) < 0.5
            )

            # job2 ran next while agent2 was occupied (there was an extra 2GB/1cpu but
            # that's too small for another task), so all of its tasks should have
            # started around the same time on agent1:
            job2_results = await job2_future
            assert all(r[1] == agent1_pid for r in job2_results)
            assert (
                max(r[0] for r in job2_results) - min(r[0] for r in job2_results) < 0.5
            )

            # finally, job3 started while both agent1 and agent2 were fully
            # occupied--tasks would have started on both agents as they freed up.
            job3_results = await job3_future
            assert len(set(r[1] for r in job3_results)) == 2
            assert (
                # TODO same performance issues as above, these jobs should start within
                #  0.5s of each other
                max(r[0] for r in job3_results) - min(r[0] for r in job3_results)
                < 1.5
            )


@pytest.mark.asyncio
async def test_custom_resources():
    with meadowgrid.coordinator_main.main_in_child_process():
        # create 2 agents that each think they have 2 logical CPUs available. Also,
        # agent1 has 2 "foo" resources available
        with meadowgrid.agent_main.main_in_child_process(
            TEST_WORKING_FOLDER, {"foo": 2, LOGICAL_CPU: 2}
        ) as agent1_pid, meadowgrid.agent_main.main_in_child_process(
            TEST_WORKING_FOLDER + "2", {LOGICAL_CPU: 2}
        ) as agent2_pid:

            async with MeadowGridCoordinatorClientAsync() as coordinator_client:
                await wait_for_agents_async(coordinator_client, 2)

            t0 = time.time()

            async def run_jobs(
                resources_required: Iterable[Dict[str, float]]
            ) -> Tuple[List[float], List[int], List[int]]:
                """
                Effectively calls run_job for each of resources_required, and then
                splits the output into a list of task start times, a list of agent pids,
                and a list of job/task pids. The outputs are sorted by the task start
                times.
                """
                results = itertools.chain(
                    *(
                        await asyncio.gather(
                            *(run_job([1], t0, r) for r in resources_required)
                        )
                    )
                )

                task_times = []
                agent_pids = []
                task_pids = []

                for task_time, agent_pid, task_pid in sorted(
                    results, key=lambda r: r[0]
                ):
                    task_times.append(task_time)
                    agent_pids.append(agent_pid)
                    task_pids.append(task_pid)

                return task_times, agent_pids, task_pids

            # first, run 5 jobs (with one task each) that requires 1 "foo". agent1
            # has 2 "foo"s, while agent2 doesn't have any
            task_times, agent_pids, task_pids = await run_jobs(
                {"foo": 1, LOGICAL_CPU: 0, MEMORY_GB: 0} for _ in range(5)
            )
            # So we should see 2 jobs running at a time:
            # TODO we should also be able to assert that e.g. task[2] starts as soon as
            # task[1] finishes, i.e. task_times[2] - task_times[1] < 1.25 (or some
            # tolerance), but right now our overhead appears to be 1s or more
            assert task_times[1] - task_times[0] < 0.25
            assert task_times[2] - task_times[1] > 1
            assert task_times[3] - task_times[2] < 0.25
            assert task_times[4] - task_times[3] > 1
            # and everything should have run on agent1:
            assert all(p == agent1_pid for p in agent_pids)
            # sanity check that each job ran in its own process
            assert len(set(task_pids)) == 5

            # next, run more "normal" jobs that just require 1 CPU
            task_times, agent_pids, task_pids = await run_jobs(
                {LOGICAL_CPU: 1, MEMORY_GB: 0} for _ in range(5)
            )
            # the first 4 tasks should get scheduled "at the same time", and then
            # the last one should get scheduled after those first 4 run
            assert task_times[3] - task_times[0] < 0.5
            assert task_times[4] - task_times[3] > 1
            # both agents should have been used
            assert agent1_pid in agent_pids and agent2_pid in agent_pids
            # sanity check that each job ran in its own process
            assert len(set(task_pids)) == 5
