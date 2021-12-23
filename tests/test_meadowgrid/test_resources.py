import asyncio
import os
import time
from typing import Tuple, Dict, Iterable, List

import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
from meadowgrid import ServerAvailableInterpreter, ServerAvailableFolder
from meadowgrid.config import (
    LOGICAL_CPU,
    MEADOWGRID_INTERPRETER,
    MEADOWGRID_JOB_WORKER_PID,
    MEMORY_GB,
)
from meadowgrid.grid import grid_map_async
from test_meadowgrid.test_meadowgrid_basics import TEST_WORKING_FOLDER, MEADOWDATA_CODE


def test_resources():
    # make this a local function so that it just gets cloudpickled and we don't have to
    # worry about getting the code over to the executor
    def get_time_and_pid(x: float) -> Tuple[float, int, int]:
        t0 = time.time()
        time.sleep(x)
        results = t0, int(os.environ[MEADOWGRID_JOB_WORKER_PID]), os.getpid()
        return results

    # event_loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(event_loop)

    with meadowgrid.coordinator_main.main_in_child_process():
        # create 2 workers that each think they have 2 logical CPUs available. Also,
        # worker1 has 2 "foo" resources available
        with meadowgrid.job_worker_main.main_in_child_process(
            TEST_WORKING_FOLDER, {"foo": 2, LOGICAL_CPU: 2}
        ) as worker1_pid, meadowgrid.job_worker_main.main_in_child_process(
            TEST_WORKING_FOLDER + "2", {LOGICAL_CPU: 2}
        ) as worker2_pid:

            t0 = time.time()

            async def run_job(
                resources_required_per_task: Dict[str, float]
            ) -> Tuple[float, int, int]:
                """
                Run one job that runs just one task which will run for 1 second, and
                returns the time the task started, the pid of the job worker that
                launched the task/job, and the pid of the task/job process itself
                """
                tasks = await grid_map_async(
                    get_time_and_pid,
                    [1],
                    ServerAvailableInterpreter(interpreter_path=MEADOWGRID_INTERPRETER),
                    ServerAvailableFolder(code_paths=[MEADOWDATA_CODE]),
                    resources_required_per_task=resources_required_per_task,
                )

                t1, worker_pid, task_pid = await tasks[0]
                return t1 - t0, worker_pid, task_pid

            async def run_jobs(
                resources_required: Iterable[Dict[str, float]]
            ) -> Tuple[List[float], List[int], List[int]]:
                """
                Effectively calls run_job for each of resources_required, and then
                splits the output into a list of task start times, a list of job worker
                pids, and a list of job/task pids. The outputs are sorted by the task
                start times.
                """
                results = await asyncio.gather(
                    *(run_job(r) for r in resources_required)
                )

                task_times = []
                worker_pids = []
                task_pids = []

                for task_time, worker_pid, task_pid in sorted(
                    results, key=lambda r: r[0]
                ):
                    task_times.append(task_time)
                    worker_pids.append(worker_pid)
                    task_pids.append(task_pid)

                return task_times, worker_pids, task_pids

            async def run_test():
                # first, run 5 jobs (with one task each) that requires 1 "foo". worker1
                # has 2 "foo"s, while worker2 doesn't have any
                task_times, worker_pids, task_pids = await run_jobs(
                    {"foo": 1, LOGICAL_CPU: 0, MEMORY_GB: 0} for _ in range(5)
                )
                # So we should see 2 jobs running at a time:
                # TODO we should also be able to assert that e.g. task[2] starts as soon
                # as task[1] finishes, i.e. task_times[2] - task_times[1] < 1.25 (or
                # some tolerance), but right now our overhead appears to be 1s or more
                assert task_times[1] - task_times[0] < 0.25
                assert task_times[2] - task_times[1] > 1
                assert task_times[3] - task_times[2] < 0.25
                assert task_times[4] - task_times[3] > 1
                # and everything should have run on worker1:
                assert all(p == worker1_pid for p in worker_pids)
                # sanity check that each job ran in its own process
                assert len(set(task_pids)) == 5

                # next, run more "normal" jobs that just require 1 CPU
                task_times, worker_pids, task_pids = await run_jobs(
                    {LOGICAL_CPU: 1, MEMORY_GB: 0} for _ in range(5)
                )
                # the first 4 tasks should get scheduled "at the same time", and then
                # the last one should get scheduled after those first 4 run
                assert task_times[3] - task_times[0] < 0.5
                assert task_times[4] - task_times[3] > 1
                # both workers should have been used
                assert worker1_pid in worker_pids and worker2_pid in worker_pids
                # sanity check that each job ran in its own process
                assert len(set(task_pids)) == 5

            # event_loop.run_until_complete(run_test())
            asyncio.run(run_test())
