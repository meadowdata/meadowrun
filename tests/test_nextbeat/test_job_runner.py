from typing import Any, Tuple
import uuid
from nextbeat.job_runner import LocalJobRunner
import time


def launch(*args: Any, **kwargs: Any) -> Tuple[int, int]:
    return len(args), len(kwargs)


def test_run() -> None:
    jr = LocalJobRunner()

    launch_id = uuid.uuid4()

    jr.run(launch_id, launch, 1, 2, 3, a=1, b=2, c=3)
    res = jr.poll_jobs([launch_id])

    assert launch_id in res
    while res[launch_id].outcome is "running":
        time.sleep(0.1)
        res = jr.poll_jobs([launch_id])
        assert launch_id in res

    assert res[launch_id].outcome == "succeeded"
    assert res[launch_id].result == (3, 3)
