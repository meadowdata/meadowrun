"""Lots of similarities with __meadowgrid_func_worker.py"""

import argparse
import functools
import importlib
import os
import pickle
import traceback
from typing import Optional, Tuple, Any, Callable, Union

from meadowgrid.coordinator_client import MeadowGridCoordinatorClientForWorkersSync
from meadowgrid.meadowgrid_pb2 import ProcessState
from meadowgrid.shared import pickle_exception


def _main_loop(
    coordinator_address: str,
    job_id: str,
    grid_worker_id: str,
    module_function_or_pickled_function: Union[Tuple[str, str], str],
    pickled_arguments_path: Optional[str],
) -> None:
    """
    Requests tasks from the coordinator for the specified job_id, runs them, and sends
    results back to the coordinator.

    Exactly one of module_name with function_name OR pickled_function_path must be not
    None. We don't check this in this function, it should be checked in the caller.

    If arguments are specified, they will be partially applied to the function
    before applying the task arguments.
    """

    pid = os.getpid()

    function: Optional[Callable] = None
    # (task_id, process_state)
    previous_task_state: Optional[Tuple[int, Any]] = None

    client = MeadowGridCoordinatorClientForWorkersSync(coordinator_address)

    while True:
        next_task = client.update_grid_task_state_and_get_next(
            job_id, grid_worker_id, previous_task_state
        )

        # task_id == -1 means the coordinator has no more tasks for this grid job
        if next_task.task_id == -1:
            break

        try:
            # populate function the first time through the loop
            if function is None:
                if isinstance(module_function_or_pickled_function, tuple):
                    module_name, function_name = module_function_or_pickled_function
                    print("About to import {module_name}.{function_name}")
                    module = importlib.import_module(module_name)
                    function = getattr(module, function_name)
                    print(f"Imported {function_name} from {module.__file__}")
                else:
                    pickled_function_path = module_function_or_pickled_function
                    print(f"Unpickling function from {pickled_function_path}")
                    with open(pickled_function_path, "rb") as f:
                        function = pickle.load(f)

                if pickled_arguments_path:
                    print("Unpickling partial function arguments")
                    with open(pickled_arguments_path, "rb") as f:
                        args, kwargs = pickle.load(f)

                    function = functools.partial(function, *args, **kwargs)

            task_args, task_kwargs = pickle.loads(next_task.pickled_function_arguments)

            # run the function
            result = function(*task_args, **task_kwargs)
        except Exception as e:
            # first print the exception for the local log file
            traceback.print_exc()

            # then populate process state
            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.PYTHON_EXCEPTION,
                pid=pid,
                # TODO populate log_file_name
                pickled_result=pickle_exception(e, pickle.HIGHEST_PROTOCOL),
                return_code=0,
            )
        else:
            # get effects, make sure we don't fail if meadowflow is not available
            try:
                import meadowflow.effects
            except ModuleNotFoundError:
                effects = None
            else:
                effects = meadowflow.effects.get_effects()

            process_state = ProcessState(
                state=ProcessState.ProcessStateEnum.SUCCEEDED,
                pid=pid,
                # TODO populate log_file_name
                pickled_result=pickle.dumps(
                    (result, effects), protocol=pickle.HIGHEST_PROTOCOL
                ),
                return_code=0,
            )

        # prepare for the next iteration through the loop
        previous_task_state = next_task.task_id, process_state


def main() -> None:
    usage = (
        "Runs a grid worker which requests grid tasks for the specified job id from the"
        " specified coordinator. The function for the grid job can be defined by "
        "--module-name and --function-name together, or by --has-pickled-function and "
        "--io-path. Also, if --has-pickled-arguments is set, the specified arguments "
        "will be partially applied to the function before the task-specific arguments."
    )
    parser = argparse.ArgumentParser(usage)
    parser.add_argument("--coordinator-address", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--grid-worker-id", required=True)
    parser.add_argument("--module-name")
    parser.add_argument("--function-name")
    # --io-path is not technically required if neither --has-pickled-function nor
    # --has-pickled-arguments is set, but simpler to just make it required
    parser.add_argument("--io-path", required=True)
    parser.add_argument("--has-pickled-function", action="store_true")
    parser.add_argument("--has-pickled-arguments", action="store_true")

    args = parser.parse_args()

    if bool(args.module_name) ^ bool(args.function_name):
        raise ValueError(
            "Cannot specify just one of --module-name and --function-name without the "
            "other"
        )
    if not (bool(args.module_name) ^ args.has_pickled_function):
        raise ValueError(
            "Must specify either --module-name with --function-name OR "
            "--has-pickled-function but not both"
        )
    function = (
        args.io_path + ".function"
        if args.has_pickled_function
        else (args.module_name, args.function_name)
    )
    _main_loop(
        args.coordinator_address,
        args.job_id,
        args.grid_worker_id,
        function,
        args.io_path + ".arguments" if args.has_pickled_arguments else None,
    )


if __name__ == "__main__":
    main()
