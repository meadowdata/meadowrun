import os.path
import pathlib
import traceback
from typing import Dict, Optional
import subprocess
import string

import grpc.aio

import nextbeat.server.client
from nextrun.config import DEFAULT_PORT, DEFAULT_HOST
from nextrun.nextrun_pb2 import (
    ProcessState,
    RunPyFuncRequest,
    ProcessStatesRequest,
    ProcessStates,
)
from nextrun.nextrun_pb2_grpc import (
    NextRunServerServicer,
    add_NextRunServerServicer_to_server,
)


ProcessStateEnum = ProcessState.ProcessStateEnum


_REQUEST_ID_VALID_CHARS = set(string.ascii_letters + string.digits + "-_")


class NextRunServerHandler(NextRunServerServicer):
    def __init__(self, io_folder: str):
        self._io_folder = io_folder

        # TODO every update to this data should get persisted so that we don't run a
        #  process and then forget about it if this process crashes. (Although I think
        #  it's not possible to be airtight on that, but we can do a lot better than we
        #  do right now.)

        # TODO we should periodically populate _completed_process_states proactively
        #  rather than just waiting for requests, as the Popen objects don't seem to
        #  live forever (i.e. we shouldn't rely on the client to call get_process_state
        #  in a timely manner)

        # TODO we need to periodically clean up .argument and .result files when the
        #  processes complete

        # request_id -> process
        self._processes: Dict[str, subprocess.Popen] = {}
        # request_id -> output
        self._completed_process_states: Dict[str, ProcessState] = {}

    async def run_py_func(
        self, request: RunPyFuncRequest, context: grpc.aio.ServicerContext
    ) -> ProcessState:
        """
        See docstring on NextRunClientAsync.run_py_func for semantics.

        Roughly speaking, this function takes RunPyFuncRequest and runs it in a new
        python process by feeding it into __nextrun_func_runner.py.
        """

        # validate request_id

        if not request.request_id:
            raise ValueError("request_id must not be None or empty string")
        if any(c not in _REQUEST_ID_VALID_CHARS for c in request.request_id):
            raise ValueError(
                f"request_id {request.request_id} contains invalid characters. Only "
                "string.ascii_letters, numbers, -, and _ are permitted."
            )
        if request.request_id in self._processes:
            return ProcessState(state=ProcessStateEnum.REQUEST_IS_DUPLICATE)

        # TODO consider validating module_name and function_name if only to get
        #  better error messages

        # prepare paths for child process

        func_runner_path = str(
            (
                pathlib.Path(__file__).parent
                / "func_runner"
                / "__nextrun_func_runner.py"
            ).resolve()
        )

        if len(request.code_paths) == 0:
            raise ValueError("At least one code_path must be specified")
        working_directory = request.code_paths[0]

        environment = os.environ.copy()
        # we intentionally overwrite any existing PYTHONPATH--if for some reason we need
        # the current server process' code for the child process, the user needs to
        # include it directly
        environment["PYTHONPATH"] = ";".join(request.code_paths)

        # write function arguments to file

        if request.pickled_function_arguments is None:
            raise ValueError("argument cannot be None")
        argument_path = os.path.join(self._io_folder, request.request_id + ".argument")
        with open(argument_path, "wb") as f:
            f.write(request.pickled_function_arguments)

        # run the process

        print(
            f"Running process with: {request.interpreter_path} {func_runner_path} "
            f"{request.module_name} {request.function_name} {argument_path}; "
            f'cwd={working_directory}; PYTHONPATH={environment["PYTHONPATH"]}'
        )

        # TODO func_runner_path and argument_path need to be escaped properly on both
        #  Windows and Linux
        process = subprocess.Popen(
            [
                request.interpreter_path,
                func_runner_path,
                request.module_name,
                request.function_name,
                argument_path,
                str(request.result_highest_pickle_protocol),
            ],
            cwd=working_directory,
            env=environment,
        )
        self._processes[request.request_id] = process
        return ProcessState(state=ProcessStateEnum.RUNNING, pid=process.pid)

    async def get_process_states(
        self, request: ProcessStatesRequest, context: grpc.aio.ServicerContext
    ) -> ProcessState:
        """See docstring on NextRunClientAsync.get_process_states"""
        results = []
        for request_id in request.request_ids:
            try:
                results.append(self._get_process_state(request_id))
            except Exception:
                # TODO probably would be nice if we had a way to encode the error
                traceback.print_exc()
                results.append(ProcessState(state=ProcessStateEnum.ERROR_GETTING_STATE))
        return ProcessStates(process_states=results)

    def _get_process_state(self, request_id: str) -> ProcessState:
        """Gets the process state for a single process"""

        # first, see if we know about the request_id
        if request_id not in self._processes:
            # TODO add code for "reattaching" to child processes if the server process
            #  has died and recovered
            return ProcessState(state=ProcessStateEnum.UNKNOWN)

        # next, see if it is still running
        process = self._processes[request_id]
        return_code = process.poll()
        if return_code is None:
            return ProcessState(state=ProcessStateEnum.RUNNING, pid=process.pid)

        # see if we got a normal return code
        if return_code != 0:
            process_state = ProcessState(
                state=ProcessStateEnum.NON_ZERO_RETURN_CODE,
                pid=process.pid,
                return_code=return_code,
            )
            self._completed_process_states[request_id] = process_state
            return process_state

        # if we returned normally, return the state + result
        state_file = os.path.join(self._io_folder, request_id + ".state")
        with open(state_file, "r", encoding="utf-8") as f:
            state_string = f.read()
        if state_string == "SUCCEEDED":
            state = ProcessStateEnum.SUCCEEDED
        elif state_string == "PYTHON_EXCEPTION":
            state = ProcessStateEnum.PYTHON_EXCEPTION
        else:
            raise ValueError(f"Unknown state string: {state_string}")

        result_file = os.path.join(self._io_folder, request_id + ".result")
        with open(result_file, "rb") as f:
            result = f.read()

        process_state = ProcessState(
            state=state,
            pid=process.pid,
            return_code=0,
            pickled_result=result,
        )
        self._completed_process_states[request_id] = process_state
        return process_state


async def start_nextrun_server(
    io_folder,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    nextbeat_address: Optional[str] = None,
) -> None:
    """
    Runs the nextrun server

    If nextbeat_address is provided, this process will try to register itself with the
    nextbeat server at that address.

    io_folder must be read/write accessible for both the server and any child processes.
    It is used for communication with the child processes
    """

    # TODO io_folder should only be used by a single instance at a time. This will
    #  "usually" be enforced by the fact that the port can only be used by a single
    #  instance at a time, but we should put in code to protect against the case where
    #  we have multiple instances with different port numbers and the same io_folder

    server = grpc.aio.server()
    add_NextRunServerServicer_to_server(NextRunServerHandler(io_folder), server)
    nextrun_address = f"{host}:{port}"
    server.add_insecure_port(nextrun_address)
    await server.start()

    if nextbeat_address is not None:
        # TODO this is a little weird that we're taking a dependency on the nextbeat
        #  code
        async with nextbeat.server.client.NextBeatClientAsync(nextbeat_address) as c:
            await c.register_job_runner("nextrun", nextrun_address)

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Shuts down the server with 0 seconds of grace period. During the grace period,
        # the server won't accept new connections and allow existing RPCs to continue
        # within the grace period.
        await server.stop(0)
