import asyncio.subprocess
import dataclasses
import os.path
import pathlib
import pickle
import shutil
import traceback
from typing import Dict, Optional, Tuple, List, Literal, Union
import subprocess
import string

import grpc.aio

import meadowflow.server.client
import meadowflow.context
from meadowrun.config import DEFAULT_PORT, DEFAULT_HOST
from meadowrun.meadowrun_pb2 import (
    ProcessState,
    RunPyFuncRequest,
    ProcessStatesRequest,
    ProcessStates,
    GitRepoCommit,
    RunPyCommandRequest,
)
from meadowrun.meadowrun_pb2_grpc import (
    MeadowRunServerServicer,
    add_MeadowRunServerServicer_to_server,
)


ProcessStateEnum = ProcessState.ProcessStateEnum


_REQUEST_ID_VALID_CHARS = set(string.ascii_letters + string.digits + "-_.")


_GIT_REPO_URL_SUFFIXES_TO_REMOVE = [".git", "/"]


_FUNC_RUNNER_PATH = str(
    (
        pathlib.Path(__file__).parent / "func_runner" / "__meadowrun_func_runner.py"
    ).resolve()
)


def _pickle_exception(e: Exception, pickle_protocol: int) -> bytes:
    """
    We generally don't want to pickle exceptions directly--there's no guarantee that a
    random exception that was thrown can be unpickled in a different process.
    """
    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return pickle.dumps(
        (str(type(e)), str(e), tb),
        protocol=pickle_protocol,
    )


@dataclasses.dataclass
class _GitRepoLocalClone:
    """This represents the local clone of a git repo"""

    local_name: str

    # any changes to the state field or manipulations of the on-disk git repo must be
    # done while holding this lock
    lock: asyncio.Lock

    # this should be the only mutable field
    state: Literal[
        # brand new repo, folder has not even been created yet
        "new",
        # TODO more states?
        "initialized",
    ]


async def _run_git(args: List[str], cwd: Optional[str] = None) -> Tuple[str, str]:
    """Runs a git command in an external process. Returns stdout, stderr"""
    p = await asyncio.create_subprocess_exec(
        # TODO make the location of the git executable configurable
        "git",
        *args,
        cwd=cwd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await p.communicate()

    if p.returncode != 0:
        raise ValueError(
            f"git {' '.join(args)} failed, return code {p.returncode}: "
            + stderr.decode()
        )

    # TODO lookup whether we should specify an encoding here?
    return stdout.decode(), stderr.decode()


@dataclasses.dataclass(frozen=True)
class _ProcessHandle:
    """Represents a running child process"""

    # What kind of process this represents
    process_type: Literal["py_func", "py_command"]
    # The (potentially) running process
    process: subprocess.Popen
    # The log file for the process
    log_file_name: str


class MeadowRunServerHandler(MeadowRunServerServicer):
    def __init__(self, working_folder: str):
        # this holds files for transferring data to and from this server process and the
        # child processes
        self._io_folder = os.path.join(working_folder, "io")
        # this holds local versions of git repos
        self._git_repos_folder = os.path.join(working_folder, "git_repos")
        # this holds immutable local copies of a single version of a git repo (or other
        # sources of code)
        self._local_copies_folder = os.path.join(working_folder, "local_copies")
        # holds the logs for the functions/commands that this server runs
        self._job_logs_folder = os.path.join(working_folder, "job_logs")

        os.makedirs(self._io_folder, exist_ok=True)
        os.makedirs(self._git_repos_folder, exist_ok=True)
        os.makedirs(self._local_copies_folder, exist_ok=True)
        os.makedirs(self._job_logs_folder, exist_ok=True)

        # Maps from remote git url to _GitRepoLocalClone. Note that we need to hold onto
        # the full original url to to avoid collisions between e.g.
        # https://github.com/foo/bar.git and https://github.com/baz/bar.git
        # TODO This means that multiple URLs that point to the same repo "in reality"
        #  will be treated as different, which we should try to dedupe.
        # TODO This data needs to be persisted so that we don't forget about clones
        #  between restarts of this server
        # TODO we need to periodically clean up the local clones of these git repos if
        #  they haven't been used in a while, as well as the local copies of specific
        #  versions of the git repos
        self._git_local_clones: Dict[str, _GitRepoLocalClone] = {}
        # This lock must be taken before modifying the _git_local_clones dictionary
        # (each local clone has its own lock separately for doing operations just on
        # that local clone)
        self._git_local_clones_lock = asyncio.Lock()

        # TODO every update to this data should get persisted so that we don't run a
        #  process and then forget about it if this process crashes. (Although I think
        #  it's not possible to be airtight on that, but we can do a lot better than we
        #  do right now.)

        # TODO we should periodically populate _completed_process_states proactively
        #  rather than just waiting for requests, as the Popen objects don't seem to
        #  live forever (i.e. we shouldn't rely on the client to call get_process_state
        #  in a timely manner)
        # TODO we should periodically clean up these handles/outputs so they don't grow
        #  without bound
        # request_id -> ProcessHandle. None means that we're in the process of
        # constructing the process
        self._processes: Dict[str, Optional[_ProcessHandle]] = {}
        # request_id -> output
        self._completed_process_states: Dict[str, ProcessState] = {}

        # TODO we need to periodically clean up .argument and .result files when the
        #  processes complete

    def _validate_request_id(
        self, request: Union[RunPyFuncRequest, RunPyCommandRequest]
    ) -> Optional[ProcessState]:
        """
        Returns None if the request_id is valid. If the request_id is a duplicate,
        returns a ProcessState indicating that that should be returned by the calling
        function. If the request_id is invalid, raises an exception.
        """
        if not request.request_id:
            raise ValueError("request_id must not be None or empty string")
        if any(c not in _REQUEST_ID_VALID_CHARS for c in request.request_id):
            raise ValueError(
                f"request_id {request.request_id} contains invalid characters. Only "
                "string.ascii_letters, numbers, -, and _ are permitted."
            )
        if request.request_id in self._processes:
            return ProcessState(state=ProcessStateEnum.REQUEST_IS_DUPLICATE)
        else:
            return None

    async def run_py_command(
        self, request: RunPyCommandRequest, context: grpc.aio.ServicerContext
    ) -> ProcessState:
        """See docstring on MeadowRunClientAsync for semantics."""

        result = self._validate_request_id(request)
        if result is not None:
            return result

        # we'll replace the None later with a handle to our subprocess
        self._processes[request.request_id] = None

        try:
            # prepare paths for child process

            interpreter_path, code_paths = await self._get_interpreter_and_code(request)
            working_directory = code_paths[0]

            environment = os.environ.copy()
            # we intentionally overwrite any existing PYTHONPATH--if for some reason we
            # need the current server process' code for the child process, the user
            # needs to include it directly
            environment["PYTHONPATH"] = ";".join(code_paths)

            # We believe that interpreter_path can be one of two formats,
            # python_or_venv_dir/python or python_or_venv_dir/Scripts/python. We need to
            # add the scripts directory to the path so that we can run executables as if
            # we're "in the python environment".
            interpreter_path = pathlib.Path(interpreter_path)
            if interpreter_path.parent.name == "Scripts":
                scripts_dir = str(interpreter_path.parent.resolve())
            else:
                scripts_dir = str((interpreter_path.parent / "Scripts").resolve())
            environment["PATH"] = scripts_dir + ";" + environment["PATH"]

            log_file_name = os.path.join(
                self._job_logs_folder,
                f"{request.log_file_name}.{request.request_id}.log",
            )

            # request the results file

            environment[meadowflow.context._MEADOWRUN_RESULT_FILE] = os.path.join(
                self._io_folder, request.request_id + ".result"
            )
            environment[meadowflow.context._MEADOWRUN_RESULT_PICKLE_PROTOCOL] = str(
                request.result_highest_pickle_protocol
            )

            # write context variables to file

            if request.pickled_context_variables:
                context_variables_path = os.path.join(
                    self._io_folder, request.request_id + ".context_variables"
                )
                with open(context_variables_path, "wb") as f:
                    f.write(request.pickled_context_variables)
                # we can't communicate "directly" with the arbitrary command that the
                # user is running so we'll use environment variables
                environment[
                    meadowflow.context._MEADOWRUN_CONTEXT_VARIABLES
                ] = context_variables_path

            # run the process

            if not request.command_line:
                raise ValueError("command_line must have at least one string")

            # Popen uses cwd and env to search for the specified command on Linux but
            # not on Windows according to the docs:
            # https://docs.python.org/3/library/subprocess.html#subprocess.Popen
            # We can use shutil to make the behavior more similar on both platforms
            new_first_command_line = shutil.which(
                request.command_line[0],
                path=f"{working_directory};{environment['PATH']}",
            )
            if new_first_command_line:
                command_line = [new_first_command_line] + request.command_line[1:]
            else:
                command_line = request.command_line

            print(
                f"Running process: {' '.join(command_line)}; cwd={working_directory}; "
                f"added {scripts_dir} to PATH; PYTHONPATH={environment['PYTHONPATH']}; "
                f"log_file_name={log_file_name}"
            )

            with open(log_file_name, "w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    command_line,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=working_directory,
                    env=environment,
                )
        except Exception as e:
            # we failed to launch the process
            process_state = ProcessState(
                state=ProcessStateEnum.RUN_REQUEST_FAILED,
                pickled_result=_pickle_exception(
                    e, request.result_highest_pickle_protocol
                ),
            )
            # leave self._processes as None, just update _completed_process_states
            self._completed_process_states[request.request_id] = process_state
            return process_state
        else:
            self._processes[request.request_id] = _ProcessHandle(
                "py_command", process, log_file_name
            )
            return ProcessState(
                state=ProcessStateEnum.RUNNING,
                pid=process.pid,
                log_file_name=log_file_name,
            )

    async def run_py_func(
        self, request: RunPyFuncRequest, context: grpc.aio.ServicerContext
    ) -> ProcessState:
        """
        See docstring on MeadowRunClientAsync.run_py_func for semantics.

        Roughly speaking, this function takes RunPyFuncRequest and runs it in a new
        python process by feeding it into __meadowrun_func_runner.py.
        """

        result = self._validate_request_id(request)
        if result is not None:
            return result

        # we'll replace the None later with a handle to our subprocess
        self._processes[request.request_id] = None

        try:
            # TODO consider validating module_name and function_name if only to get
            #  better error messages

            # prepare paths for child process

            interpreter_path, code_paths = await self._get_interpreter_and_code(request)
            working_directory = code_paths[0]

            environment = os.environ.copy()
            # we intentionally overwrite any existing PYTHONPATH--if for some reason we
            # need the current server process' code for the child process, the user
            # needs to include it directly
            environment["PYTHONPATH"] = ";".join(code_paths)

            log_file_name = os.path.join(
                self._job_logs_folder,
                f"{request.log_file_name}.{request.request_id}.log",
            )

            # write function arguments to file

            if request.pickled_function_arguments is None:
                raise ValueError("argument cannot be None")
            argument_path = os.path.join(
                self._io_folder, request.request_id + ".argument"
            )
            with open(argument_path, "wb") as f:
                f.write(request.pickled_function_arguments)

            # run the process

            print(
                f"Running process with: {interpreter_path} {_FUNC_RUNNER_PATH} "
                f"{request.module_name} {request.function_name} {argument_path}; "
                f"cwd={working_directory}; PYTHONPATH={environment['PYTHONPATH']} "
                f"log={log_file_name}"
            )

            # TODO func_runner_path and argument_path need to be escaped properly on
            #  both Windows and Linux
            # this isn't documented per se, but closing the file handle for the log file
            # doesn't affect the subprocess being able to write to it
            with open(log_file_name, "w", encoding="utf-8") as log_file:
                process = subprocess.Popen(
                    [
                        interpreter_path,
                        _FUNC_RUNNER_PATH,
                        request.module_name,
                        request.function_name,
                        argument_path,
                        str(request.result_highest_pickle_protocol),
                    ],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    cwd=working_directory,
                    env=environment,
                )

        except Exception as e:
            # we failed to launch the process
            process_state = ProcessState(
                state=ProcessStateEnum.RUN_REQUEST_FAILED,
                pickled_result=_pickle_exception(
                    e, request.result_highest_pickle_protocol
                ),
            )
            # leave self._processes as None, just update _completed_process_states
            self._completed_process_states[request.request_id] = process_state
            return process_state
        else:
            self._processes[request.request_id] = _ProcessHandle(
                "py_func", process, log_file_name
            )
            return ProcessState(
                state=ProcessStateEnum.RUNNING,
                pid=process.pid,
                log_file_name=log_file_name,
            )

    async def _get_interpreter_and_code(
        self, request: RunPyFuncRequest
    ) -> Tuple[str, List[str]]:
        """
        Returns interpreter_path, code_paths. code_paths will have at least one element
        """
        case = request.WhichOneof("deployment")
        if case == "server_available_folder":
            r = request.server_available_folder
            if len(r.code_paths) == 0:
                raise ValueError(
                    "At least one server_available_folder.code_path must be specified"
                )
            return r.interpreter_path, r.code_paths
        elif case == "git_repo_commit":
            return await self._get_git_repo_commit_interpreter_and_code(
                request.git_repo_commit
            )
        elif case is None:
            raise ValueError("One of interpreter_and_code must be set!")
        else:
            raise ValueError(f"Unrecognized interpreter_and_code {case}")

    async def _get_git_repo_local_clone(self, repo_url: str) -> _GitRepoLocalClone:
        """Clones the specified repo locally"""

        async with self._git_local_clones_lock:
            if repo_url in self._git_local_clones:
                local_clone = self._git_local_clones[repo_url]
            else:
                # This tries to replicate git's behavior on clone from
                # https://git-scm.com/docs/git-clone:
                #
                # The "humanish" part of the source repository is used if no directory
                # is explicitly given (repo for /path/to/repo.git and foo for
                # host.xz:foo/.git)
                #
                # This doesn't "really" matter, as this folder name is not usually
                # exposed to the user
                #
                # TODO probably worth looking at git source code to figure out the exact
                #  semantics and avoid failures due to invalid paths especially on
                #  Windows
                suffix_removed = True
                while suffix_removed:
                    suffix_removed = False
                    for s in _GIT_REPO_URL_SUFFIXES_TO_REMOVE:
                        if repo_url.endswith(s):
                            repo_url = repo_url[: -len(s)]
                            suffix_removed = True

                last_slash = max(repo_url.rfind("/"), repo_url.rfind("\\"))
                local_folder_prefix = repo_url[last_slash + 1 :]

                # now add an integer suffix to make sure this is unique
                i = 0
                while (
                    f"{local_folder_prefix}_{i}" in self._git_local_clones.values()
                    # TODO this is terrible--there shouldn't be folders we don't know
                    #  about lying around
                    or os.path.exists(
                        os.path.join(
                            self._git_repos_folder, f"{local_folder_prefix}_{i}"
                        )
                    )
                ):
                    i += 1
                local_clone = _GitRepoLocalClone(
                    f"{local_folder_prefix}_{i}", asyncio.Lock(), "new"
                )
                self._git_local_clones[repo_url] = local_clone

            return local_clone

    async def _get_git_repo_commit_interpreter_and_code(
        self, git_repo_commit: GitRepoCommit
    ) -> Tuple[str, List[str]]:
        """Returns interpreter_path, code_paths"""

        local_clone = await self._get_git_repo_local_clone(git_repo_commit.repo_url)

        async with local_clone.lock:
            # clone the repo locally/update it
            local_path = os.path.join(self._git_repos_folder, local_clone.local_name)
            if local_clone.state == "new":
                # TODO do something with output?
                _ = await _run_git(["clone", git_repo_commit.repo_url, local_path])
                local_clone.state = "initialized"
            else:
                # TODO do something with output?
                _ = await _run_git(["fetch"], cwd=local_path)

            # TODO we should (maybe) prevent very ambiguous specifications like HEAD
            out, err = await _run_git(
                ["rev-parse", git_repo_commit.commit], cwd=local_path
            )
            commit_hash = out.strip()
            # TODO do something with output?
            # get and checkout the specified hash
            _ = await _run_git(["checkout", commit_hash], cwd=local_path)

            # copy the specified version of this repo from local_path into
            # local_copy_path

            local_copy_path = os.path.join(
                self._local_copies_folder, local_clone.local_name + "_" + commit_hash
            )
            # it's important that it's impossible to create a scenario where
            # different local_clone.local_name + commit_hash can result in identical
            # strings
            if not os.path.exists(local_copy_path):
                # TODO really we should do the whole thing where we hash each
                #  file/folder and create symlinks so that we don't end up with tons of
                #  copies of identical files
                shutil.copytree(local_path, local_copy_path)

        return git_repo_commit.interpreter_path, [local_copy_path]

    async def get_process_states(
        self, request: ProcessStatesRequest, context: grpc.aio.ServicerContext
    ) -> ProcessState:
        """See docstring on MeadowRunClientAsync.get_process_states"""
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

        # next, see if we're still in the process of constructing the process
        process = self._processes[request_id]
        if process is None:
            # check if we've failed to launch
            if request_id in self._completed_process_states:
                # this should always be RUN_REQUEST_FAILED
                return self._completed_process_states[request_id]
            else:
                # we're still trying to start the process
                return ProcessState(state=ProcessStateEnum.RUN_REQUESTED)

        # next, see if we have launched the process yet
        return_code = process.process.poll()
        if return_code is None:
            return ProcessState(
                state=ProcessStateEnum.RUNNING,
                pid=process.process.pid,
                log_file_name=process.log_file_name,
            )

        # see if we got a normal return code
        if return_code != 0:
            process_state = ProcessState(
                state=ProcessStateEnum.NON_ZERO_RETURN_CODE,
                pid=process.process.pid,
                log_file_name=process.log_file_name,
                return_code=return_code,
            )
            self._completed_process_states[request_id] = process_state
            return process_state

        # if we returned normally

        # for py_funcs, get the state
        if process.process_type == "py_func":
            state_file = os.path.join(self._io_folder, request_id + ".state")
            with open(state_file, "r", encoding="utf-8") as f:
                state_string = f.read()
            if state_string == "SUCCEEDED":
                state = ProcessStateEnum.SUCCEEDED
            elif state_string == "PYTHON_EXCEPTION":
                state = ProcessStateEnum.PYTHON_EXCEPTION
            else:
                raise ValueError(f"Unknown state string: {state_string}")
        elif process.process_type == "py_command":
            state = ProcessStateEnum.SUCCEEDED
        else:
            raise ValueError(f"process_type was not recognized {process.process_type}")

        # Next get the result. The result file is optional for py_commands because we
        # don't have full control over the process and there's no way to guarantee that
        # "our code" gets executed
        result_file = os.path.join(self._io_folder, request_id + ".result")
        if process.process_type != "py_command" or os.path.exists(result_file):
            with open(result_file, "rb") as f:
                result = f.read()
        else:
            result = None

        process_state = ProcessState(
            state=state,
            pid=process.process.pid,
            log_file_name=process.log_file_name,
            return_code=0,
            pickled_result=result,
        )

        self._completed_process_states[request_id] = process_state
        return process_state


async def start_meadowrun_server(
    working_folder,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    meadowflow_address: Optional[str] = None,
) -> None:
    """
    Runs the meadowrun server

    If meadowflow_address is provided, this process will try to register itself with the
    meadowflow server at that address.

    working_folder must be read/write accessible for both the server and any child
    processes. It is used (among other things) for communication with the child
    processes
    """

    # TODO working_folder should only be used by a single instance at a time. This will
    #  "usually" be enforced by the fact that the port can only be used by a single
    #  instance at a time, but we should put in code to protect against the case where
    #  we have multiple instances with different port numbers and the same
    #  working_folder

    server = grpc.aio.server()
    add_MeadowRunServerServicer_to_server(
        MeadowRunServerHandler(working_folder), server
    )
    meadowrun_address = f"{host}:{port}"
    server.add_insecure_port(meadowrun_address)
    await server.start()

    if meadowflow_address is not None:
        # TODO this is a little weird that we're taking a dependency on the meadowflow
        #  code
        async with meadowflow.server.client.MeadowFlowClientAsync(
            meadowflow_address
        ) as c:
            await c.register_job_runner("meadowrun", meadowrun_address)

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Shuts down the server with 0 seconds of grace period. During the grace period,
        # the server won't accept new connections and allow existing RPCs to continue
        # within the grace period.
        await server.stop(0)
