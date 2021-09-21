import asyncio.subprocess
import dataclasses
import os.path
import pathlib
import shutil
import traceback
from typing import Dict, Optional, Tuple, List, Literal
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
    GitRepoCommit,
)
from nextrun.nextrun_pb2_grpc import (
    NextRunServerServicer,
    add_NextRunServerServicer_to_server,
)


ProcessStateEnum = ProcessState.ProcessStateEnum


_REQUEST_ID_VALID_CHARS = set(string.ascii_letters + string.digits + "-_")


_GIT_REPO_URL_SUFFIXES_TO_REMOVE = [".git", "/"]


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


class NextRunServerHandler(NextRunServerServicer):
    def __init__(self, working_folder: str):
        # this holds files for transferring data to and from this server process and the
        # child processes
        self._io_folder = os.path.join(working_folder, "io")
        # this holds local versions of git repos
        self._git_repos_folder = os.path.join(working_folder, "git_repos")
        # this holds immutable local copies of a single version of a git repo (or other
        # sources of code)
        self._local_copies_folder = os.path.join(working_folder, "local_copies")

        os.makedirs(self._io_folder, exist_ok=True)
        os.makedirs(self._git_repos_folder, exist_ok=True)
        os.makedirs(self._local_copies_folder, exist_ok=True)

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
        # request_id -> process. None means that we're in the process of constructing
        # the process
        self._processes: Dict[str, Optional[subprocess.Popen]] = {}
        # request_id -> output
        self._completed_process_states: Dict[str, ProcessState] = {}

        # TODO we need to periodically clean up .argument and .result files when the
        #  processes complete

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

        # we'll replace the None later with a handle to our subprocess
        self._processes[request.request_id] = None

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

        interpreter_path, code_paths = await self._get_interpreter_and_code(request)
        working_directory = code_paths[0]

        environment = os.environ.copy()
        # we intentionally overwrite any existing PYTHONPATH--if for some reason we need
        # the current server process' code for the child process, the user needs to
        # include it directly
        environment["PYTHONPATH"] = ";".join(code_paths)

        # write function arguments to file

        if request.pickled_function_arguments is None:
            raise ValueError("argument cannot be None")
        argument_path = os.path.join(self._io_folder, request.request_id + ".argument")
        with open(argument_path, "wb") as f:
            f.write(request.pickled_function_arguments)

        # run the process

        print(
            f"Running process with: {interpreter_path} {func_runner_path} "
            f"{request.module_name} {request.function_name} {argument_path}; "
            f'cwd={working_directory}; PYTHONPATH={environment["PYTHONPATH"]}'
        )

        # TODO func_runner_path and argument_path need to be escaped properly on both
        #  Windows and Linux
        process = subprocess.Popen(
            [
                interpreter_path,
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

    async def _get_interpreter_and_code(
        self, request: RunPyFuncRequest
    ) -> Tuple[str, List[str]]:
        """Returns interpreter_path, code_paths"""
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

        # next, see if we're still in the process of constructing the process
        process = self._processes[request_id]
        if process is None:
            return ProcessState(state=ProcessStateEnum.RUN_REQUESTED)

        # next, see if it is still running
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
    working_folder,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    nextbeat_address: Optional[str] = None,
) -> None:
    """
    Runs the nextrun server

    If nextbeat_address is provided, this process will try to register itself with the
    nextbeat server at that address.

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
    add_NextRunServerServicer_to_server(NextRunServerHandler(working_folder), server)
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
