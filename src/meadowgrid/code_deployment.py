import asyncio.subprocess
import dataclasses
import os
import pathlib
import shutil
import tempfile
from typing import Literal, List, Optional, Tuple, Dict, Sequence

from meadowgrid.credentials import RawCredentials, SshKey
from meadowgrid.meadowgrid_pb2 import Job

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


async def _run_git(
    args: List[str], cwd: str, credentials: Optional[RawCredentials]
) -> Tuple[str, str]:
    """Runs a git command in an external process. Returns stdout, stderr"""
    if credentials is None:
        p = await asyncio.create_subprocess_exec(
            # TODO make the location of the git executable configurable
            "git",
            *args,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await p.communicate()
    elif isinstance(credentials, SshKey):
        fd, filename = tempfile.mkstemp()
        try:
            os.write(fd, credentials.private_key.encode("utf-8"))
            os.close(fd)

            env = os.environ.copy()
            # TODO investigate rules for GIT_SSH_COMMAND escaping on Windows and Linux
            escaped_private_key_file = filename.replace("\\", "\\\\")
            # TODO perhaps warn if GIT_SSH_COMMAND is somehow already populated?
            env[
                "GIT_SSH_COMMAND"
            ] = f"ssh -i {escaped_private_key_file} -o IdentitiesOnly=yes"

            p = await asyncio.create_subprocess_exec(
                "git",
                *args,
                cwd=cwd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await p.communicate()
        finally:
            # TODO we should try even harder to delete the temporary file even if the
            #  process crashes before getting to this finally
            os.remove(filename)

    else:
        # TODO we should add support for username/password, e.g. from an OAuth flow
        raise ValueError(f"Unknown type of RawCredentials {type(credentials)}")

    if p.returncode != 0:
        raise ValueError(
            f"git {' '.join(args)} failed, return code {p.returncode}: "
            + stderr.decode()
        )

    # TODO lookup whether we should specify an encoding here?
    return stdout.decode(), stderr.decode()


class CodeDeploymentManager:
    """
    get_interpreter_and_code is this class' only public API. This is a class instead
    of a function because we need to cache data/folders for get_interpreter_and_code
    """

    def __init__(self, git_repos_folder: str, local_copies_folder: str):
        # this holds local versions of git repos
        self._git_repos_folder = git_repos_folder
        # this holds immutable local copies of a single version of a git repo (or other
        # sources of code)
        self._local_copies_folder = local_copies_folder

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

    async def get_code_paths(
        self, job: Job, credentials: Optional[RawCredentials]
    ) -> Sequence[str]:
        """
        Returns code_paths based on Job.code_deployment. code_paths will have paths to
        folders that are available on this machine that contain "the user's code". Code
        paths can be empty, which would mean that we don't need any user code.

        For ServerAvailableFolder, this function will effectively use code_paths as
        specified.

        For GitRepoCommit, this function will create an immutable local copy of the
        specific commit.
        """
        case = job.WhichOneof("code_deployment")
        if case == "server_available_folder":
            return job.server_available_folder.code_paths
        elif case == "git_repo_commit":
            return await self._get_git_code_paths(
                job.git_repo_commit.repo_url,
                job.git_repo_commit.commit,
                job.git_repo_commit.path_in_repo,
                credentials,
            )
        elif case == "git_repo_branch":
            # warning this is not reproducible!!! should ideally be resolved on the
            # client
            return await self._get_git_code_paths(
                job.git_repo_branch.repo_url,
                job.git_repo_branch.branch,
                job.git_repo_branch.path_in_repo,
                credentials,
            )
        else:
            raise ValueError(f"Unrecognized code_deployment {case}")

    async def _get_git_repo_local_clone(self, repo_url: str) -> _GitRepoLocalClone:
        """Clones the specified repo locally"""

        # try to normalize the url a little bit
        # TODO also consider .lower() in some cases?
        suffix_removed = True
        while suffix_removed:
            suffix_removed = False
            for s in _GIT_REPO_URL_SUFFIXES_TO_REMOVE:
                if repo_url.endswith(s):
                    repo_url = repo_url[: -len(s)]
                    suffix_removed = True

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

                last_slash = max(repo_url.rfind("/"), repo_url.rfind("\\"))
                local_folder_prefix = repo_url[last_slash + 1 :]

                # now add an integer suffix to make sure this is unique
                i = 0
                local_folder = f"{local_folder_prefix}_{i}"
                while (
                    # just in case our "human-ish" name collides with a different URL's
                    # human-ish name
                    any(
                        local_folder == local_clone.local_name
                        for local_clone in self._git_local_clones.values()
                    )
                    # TODO this is terrible--there shouldn't be folders we don't know
                    #  about lying around
                    or os.path.exists(
                        os.path.join(self._git_repos_folder, local_folder)
                    )
                ):
                    i += 1
                    local_folder = f"{local_folder_prefix}_{i}"
                local_clone = _GitRepoLocalClone(local_folder, asyncio.Lock(), "new")
                self._git_local_clones[repo_url] = local_clone

            return local_clone

    async def _get_git_code_paths(
        self,
        repo_url: str,
        revision_spec: str,
        path_in_repo: str,
        credentials: Optional[RawCredentials],
    ) -> Sequence[str]:
        """Returns code_paths for GitRepoCommit"""

        local_clone = await self._get_git_repo_local_clone(repo_url)

        async with local_clone.lock:
            # clone the repo locally/update it
            local_path = os.path.join(self._git_repos_folder, local_clone.local_name)
            if local_clone.state == "new":
                # TODO do something with output?
                _ = await _run_git(
                    ["clone", repo_url, local_path],
                    self._git_repos_folder,
                    credentials,
                )
                local_clone.state = "initialized"
            else:
                # TODO do something with output?
                _ = await _run_git(["fetch"], local_path, credentials)

            # TODO we should (maybe) prevent very ambiguous specifications like HEAD
            out, err = await _run_git(
                ["rev-parse", revision_spec], local_path, credentials
            )
            commit_hash = out.strip()
            # TODO do something with output?
            # get and checkout the specified hash
            _ = await _run_git(["checkout", commit_hash], local_path, credentials)

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

        # TODO raise a friendlier exception if this path doesn't exist
        path_in_local_copy = str(
            (pathlib.Path(local_copy_path) / path_in_repo).resolve()
        )
        return [path_in_local_copy]
