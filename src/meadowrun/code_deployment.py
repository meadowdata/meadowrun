import asyncio.subprocess
import hashlib
import os
import pathlib
import shutil
import tempfile
from typing import List, Optional, Tuple, Sequence

import filelock

from meadowrun.credentials import RawCredentials, SshKey
from meadowrun.meadowrun_pb2 import Job

_GIT_REPO_URL_SUFFIXES_TO_REMOVE = [".git", "/"]


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


async def get_code_paths(
    git_repos_folder: str,
    local_copies_folder: str,
    job: Job,
    credentials: Optional[RawCredentials],
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
        return await _get_git_code_paths(
            git_repos_folder,
            local_copies_folder,
            job.git_repo_commit.repo_url,
            job.git_repo_commit.commit,
            job.git_repo_commit.path_in_repo,
            credentials,
        )
    elif case == "git_repo_branch":
        # warning this is not reproducible!!! should ideally be resolved on the
        # client
        return await _get_git_code_paths(
            git_repos_folder,
            local_copies_folder,
            job.git_repo_branch.repo_url,
            job.git_repo_branch.branch,
            job.git_repo_branch.path_in_repo,
            credentials,
        )
    else:
        raise ValueError(f"Unrecognized code_deployment {case}")


def _get_git_repo_local_clone_name(repo_url: str) -> str:
    """Gets a name to use for cloning the specified URL locally"""

    # try to normalize the url a little bit
    # TODO also consider .lower() in some cases?
    suffix_removed = True
    while suffix_removed:
        suffix_removed = False
        for s in _GIT_REPO_URL_SUFFIXES_TO_REMOVE:
            if repo_url.endswith(s):
                repo_url = repo_url[: -len(s)]
                suffix_removed = True

    return hashlib.blake2b(repo_url.encode("utf-8"), digest_size=16).hexdigest()


_DEFAULT_GIT_OPERATION_LOCK_TIMEOUT_SECS = 10 * 60


async def _get_git_code_paths(
    git_repos_folder: str,
    local_copies_folder: str,
    repo_url: str,
    revision_spec: str,
    path_in_repo: str,
    credentials: Optional[RawCredentials],
) -> Sequence[str]:
    """Returns code_paths for GitRepoCommit"""

    local_clone_name = _get_git_repo_local_clone_name(repo_url)
    with filelock.FileLock(
        os.path.join(git_repos_folder, f"{local_clone_name}.lock"),
        _DEFAULT_GIT_OPERATION_LOCK_TIMEOUT_SECS,
    ):
        # clone the repo locally/update it
        local_path = os.path.join(git_repos_folder, local_clone_name)

        if os.path.exists(local_path):
            _ = await _run_git(["fetch"], local_path, credentials)
        else:
            # TODO do something with output?
            temp_path = f"{local_path}_{os.getpid()}"
            _ = await _run_git(
                ["clone", repo_url, temp_path], git_repos_folder, credentials
            )
            os.rename(temp_path, local_path)

        # TODO we should (maybe) prevent very ambiguous specifications like HEAD
        out, err = await _run_git(["rev-parse", revision_spec], local_path, credentials)
        commit_hash = out.strip()
        # TODO do something with output?
        # get and checkout the specified hash
        _ = await _run_git(["checkout", commit_hash], local_path, credentials)

        # copy the specified version of this repo from local_path into
        # local_copy_path

        local_copy_path = os.path.join(
            local_copies_folder, local_clone_name + "_" + commit_hash
        )

        # it's important that it's impossible to create a scenario where different
        # local_clone.local_name + commit_hash can result in identical strings
        if not os.path.exists(local_copy_path):
            # TODO really we should do the whole thing where we hash each file/folder
            # and create symlinks so that we don't end up with tons of copies of
            # identical files
            shutil.copytree(local_path, local_copy_path)

        # TODO raise a friendlier exception if this path doesn't exist
        path_in_local_copy = str(
            (pathlib.Path(local_copy_path) / path_in_repo).resolve()
        )
        return [path_in_local_copy]
