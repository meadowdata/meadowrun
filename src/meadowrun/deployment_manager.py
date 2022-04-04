"""Manages local deployments for run_job_local"""

import asyncio.subprocess
import hashlib
import os
import shutil
import tempfile
from typing import List, Optional, Tuple, Sequence

import filelock

from meadowrun._vendor.aiodocker import exceptions as aiodocker_exceptions
from meadowrun.aws_integration.aws_core import _get_default_region_name
from meadowrun.aws_integration.ecr import (
    does_image_exist,
    ensure_repository,
    get_username_password,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _MEADOWRUN_GENERATED_DOCKER_REPO,
)
from meadowrun.credentials import RawCredentials, SshKey
from meadowrun.docker_controller import (
    build_image,
    push_image,
    pull_image,
    _does_digest_exist_locally,
)
from meadowrun.meadowrun_pb2 import (
    EnvironmentSpecInCode,
    Job,
    ServerAvailableContainer,
)

_GIT_REPO_URL_SUFFIXES_TO_REMOVE = [".git", "/"]


async def _run_git(
    args: List[str], cwd: str, credentials: Optional[RawCredentials]
) -> Tuple[str, str]:
    """Runs a git command in an external process. Returns stdout, stderr"""
    env = os.environ.copy()

    if credentials is None:
        env["GIT_SSH_COMMAND"] = "ssh -o StrictHostKeyChecking=no"

        p = await asyncio.create_subprocess_exec(
            # TODO make the location of the git executable configurable
            "git",
            *args,
            cwd=cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await p.communicate()
    elif isinstance(credentials, SshKey):
        fd, filename = tempfile.mkstemp()
        try:
            os.write(fd, credentials.private_key.encode("utf-8"))
            os.close(fd)

            # TODO investigate rules for GIT_SSH_COMMAND escaping on Windows and Linux
            escaped_private_key_file = filename.replace("\\", "\\\\")
            # TODO perhaps warn if GIT_SSH_COMMAND is somehow already populated?
            env["GIT_SSH_COMMAND"] = (
                f"ssh -i {escaped_private_key_file} -o IdentitiesOnly=yes "
                f"-o StrictHostKeyChecking=no"
            )

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
) -> Tuple[Sequence[str], Optional[str]]:
    """
    Returns [code_paths, interpreter_spec_path] based on Job.code_deployment. code_paths
    will have paths to folders that are available on this machine that contain "the
    user's code". interpreter_spec_path will be a path where we can find e.g. an
    environment.yml file that can be used to build a conda environment.

    For ServerAvailableFolder, this function will effectively use code_paths as
    specified.

    For GitRepoCommit, this function will create an immutable local copy of the
    specific commit.
    """
    case = job.WhichOneof("code_deployment")
    if case == "server_available_folder":
        if job.server_available_folder.code_paths:
            interpreter_spec_path = job.server_available_folder.code_paths[0]
        else:
            interpreter_spec_path = None
        return (
            job.server_available_folder.code_paths,
            # TODO theoretically the environment file could be not in the first code
            # path, but should be a very unlikely scenario for now.
            interpreter_spec_path,
        )
    elif case == "git_repo_commit":
        return await _get_git_code_paths(
            git_repos_folder,
            local_copies_folder,
            job.git_repo_commit.repo_url,
            job.git_repo_commit.commit,
            job.git_repo_commit.path_to_source,
            credentials,
        )
    elif case == "git_repo_branch":
        # warning this is not reproducible!!! should ideally be resolved on the
        # client
        return await _get_git_code_paths(
            git_repos_folder,
            local_copies_folder,
            job.git_repo_branch.repo_url,
            # TODO this is a bit of a hack. We don't want to bother pulling/merging e.g.
            # origin/main into main, so we just always reference origin/main.
            f"origin/{job.git_repo_branch.branch}",
            job.git_repo_branch.path_to_source,
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
    path_to_source: str,
    credentials: Optional[RawCredentials],
) -> Tuple[Sequence[str], str]:
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
        return [os.path.join(local_copy_path, path_to_source)], local_copy_path


async def compile_environment_spec_to_container(
    environment_spec_in_code: EnvironmentSpecInCode, interpreter_spec_path: str
) -> ServerAvailableContainer:
    """
    Turns e.g. a conda_environment.yml file into a docker container which will be
    available locally (as per the returned ServerAvailableContainer), and also cached in
    ECR.

    TODO we should also consider creating conda environments locally rather than always
    making a container for them, that might be more efficient.
    """
    if (
        environment_spec_in_code.environment_type
        == EnvironmentSpecInCode.EnvironmentType.CONDA
    ):
        path_to_spec = os.path.join(
            interpreter_spec_path, environment_spec_in_code.path_to_spec
        )
        with open(path_to_spec, "rb") as spec:
            # TODO probably better to exclude the name and prefix in the file as those
            # are ignored
            spec_hash = hashlib.blake2b(spec.read(), digest_size=64).hexdigest()

        region_name = await _get_default_region_name()
        repository_prefix = ensure_repository(
            _MEADOWRUN_GENERATED_DOCKER_REPO, region_name
        )
        image_name = (
            f"{repository_prefix}/{_MEADOWRUN_GENERATED_DOCKER_REPO}:{spec_hash}"
        )
        result = ServerAvailableContainer(image_name=image_name)

        username_password = get_username_password(region_name)

        # if the image already exists locally, just return it
        if await _does_digest_exist_locally(image_name):
            return result

        # if the image doesn't exist locally but does exist in ECR, try to pull it
        if does_image_exist(_MEADOWRUN_GENERATED_DOCKER_REPO, spec_hash, region_name):
            try:
                await pull_image(image_name, username_password)
                # TODO we're assuming that once we pull the image it will be available
                # locally until the job runs, which might not be true if we implement
                # something to clean unused images.
                return result
            except (aiodocker_exceptions.DockerError, ValueError) as e:
                print(
                    f"Warning, image {image_name} was supposed to exist, but we were "
                    "unable to pull, so re-building locally. This could happen if the "
                    "image was deleted between when we checked for it and when we "
                    f"pulled, but that should be rare: {e}"
                )

        # the image doesn't exist locally or in ECR, so build it ourselves and cache it
        # in ECR
        spec_filename = os.path.basename(path_to_spec)
        docker_file_path = os.path.join(
            os.path.dirname(__file__), "docker_files", "CondaDockerfile"
        )
        await build_image(
            [(docker_file_path, "Dockerfile"), (path_to_spec, spec_filename)],
            image_name,
            {"ENV_FILE": spec_filename},
        )

        # try to push the image so that we can reuse it later
        # TODO this should really happen asynchronously as it takes a long time and
        # isn't critical.
        # TODO we should also somehow avoid multiple processes on the same (or even on
        # different machines) building the identical image at the same time.
        try:
            await push_image(image_name, username_password)
        except aiodocker_exceptions.DockerError as e:
            print(
                f"Warning, image {image_name} couldn't be pushed. Execution can "
                f"continue, but we won't be able to reuse this image later: {e}"
            )

        return result
    else:
        raise ValueError(
            f"Unexpected environment_type {environment_spec_in_code.environment_type}"
        )
