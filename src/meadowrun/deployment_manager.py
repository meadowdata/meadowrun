"""Manages local deployments for run_job_local"""

from __future__ import annotations

import asyncio.subprocess
import functools
import hashlib
import os
import shutil
import tempfile
import urllib.parse
from typing import (
    Awaitable,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import filelock

from meadowrun._vendor.aiodocker import exceptions as aiodocker_exceptions
from meadowrun.aws_integration.ecr import (
    get_ecr_helper,
)
from meadowrun.aws_integration.management_lambdas.ec2_alloc_stub import (
    _MEADOWRUN_GENERATED_DOCKER_REPO,
)
from meadowrun.azure_integration.acr import get_acr_helper
from meadowrun.conda import get_cached_or_create_conda_environment
from meadowrun.credentials import RawCredentials, SshKey
from meadowrun.docker_controller import (
    _does_digest_exist_locally,
    build_image,
    pull_image,
    push_image,
)
from meadowrun.func_worker_storage_helper import FuncWorkerClientObjectStorage
from meadowrun.meadowrun_pb2 import (
    CodeZipFile,
    EnvironmentSpec,
    EnvironmentSpecInCode,
    EnvironmentType,
    Job,
    ServerAvailableContainer,
    ServerAvailableInterpreter,
)
from meadowrun.pip_integration import get_cached_or_create_pip_environment
from meadowrun.poetry_integration import get_cached_or_create_poetry_environment
from meadowrun.azure_integration.blob_storage import AzureBlobStorage
from meadowrun.aws_integration.s3 import S3ObjectStorage
from meadowrun.run_job_core import (
    CloudProviderType,
    ContainerRegistryHelper,
    LocalObjectStorage,
)
from meadowrun.func_worker_storage_helper import (
    try_get_storage_file,
    write_storage_file,
)
import meadowrun.func_worker_storage_helper

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
) -> Tuple[Sequence[str], Optional[str], Optional[str]]:
    """
    Returns [code_paths, interpreter_spec_path, current_working_directory] based on
    Job.code_deployment. code_paths will have paths to folders that are available on
    this machine that contain "the user's code". interpreter_spec_path will be a path
    where we can find e.g. an environment.yml file that can be used to build a conda
    environment.

    For ServerAvailableFolder, this function will effectively use code_paths as
    specified.

    For GitRepoCommit, this function will create an immutable local copy of the specific
    commit.
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
            None,
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
    elif case == "code_zip_file":
        return await _get_zip_file_code_paths(local_copies_folder, job.code_zip_file)
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
) -> Tuple[Sequence[str], str, str]:
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
        source_path = os.path.join(local_copy_path, path_to_source)
        return ([source_path], local_copy_path, local_copy_path)


_ALL_OBJECT_STORAGES = {
    # see the note on get_url_scheme
    s.get_url_scheme(): s  # type: ignore
    for s in [
        AzureBlobStorage,
        FuncWorkerClientObjectStorage,
        LocalObjectStorage,
        S3ObjectStorage,
    ]
}


async def _get_zip_file_code_paths(
    local_copies_folder: str, code_zip_file: CodeZipFile
) -> Tuple[List[str], None, str]:
    decoded_url = urllib.parse.urlparse(code_zip_file.url)
    if decoded_url.scheme not in _ALL_OBJECT_STORAGES:
        raise ValueError(f"Unknown URL scheme in {code_zip_file.url}")

    object_storage = _ALL_OBJECT_STORAGES[decoded_url.scheme]()
    extracted_folder = await object_storage.download_and_unzip(
        code_zip_file.url, local_copies_folder
    )

    return (
        [
            os.path.join(local_copies_folder, extracted_folder, zip_path)
            for zip_path in code_zip_file.code_paths
        ],
        None,
        os.path.join(local_copies_folder, extracted_folder, code_zip_file.cwd_path),
    )


async def compile_environment_spec_to_container(
    environment_spec: Union[EnvironmentSpecInCode, EnvironmentSpec],
    interpreter_spec_path: str,
    cloud: Optional[Tuple[CloudProviderType, str]],
) -> ServerAvailableContainer:
    """
    Turns e.g. a conda_environment.yml file into a docker container which will be
    available locally (as per the returned ServerAvailableContainer), and also cached in
    ECR.

    TODO we should also consider creating conda environments locally rather than always
    making a container for them, that might be more efficient.
    """
    path_to_spec, spec_hash, has_git_dependency = _get_path_and_hash(
        environment_spec, interpreter_spec_path
    )

    if cloud is None:
        helper = ContainerRegistryHelper(
            False, None, f"{_MEADOWRUN_GENERATED_DOCKER_REPO}:{spec_hash}", False
        )
    else:
        cloud_type, region_name = cloud
        if cloud_type == "EC2":
            helper = await get_ecr_helper(
                _MEADOWRUN_GENERATED_DOCKER_REPO, spec_hash, region_name
            )
        elif cloud_type == "AzureVM":
            helper = await get_acr_helper(
                _MEADOWRUN_GENERATED_DOCKER_REPO, spec_hash, region_name
            )
        else:
            raise ValueError(f"Unexpected cloud_type {cloud_type}")

    result = ServerAvailableContainer(image_name=helper.image_name)

    # if the image already exists locally, just return it
    if await _does_digest_exist_locally(helper.image_name):
        return result

    # if the image doesn't exist locally but does exist in ECR, try to pull it
    if helper.does_image_exist:
        try:
            await pull_image(helper.image_name, helper.username_password)
            # TODO we're assuming that once we pull the image it will be available
            # locally until the job runs, which might not be true if we implement
            # something to clean unused images.
            return result
        except (aiodocker_exceptions.DockerError, ValueError) as e:
            print(
                f"Warning, image {helper.image_name} was supposed to exist, but we were"
                " unable to pull, so re-building locally. This could happen if the "
                "image was deleted between when we checked for it and when we pulled, "
                f"but that should be rare: {e}"
            )

    # the image doesn't exist locally or in ECR, so build it ourselves and cache it in
    # ECR

    print(f"Building python environment in container {spec_hash}")

    build_args = {}
    files_to_copy = []
    apt_packages = [
        p for p in environment_spec.additional_software.keys() if p != "cuda"
    ]
    if has_git_dependency:
        apt_packages.append("git")
    if apt_packages:
        build_args["APT_PACKAGES"] = " ".join(apt_packages)

    if environment_spec.environment_type == EnvironmentType.CONDA:
        if apt_packages:
            docker_file_name = "CondaAptDockerfile"
        else:
            docker_file_name = "CondaDockerfile"
        spec_filename = os.path.basename(path_to_spec)
        build_args["ENV_FILE"] = spec_filename
        files_to_copy.append((path_to_spec, spec_filename))
        if "cuda" in environment_spec.additional_software:
            build_args[
                "CONDA_IMAGE"
            ] = "meadowrun/cuda-conda:conda4.12.0-cuda11.6.2-cudnn8-ubuntu20.04"
        else:
            build_args["CONDA_IMAGE"] = "continuumio/miniconda3"
    else:
        # common to pip and poetry
        if "cuda" in environment_spec.additional_software:
            build_args["PYTHON_IMAGE"] = (
                f"meadowrun/cuda-python:py{environment_spec.python_version}-"
                f"cuda11.6.2-cudnn8-ubuntu20.04"
            )
        else:
            build_args[
                "PYTHON_IMAGE"
            ] = f"python:{environment_spec.python_version}-slim-bullseye"

        if environment_spec.environment_type == EnvironmentType.PIP:
            if apt_packages:
                docker_file_name = "PipAptDockerfile"
            else:
                docker_file_name = "PipDockerfile"
            spec_filename = os.path.basename(path_to_spec)
            files_to_copy.append((path_to_spec, spec_filename))
            build_args["ENV_FILE"] = spec_filename
        elif environment_spec.environment_type == EnvironmentType.POETRY:
            if apt_packages:
                docker_file_name = "PoetryAptDockerfile"
            else:
                docker_file_name = "PoetryDockerfile"
            files_to_copy.append(
                (os.path.join(path_to_spec, "pyproject.toml"), "pyproject.toml")
            )
            files_to_copy.append(
                (os.path.join(path_to_spec, "poetry.lock"), "poetry.lock")
            )
        else:
            raise ValueError(
                f"Unexpected environment_type {environment_spec.environment_type}"
            )

    docker_file_path = os.path.join(
        os.path.dirname(__file__), "docker_files", docker_file_name
    )
    files_to_copy.append((docker_file_path, "Dockerfile"))
    await build_image(files_to_copy, helper.image_name, build_args)

    # try to push the image so that we can reuse it later
    # TODO this should really happen asynchronously as it takes a long time and isn't
    # critical.
    # TODO we should also somehow avoid multiple processes on the same (or even on
    # different machines) building the identical image at the same time.
    if helper.should_push:
        try:
            print("Caching container")
            await push_image(helper.image_name, helper.username_password)
        except aiodocker_exceptions.DockerError as e:
            print(
                f"Warning, image {helper.image_name} couldn't be pushed. Execution can "
                f"continue, but we won't be able to reuse this image later: {e}"
            )

    return result


async def compile_environment_spec_locally(
    environment_spec: Union[EnvironmentSpecInCode, EnvironmentSpec],
    interpreter_spec_path: str,
    built_interpreters_folder: str,
) -> ServerAvailableInterpreter:
    """
    Turns e.g. a conda_environment.yml file into a locally available python interpreter
    """
    path_to_spec, spec_hash, has_git_dependency = _get_path_and_hash(
        environment_spec, interpreter_spec_path
    )
    # we don't have to worry about has_git_dependency as we assume we always have git
    # installed in this environment

    print(f"Building python environment locally {spec_hash}")

    # TODO we shouldn't install apt packages but we should check to see whether they're
    # installed
    # apt_packages = [
    #     p for p in environment_spec.additional_software.keys() if p != "cuda"
    # ]

    if environment_spec.environment_type == EnvironmentType.CONDA:
        get_cached_or_create: Callable[
            [str, str, str, Callable[[str, str], bool], Callable[[str, str], None]],
            Awaitable[str],
        ] = get_cached_or_create_conda_environment
    elif environment_spec.environment_type == EnvironmentType.PIP:
        get_cached_or_create = get_cached_or_create_pip_environment
    elif environment_spec.environment_type == EnvironmentType.POETRY:
        get_cached_or_create = get_cached_or_create_poetry_environment
    else:
        raise ValueError(
            f"Unexpected environment_type {environment_spec.environment_type}"
        )

    return ServerAvailableInterpreter(
        interpreter_path=await get_cached_or_create(
            spec_hash,
            path_to_spec,
            os.path.join(built_interpreters_folder, spec_hash),
            # TODO this isn't the right way to do this--these try_get_storage_file and
            # write_storage file functions should be getting passed in from higher up.
            # For now this works because this code path is only being hit from
            # Kubernetes
            # try_get_file(remote_file_name: str, local_file_name: str) -> bool. Tries
            # to download the specified remote file to the specified local file name.
            # Returns True if the file is available, False if the file is not available.
            functools.partial(
                try_get_storage_file,
                meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT,
                meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET,
            ),
            # upload_file(local_file_name: str, remote_file_name: str) -> None. Uploads
            # the specified file, overwrites any existing remote file.
            functools.partial(
                write_storage_file,
                meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_CLIENT,
                meadowrun.func_worker_storage_helper.FUNC_WORKER_STORAGE_BUCKET,
            ),
        )
    )


def _hash_spec(spec_contents: bytes) -> str:
    # TODO better to exclude the name and prefix for conda yml files as those
    # are ignored
    return hashlib.blake2b(spec_contents, digest_size=64).hexdigest()


def _format_additional_software(additional_software: Mapping[str, str]) -> str:
    return "\n\nmeadowrun-additional-software: " + "; ".join(
        f"{key}: {value}" for key, value in sorted(additional_software.items())
    )


def _get_path_and_hash(
    environment_spec: Union[EnvironmentSpecInCode, EnvironmentSpec],
    interpreter_spec_path: str,
) -> Tuple[str, str, bool]:
    """
    Returns path_to_spec, spec_hash, has_git_dependency. spec_hash is a hash that
    uniquely identifies the environment. path_to_spec is used slightly differently for
    each environment type's Dockerfile. has_git_dependency is also specific to each
    """

    if isinstance(environment_spec, EnvironmentSpecInCode):
        # in this case, interpreter_spec_path is a path to the root of the place where
        # the spec lives.
        path_to_spec = os.path.join(
            interpreter_spec_path, environment_spec.path_to_spec
        )
        if environment_spec.environment_type == EnvironmentType.POETRY:
            # in the case of Poetry, the path_to_spec should be a folder that contains
            # pyproject.toml and poetry.lock
            file_to_hash = os.path.join(path_to_spec, "poetry.lock")
        else:
            file_to_hash = path_to_spec
        with open(file_to_hash, "rb") as spec:
            spec_contents_bytes = spec.read()
        return (
            path_to_spec,
            _hash_spec(
                spec_contents_bytes
                + _format_additional_software(
                    environment_spec.additional_software
                ).encode("utf-8")
            ),
            _has_git_dependency(
                spec_contents_bytes.decode("utf-8"), environment_spec.environment_type
            ),
        )
    elif isinstance(environment_spec, EnvironmentSpec):
        # in this case, interpreter_spec_path is a path to where we save the spec for
        # later processing.
        if environment_spec.environment_type == EnvironmentType.POETRY:
            # for poetry, path_to_spec will be a folder that contains pyproject.toml and
            # poetry.lock
            spec_contents_str = environment_spec.spec_lock
            spec_hash = _hash_spec(
                (
                    spec_contents_str
                    + _format_additional_software(environment_spec.additional_software)
                ).encode("UTF-8")
            )
            path_to_spec = os.path.join(interpreter_spec_path, spec_hash)
            os.makedirs(path_to_spec, exist_ok=True)
            with open(
                os.path.join(path_to_spec, "pyproject.toml"), "w", encoding="utf-8"
            ) as spec_file:
                spec_file.write(environment_spec.spec)
            with open(
                os.path.join(path_to_spec, "poetry.lock"), "w", encoding="utf-8"
            ) as lock_file:
                lock_file.write(environment_spec.spec_lock)
        else:
            spec_contents_str = environment_spec.spec
            spec_hash = _hash_spec(
                (
                    spec_contents_str
                    + _format_additional_software(environment_spec.additional_software)
                ).encode("UTF-8")
            )

            if environment_spec.environment_type == EnvironmentType.CONDA:
                spec_filename = f"conda_env_spec_{spec_hash}.yml"
            elif environment_spec.environment_type == EnvironmentType.PIP:
                spec_filename = f"pip_env_spec_{spec_hash}.txt"
            else:
                raise ValueError(
                    f"Unexpected environment_type: {environment_spec.environment_type}"
                )

            path_to_spec = os.path.join(interpreter_spec_path, spec_filename)
            with open(path_to_spec, "w", encoding="utf-8") as env_spec:
                env_spec.write(environment_spec.spec)

        return (
            path_to_spec,
            spec_hash,
            _has_git_dependency(spec_contents_str, environment_spec.environment_type),
        )
    else:
        raise ValueError(
            f"Unexpected type of environment_spec {type(environment_spec)}"
        )


def _has_git_dependency(
    spec_contents: str, environment_type: EnvironmentType.ValueType
) -> bool:
    if (
        environment_type == EnvironmentType.PIP
        or environment_type == EnvironmentType.CONDA
    ):
        # example line resulting from pip freeze:
        # package @ git+https://github.com/name/repo.git@sha
        # Conda can have git-based dependencies via pip
        return "git+" in spec_contents
    elif environment_type == EnvironmentType.POETRY:
        return any(
            line.startswith('type = "git"') for line in spec_contents.splitlines()
        )
    else:
        return False
