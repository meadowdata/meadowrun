from __future__ import annotations

import asyncio
import json
import os
import shutil
import sys
from typing import List, Optional, Tuple, Callable, Awaitable, TYPE_CHECKING, Sequence

import filelock
import yaml

from meadowrun.deployment.pack_envs import pack_conda_env
from meadowrun.meadowrun_pb2 import EnvironmentFileFormat
from meadowrun.shared import remove_corrupted_environment
from meadowrun.storage_keys import STORAGE_ENV_CACHE_PREFIX
from meadowrun.deployment.prerequisites import (
    EnvironmentSpecPrerequisites,
    GOOGLE_AUTH_PACKAGE,
)

if TYPE_CHECKING:
    from typing_extensions import Final

# This is a special Meadowrun-only way of specifying pip packages in a conda list
# explicit format file. Lines starting with # will be ignored as comments by conda.
_PIP_COMMENT_PREFIX = "# mdr-pip: "


class CondaMissingException(Exception):
    """Raised when the conda executable can not be found."""

    pass


async def _run(args: List[str]) -> Tuple[str, str]:
    """Runs a conda command in an external process. Returns stdout, stderr."""

    if "CONDA_EXE" in os.environ:
        conda: Optional[str] = os.environ["CONDA_EXE"]
    else:
        conda = shutil.which("conda")

    if conda is None:
        raise CondaMissingException(
            "CONDA_EXE not set and conda is not on the path - is conda installed and "
            "configured correctly?"
        )

    env = os.environ.copy()

    p = await asyncio.create_subprocess_exec(
        conda,
        *args,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await p.communicate()

    if p.returncode != 0:
        raise ValueError(
            f"conda {' '.join(args)} failed, return code {p.returncode}: "
            + stderr.decode()
        )

    return stdout.decode("utf-8"), stderr.decode("utf-8")


def _env_export_split_pip(env_export_text: str) -> Tuple[List[str], str]:
    """
    Takes the output of `conda env export` and splits out the pip section. Returns
    (pip_dependencies, env_export_without_pip)
    """
    env_export_parsed = yaml.safe_load(env_export_text[: env_export_text.find("\x1b")])
    dependencies = env_export_parsed.get("dependencies", [])
    pip_dependency = None
    for dependency in dependencies:
        if isinstance(dependency, dict) and "pip" in dependency:
            pip_dependency = dependency

    if pip_dependency is None:
        return [], env_export_text

    dependencies.remove(pip_dependency)

    return pip_dependency["pip"], yaml.safe_dump(env_export_parsed)


async def _conda_list_explicit_with_pip(env_path: str) -> str:
    """
    Gets the list --explicit output with the pip packages added in using the special
    mdr-pip comments
    """
    list_explicit_output, _ = await _run(["list", "--explicit", "-p", env_path])
    # we don't want to run `pip freeze` because that will also include conda packages
    env_export_text, _ = await _run(["env", "export", "-p", env_path])
    pip_output, _ = _env_export_split_pip(env_export_text)
    return list_explicit_output + "".join(
        f"\n{_PIP_COMMENT_PREFIX}{line}" for line in pip_output
    )


async def try_get_current_conda_env() -> Optional[str]:
    """
    Returns the result of conda env export on the current conda environment. Returns
    None if there's no currently active conda environment
    """
    env_path = os.environ.get("CONDA_PREFIX")
    # we have to guard against the case where there's a conda environment activated
    # (e.g. base) but there is also a pip environment activated.
    if env_path is None or not os.path.realpath(sys.executable).startswith(
        os.path.realpath(env_path)
    ):
        return None
    return await _conda_list_explicit_with_pip(env_path)


async def env_export(name_or_path: str) -> str:
    """Runs `conda env export` on the conda environment with the given name, and returns
    the results.

    :param name_or_path: name or full path to the conda environment
    :return: Output of the conda env export command.
    """
    out, _ = await _run(["env", "list", "--json"])
    result = json.loads(out)
    envs: List[str] = result["envs"]
    if len(envs) == 0:
        raise ValueError("No conda environments found")
    elif len(envs) == 1:
        # praise conda: the first path is always the base path
        if name_or_path != "base":
            raise ValueError(
                f"Conda environment {name_or_path} not found - only base environment is"
                " defined"
            )
        env_path = envs[0]
    else:
        if name_or_path in envs:
            env_path = name_or_path
        else:
            for env in envs:
                if name_or_path in env:
                    env_path = env
                    break
            else:
                raise ValueError(f"Conda environment {name_or_path} not found.")

    return await _conda_list_explicit_with_pip(env_path)


_CONDA_ENVIRONMENT_TIMEOUT = 30 * 60


async def get_cached_or_create_conda_environment(
    environment_hash: str,
    working_directory: Optional[str],
    environment_yml_file_path: str,
    prerequisites: EnvironmentSpecPrerequisites,
    new_environment_path: str,
    try_get_file: Callable[[str, str], Awaitable[bool]],
    upload_file: Callable[[str, str], Awaitable[None]],
    allow_editable_install: bool,
    file_format: EnvironmentFileFormat.ValueType,
    spec_contents: str,
) -> str:
    """
    If the desired conda environment exists, does nothing. If the environment has been
    cached, creates it from the cache. Otherwise creates the environment from scratch
    and caches it. Returns the path to the newly created python interpreter.

    try_get_file and upload_file are for interacting with the cache, see
    compile_environment_spec_locally for more details
    """
    new_environment_interpreter = os.path.join(new_environment_path, "bin", "python")

    with filelock.FileLock(f"{new_environment_path}.lock", _CONDA_ENVIRONMENT_TIMEOUT):
        if os.path.exists(new_environment_path):
            return new_environment_interpreter

        remote_cached_file_name = f"{STORAGE_ENV_CACHE_PREFIX}{environment_hash}.tar.gz"
        local_cached_file = f"{new_environment_path}.tar.gz"
        download_succeeded = await try_get_file(
            remote_cached_file_name, local_cached_file
        )
        if download_succeeded:
            try:
                print("Unpacking cached conda environment")
                os.makedirs(new_environment_path, exist_ok=True)
                try:
                    # TODO maybe cleaner to use the built-in python tar libraries?
                    return_code = await (
                        await asyncio.create_subprocess_exec(
                            "tar", "-xzf", local_cached_file, "-C", new_environment_path
                        )
                    ).wait()
                    if return_code != 0:
                        raise ValueError(
                            f"Unpacking cached conda environment {local_cached_file} "
                            f"returned code {return_code}"
                        )

                    print(os.listdir(os.path.join(new_environment_path, "bin")))
                    # conda-unpack shouldn't really be necessary because we always
                    # recreate the environment in exactly the same place, but feels
                    # safer just to run it anyways
                    conda_unpack_command = (
                        "source "
                        f"{os.path.join(new_environment_path, 'bin', 'activate')} && "
                        f"{os.path.join(new_environment_path, 'bin', 'conda-unpack')}"
                    )
                    return_code = await (
                        await asyncio.create_subprocess_exec(
                            "/bin/bash",
                            "-c",
                            conda_unpack_command,
                        )
                    ).wait()
                    if return_code != 0:
                        raise ValueError(
                            f"Calling conda-unpack on restored conda environment "
                            f"{new_environment_path} returned code {return_code}"
                        )
                    return new_environment_interpreter
                except BaseException:
                    remove_corrupted_environment(new_environment_path)
                    raise
            finally:
                try:
                    os.remove(local_cached_file)
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    pass

        print("Creating the conda environment")
        try:
            await _create_conda_environment(
                environment_yml_file_path,
                new_environment_path,
                working_directory,
                filter_editable_install=not allow_editable_install,
                prerequisites=prerequisites,
                file_format=file_format,
                spec_contents=spec_contents,
            )
        except BaseException:
            remove_corrupted_environment(new_environment_path)
            raise

        is_packed = pack_conda_env(
            new_environment_path, local_cached_file, allow_editable_install
        )
        if not is_packed:
            print(
                "Warning unable to cache conda environment because conda_pack is "
                "missing"
            )
            return new_environment_interpreter

        await upload_file(local_cached_file, remote_cached_file_name)

        return new_environment_interpreter


_CONDA_EXECUTABLE: Final = "/opt/conda/bin/mamba"


async def _install_pip_packages(
    conda_environment_path: str, pip_packages: Sequence[str]
) -> None:
    return_code = await (
        await asyncio.create_subprocess_exec(
            f"{conda_environment_path}/bin/python",
            "-m",
            "pip",
            "install",
            *pip_packages,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing pip packages {', '.join(pip_packages)} in "
            f"{conda_environment_path} failed with return code {return_code}"
        )


async def _create_conda_environment(
    environment_file_path: str,
    new_environment_path: str,
    working_directory: Optional[str],
    filter_editable_install: bool,
    prerequisites: EnvironmentSpecPrerequisites,
    file_format: EnvironmentFileFormat.ValueType,
    spec_contents: str,
) -> None:
    # this code is roughly equivalent to the code in CondaDockerfile

    (
        is_list_format,
        environment_file_path,
        pip_prerequisites,
        pip_install_args,
    ) = prepare_conda_file(
        environment_file_path,
        filter_editable_install,
        prerequisites,
        file_format,
        spec_contents,
    )

    if is_list_format:
        env_arg: Sequence[str] = ()
    else:
        env_arg = ("env",)

    return_code = await (
        await asyncio.create_subprocess_exec(
            _CONDA_EXECUTABLE,
            *env_arg,
            "create",
            "--file",
            environment_file_path,
            "-p",
            new_environment_path,
            cwd=working_directory,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Conda environment creation in {new_environment_path} failed with return "
            f"code {return_code}"
        )

    if pip_prerequisites:
        await _install_pip_packages(new_environment_path, pip_prerequisites)

    if pip_install_args:
        await _install_pip_packages(new_environment_path, pip_install_args)


def _is_conda_file_list_format(
    file_format: EnvironmentFileFormat.ValueType, spec_contents: str
) -> Tuple[bool, List[str]]:
    """
    Returns is_list_format, pip_packages. is_list_format == True means we have a `conda
    list --explicit` file that needs to be created via `conda create`. is_list_format ==
    False means we have a `conda env export` file that needs to be created via `conda
    env create`. If is_list_format is True, pip_packages may be populated with
    additional pip packages that we need to install. If is_list_format is False,
    pip_packages will always be empty
    """
    if file_format == EnvironmentFileFormat.ENV_FILE_FORMAT_DEFAULT:
        if any(line.startswith("dependencies:") for line in spec_contents.splitlines()):
            is_list_format = False
        else:
            is_list_format = True
    elif file_format == EnvironmentFileFormat.ENV_FILE_FORMAT_CONDA_ENV_EXPORT:
        is_list_format = False
    elif file_format == EnvironmentFileFormat.ENV_FILE_FORMAT_CONDA_LIST:
        is_list_format = True
    else:
        raise ValueError(
            f"Unexpected value for environment_spec.file_format {file_format}"
        )
    if is_list_format:
        return True, [
            line[len(_PIP_COMMENT_PREFIX) :]
            for line in spec_contents.splitlines()
            if line.startswith(_PIP_COMMENT_PREFIX)
        ]
    else:
        return False, []


def prepare_conda_file(
    environment_file_path: str,
    filter_editable_install: bool,
    prerequisites: EnvironmentSpecPrerequisites,
    file_format: EnvironmentFileFormat.ValueType,
    spec_contents: str,
) -> Tuple[bool, str, Sequence[str], Sequence[str]]:
    """
    Returns is_list_format, environment_file_path, pip_prerequisites, pip_install_args

    Conda files can be in one of two formats, the `conda env export` format
    (is_list_format=False) or the `conda list --explicit` format (is_list_format=True).

    This function manipulates the conda file in various ways:
    - For the list format, extracts out the special meadowrun-only comments for
      specifying pip packages (the format has no way of natively specifying pip
      packages). Additional pip packages are installed via pip_install_args.
    - Sets pip_prerequisites appropriately. Splits out pip packages in the env_export
      format if necessary into the pip_install_args
    - Filters out editable installs based on the parameter

    The result can be used as follows:
    - If is_list_format, then run `conda create --file <environment_file_path>`.
      Otherwise, run `conda env create --file <environment_file_path>`
    - Then run `pip install <pip_prerequisites>`
    - Then run `pip install <pip_install_args>`
    """

    if prerequisites & EnvironmentSpecPrerequisites.GOOGLE_AUTH:
        pip_prerequisites = [GOOGLE_AUTH_PACKAGE]
    else:
        pip_prerequisites = []

    # if we're in list_format, we can mostly use the environment_file_path as is, and
    # pip_packages has been extracted for us
    is_list_format, pip_packages = _is_conda_file_list_format(
        file_format, spec_contents
    )

    if not is_list_format:
        if pip_prerequisites:
            # If we're in env_export format and we have pip prerequisites, we need to
            # rewrite the environment file to exclude the pip packages and put the pip
            # packages in the pip_packages variable. (Normally the env_export format can
            # just install conda and pip packages in one go.)
            pip_packages, new_environment_file_contents = _env_export_split_pip(
                spec_contents
            )
            path, file = os.path.split(environment_file_path)
            environment_file_path = os.path.join(path, "filtered-" + file)
            with open(environment_file_path, "w", encoding="utf-8") as flt:
                flt.write(new_environment_file_contents)
        elif filter_editable_install:
            # If we left the pip packages in the environment file, AND we want to filter
            # out editable installs, we need to filter them out now
            environment_file_path = _filter_editable_installs_from_env_export_file(
                environment_file_path
            )

    if filter_editable_install:
        # if we want to filter out editable installs and we have pip_packages populated:
        pip_packages = [
            line for line in pip_packages if not line.startswith("-e") or "git+" in line
        ]

    pip_install_args = [arg for line in pip_packages for arg in line.split(" ")]

    return is_list_format, environment_file_path, pip_prerequisites, pip_install_args


def _filter_editable_installs_from_env_export_file(
    environment_yml_file_path: str,
) -> str:
    with open(environment_yml_file_path, "r", encoding="utf-8") as orig:
        orig_lines = orig.readlines()
    path, file = os.path.split(environment_yml_file_path)
    environment_yml_file_path = os.path.join(path, "filtered-" + file)
    with open(environment_yml_file_path, "w", encoding="utf-8") as flt:
        for orig_line in orig_lines:
            line = orig_line.strip()
            if line.startswith("- -e") and "git+" not in line:
                continue
            flt.write(orig_line)
    return environment_yml_file_path
