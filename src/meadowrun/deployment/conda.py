from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import List, Optional, Tuple, Callable, Awaitable, TYPE_CHECKING

import filelock

from meadowrun.deployment.pack_envs import pack_conda_env
from meadowrun.shared import remove_corrupted_environment
from meadowrun.storage_keys import STORAGE_ENV_CACHE_PREFIX

if TYPE_CHECKING:
    from meadowrun.deployment.prerequisites import EnvironmentSpecPrerequisites
    from typing_extensions import Final


class CondaMissingException(Exception):
    """Raised when the conda executable can not be found."""

    pass


async def _run(args: List[str]) -> Tuple[str, str]:
    """Runs a conda command in an external process. Returns stdout, stderr."""

    if "CONDA_EXE" not in os.environ:
        raise CondaMissingException(
            "CONDA_EXE not set - is conda installed and configured correctly?"
        )

    conda = os.environ["CONDA_EXE"]
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

    return stdout.decode(), stderr.decode()


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
    out, _ = await _run(["env", "export", "-p", env_path])
    return out


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

    # TODO: see https://github.com/conda/conda/issues/5253
    # there is also conda list --explicit/--export
    out, _ = await _run(["env", "export", "-p", env_path])
    return out


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
            await create_conda_environment(
                environment_yml_file_path,
                new_environment_path,
                working_directory,
                filter_editable_install=not allow_editable_install,
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


async def create_conda_environment(
    environment_yml_file_path: str,
    new_environment_path: str,
    working_directory: Optional[str],
    filter_editable_install: bool,
) -> None:
    # TODO take prerequisites into account, also support files generated by `conda list
    # --explicit` as well as `conda env export`
    # mamba create -n foo3 --no-default-packages

    # this code is roughly equivalent to the code in CondaDockerfile and
    # CondaDockerfile
    if filter_editable_install:
        environment_yml_file_path = filter_editable_installs_from_conda_env_yml(
            environment_yml_file_path
        )

    return_code = await (
        await asyncio.create_subprocess_exec(
            _CONDA_EXECUTABLE,
            "env",
            "create",
            "-f",
            environment_yml_file_path,
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


def filter_editable_installs_from_conda_env_yml(environment_yml_file_path: str) -> str:
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
