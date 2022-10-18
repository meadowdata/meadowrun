from __future__ import annotations

import asyncio
import os
import sys
from typing import Callable, Awaitable, Optional

import filelock

from meadowrun.shared import remove_corrupted_environment
from meadowrun.deployment.prerequisites import (
    EnvironmentSpecPrerequisites,
    GOOGLE_AUTH_PACKAGE,
)
from meadowrun.storage_keys import STORAGE_ENV_CACHE_PREFIX


# _pip_freeze and friends are run in the user environment and have to deal with
# arbitrary configurations. get_cached_or_create_pip_environment and
# create_pip_environment run in controlled environments, either in the Meadowrun docker
# image or a Meadowrun AMI


async def _pip_freeze(python_interpreter: str, *freeze_opts: str) -> str:
    """Effectively returns the output of:
    > <python_interpreter> -m pip freeze <freeze_opts>
    """

    # `pip freeze` and `pip list --format=freeze` usually give identical results, but
    # not for "packages installed via Direct URLs":
    # https://github.com/pypa/pip/issues/8176 which poetry uses to install all packages.
    # So for us, `pip list --format=freeze` gives us the output we want both with poetry
    # and in a "regular" virtualenv. We should keep an eye on that issue, though, as it
    # looks like this difference in behavior was not totally intentional and may change
    # in the future.
    # Update Aug 22: we try to find pyproject.toml and poetry.lock before getting here,
    # so we're using pip freeze now. Direct URLs are needed to support packages
    # installed via git URLs.
    env = os.environ.copy()
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    opts = ("-m", "pip", "freeze") + freeze_opts
    p = await asyncio.create_subprocess_exec(
        python_interpreter,
        *opts,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await p.communicate()

    if p.returncode != 0:
        raise ValueError(
            f"pip freeze failed, return code {p.returncode}: {stderr.decode()}"
        )

    return stdout.decode()


async def pip_freeze_exclude_editable(
    python_interpreter: Optional[str] = None,
) -> str:
    """
    Returns the result of calling pip freeze on the specified python_interpreter, or the
    current one if not specified. Excludes editable installs - the local code deployment
    should take largely care of the editable installs, except for running setup.py or
    equivalent.
    """
    if python_interpreter is None:
        python_interpreter = sys.executable
    # TODO we have not actually implemented the portable part of this yet
    return await _pip_freeze(python_interpreter, "--exclude-editable")


_PIP_ENVIRONMENT_TIMEOUT = 10 * 60


async def get_cached_or_create_pip_environment(
    environment_hash: str,
    requirements_file_path: str,
    prerequisites: EnvironmentSpecPrerequisites,
    new_environment_path: str,
    try_get_file: Callable[[str, str], Awaitable[bool]],
    upload_file: Callable[[str, str], Awaitable[None]],
) -> str:
    """
    If the desired pip environment exists, does nothing. If the environment has been
    cached, creates it from the cache. Otherwise creates the environment from scratch
    and caches it. Returns the path to the newly created python interpreter.

    try_get_file and upload_file are for interacting with the cache, see
    compile_environment_spec_locally for more details
    """
    # this assumes that that current version of python is what will be used in
    # create_pip_environment
    new_environment_path = (
        f"{new_environment_path}-{sys.version_info.major}.{sys.version_info.minor}"
    )

    new_environment_interpreter = os.path.join(new_environment_path, "bin", "python")

    with filelock.FileLock(f"{new_environment_path}.lock", _PIP_ENVIRONMENT_TIMEOUT):
        if os.path.exists(new_environment_path):
            return new_environment_interpreter

        remote_cached_file_name = (
            f"{STORAGE_ENV_CACHE_PREFIX}{environment_hash}-{sys.version_info.major}."
            f"{sys.version_info.minor}.tar.gz"
        )
        local_cached_file = f"{new_environment_path}.tar.gz"
        download_succeeded = await try_get_file(
            remote_cached_file_name, local_cached_file
        )
        if download_succeeded:
            try:
                print("Unpacking cached pip environment")
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
                            f"Unpacking cached pip environment {local_cached_file} "
                            f"returned code {return_code}"
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

        print("Creating the pip environment")
        try:
            await create_pip_environment(
                requirements_file_path, new_environment_path, prerequisites
            )
        except BaseException:
            remove_corrupted_environment(new_environment_path)
            raise

        try:
            import venv_pack  # see note on reference in pyproject.toml
        except ImportError:
            print(
                "Warning unable to cache pip environment because venv_pack is missing"
            )
            return new_environment_interpreter

        # TODO we shouldn't wait for this to start running the job but we also shouldn't
        # kill the container until this finishes
        print("Caching the pip environment")
        try:
            # there might be an old file from a failed attempt
            os.remove(local_cached_file)
        except OSError:
            pass
        venv_pack.pack(new_environment_path, output=local_cached_file)
        await upload_file(local_cached_file, remote_cached_file_name)

        return new_environment_interpreter


async def create_pip_environment(
    requirements_file_path: str,
    new_environment_path: str,
    prerequisites: EnvironmentSpecPrerequisites,
) -> None:
    """
    Creates a pip environment in new_environment_path. requirements_file_path should
    point to a requirements.txt file that will determine what packages are installed.

    There should usually be a filelock around this function.
    """
    # this code is roughly equivalent to the code in PipDockerfile and PipDockerfile
    return_code = await (
        await asyncio.create_subprocess_exec(
            "python", "-m", "venv", new_environment_path
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"venv creation in {new_environment_path} failed with return code "
            f"{return_code}"
        )

    disable_pip_version_check = {"PIP_DISABLE_PIP_VERSION_CHECK": "1"}
    pip_install = [
        os.path.join(new_environment_path, "bin", "python"),
        "-m",
        "pip",
        "install",
    ]

    if prerequisites & EnvironmentSpecPrerequisites.GOOGLE_AUTH:
        return_code = await (
            await asyncio.create_subprocess_exec(
                *pip_install,
                GOOGLE_AUTH_PACKAGE,
                env=disable_pip_version_check,
            )
        ).wait()
        if return_code != 0:
            raise ValueError(
                f"Installing pre-requirements for {requirements_file_path} in "
                f"{new_environment_path} failed with return code {return_code}"
            )

    return_code = await (
        await asyncio.create_subprocess_exec(
            *pip_install,
            "-r",
            requirements_file_path,
            env=disable_pip_version_check,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing requirements from {requirements_file_path} in "
            f"{new_environment_path} failed with return code {return_code}"
        )
