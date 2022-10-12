from __future__ import annotations

import asyncio
import os
import sys
from typing import Callable, Awaitable, Optional, List

import filelock

from meadowrun.deployment.prerequisites import (
    EnvironmentSpecPrerequisites,
    GOOGLE_AUTH_PACKAGE,
)
from meadowrun.shared import remove_corrupted_environment
from meadowrun.deployment.pack_envs import pack_venv
from meadowrun.storage_keys import STORAGE_ENV_CACHE_PREFIX


# _pip_freeze and friends are run in the user environment and have to deal with
# arbitrary configurations. get_cached_or_create_pip_environment and
# create_pip_environment run in controlled environments, either in the Meadowrun docker
# image or a Meadowrun AMI


def _parse_pip_config_output(s: str, multi_value_separator: str) -> List[str]:
    if s is None:
        return []

    if s.startswith("'") and s.endswith("'"):
        s = s[1:-1]
    return [substring for substring in s.split(multi_value_separator) if substring]


async def _get_index_urls_for_requirements_file(python_interpreter: str) -> str:
    # pip freeze doesn't capture the index-url that packages came from. Our best option
    # is to call pip config list and add any index-url/extra-index-url options to the
    # output of pip freeze

    # docs on pip.conf: https://pip.pypa.io/en/stable/topics/configuration/
    # pip config list outputs lines that look like:
    # global.extra-index-url='https://us-east1-python.pkg.dev/meadowrun-playground/test-py-repo/simple/'

    p = await asyncio.create_subprocess_exec(
        python_interpreter,
        "-m",
        "pip",
        "config",
        "list",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await p.communicate()
    if p.returncode != 0:
        raise ValueError(
            f"pip config list failed, return code {p.returncode}: {stderr.decode()}"
        )
    # this seems to be the list of possible scopes for the "install" command in order of
    # priority. :env: comes from environment variables and is highest priority. install
    # comes from pip.conf and is second priority. global also comes from pip.conf
    # (applies to all pip commands) and is lowest priority. Higher-priority configs
    # override lower-priority configs (no merging)
    scopes = [":env:", "install", "global"]
    parameters = ["index-url", "extra-index-url"]
    values = {}
    for line in stdout.decode("utf-8").splitlines():
        for parameter in parameters:
            for scope in scopes:
                prefix = f"{scope}.{parameter}="
                if line.startswith(prefix):
                    if (scope, parameter) in values:
                        raise ValueError(
                            "pip config list returned more than one line starting with "
                            f"{prefix}"
                        )

                    # For multiple values, :env: splits by whitespace and pip.conf items
                    # split by newlines
                    if scope == ":env:":
                        multi_value_separator = " "
                    else:
                        multi_value_separator = "\\n"

                    values[(scope, parameter)] = _parse_pip_config_output(
                        line[len(prefix) :], multi_value_separator
                    )

    result = []
    for parameter in parameters:
        for scope in scopes:
            if (scope, parameter) in values:
                for value in values[(scope, parameter)]:
                    result.append(f"--{parameter} {value}")
                # just break out of the scopes loop, move on to the next parameter
                break
    return "\n".join(result)


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

    freeze_output = stdout.decode("utf-8")

    # add index urls from pip.conf if needed
    index_urls = await _get_index_urls_for_requirements_file(python_interpreter)
    if index_urls:
        return index_urls + "\n" + freeze_output
    else:
        return freeze_output


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


async def get_python_version(python_interpreter: str) -> str:
    """
    Not strictly pip related but currently only used in the context of pip environments
    """
    p = await asyncio.create_subprocess_exec(
        python_interpreter,
        "-c",
        "import sys; print(sys.version[0:3])",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await p.communicate()
    if p.returncode != 0:
        raise ValueError(
            f"pip freeze failed, return code {p.returncode}: {stderr.decode()}"
        )

    return stdout.decode("utf-8").strip()


_PIP_ENVIRONMENT_TIMEOUT = 10 * 60


async def get_cached_or_create_pip_environment(
    environment_hash: str,
    working_directory: Optional[str],
    requirements_file_path: str,
    prerequisites: EnvironmentSpecPrerequisites,
    new_environment_path: str,
    try_get_file: Callable[[str, str], Awaitable[bool]],
    upload_file: Callable[[str, str], Awaitable[None]],
    editable_install: bool,
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
                requirements_file_path,
                new_environment_path,
                prerequisites,
                working_directory,
                filter_editable_install=not editable_install,
            )
        except BaseException:
            remove_corrupted_environment(new_environment_path)
            raise

        is_packed = pack_venv(
            new_environment_path, local_cached_file, "pip", editable_install
        )
        if not is_packed:
            return new_environment_interpreter

        await upload_file(local_cached_file, remote_cached_file_name)

        return new_environment_interpreter


async def create_pip_environment(
    requirements_file_path: str,
    new_environment_path: str,
    prerequisites: EnvironmentSpecPrerequisites,
    working_directory: Optional[str],
    filter_editable_install: bool,
) -> None:
    """
    Creates a pip environment in new_environment_path. requirements_file_path should
    point to a requirements.txt file that will determine what packages are installed.

    There should usually be a filelock around this function.

    If filter_editable_install is False, and there are editable installs in the
    requirements file, working_directory must be set to where the source is, because
    editable installs are specified relative to it.
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

    prerequisite_pip_packages = ["wheel"]
    if prerequisites & EnvironmentSpecPrerequisites.GOOGLE_AUTH:
        prerequisite_pip_packages.append(GOOGLE_AUTH_PACKAGE)

    return_code = await (
        await asyncio.create_subprocess_exec(
            *pip_install,
            *prerequisite_pip_packages,
            env=disable_pip_version_check,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing pre-requirements for {requirements_file_path} in "
            f"{new_environment_path} failed with return code {return_code}"
        )

    if filter_editable_install:
        requirements_file_path = filter_editable_installs_from_pip_reqs(
            requirements_file_path
        )

    return_code = await (
        await asyncio.create_subprocess_exec(
            *pip_install,
            "-r",
            requirements_file_path,
            env=disable_pip_version_check,
            cwd=working_directory,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing requirements from {requirements_file_path} in "
            f"{new_environment_path} failed with return code {return_code}"
        )


def filter_editable_installs_from_pip_reqs(requirements_file_path: str) -> str:
    with open(requirements_file_path, "r", encoding="utf-8") as orig:
        orig_lines = orig.readlines()
    path, file = os.path.split(requirements_file_path)
    requirements_file_path = os.path.join(path, "filtered-" + file)
    with open(requirements_file_path, "w", encoding="utf-8") as flt:
        for orig_line in orig_lines:
            line = orig_line.strip()
            if line.startswith("-e") and "git+" not in line:
                continue
            flt.write(orig_line)
    return requirements_file_path
