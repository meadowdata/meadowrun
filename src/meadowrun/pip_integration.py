from __future__ import annotations

import asyncio
import os
import pkgutil
import sys
from typing import Callable

import filelock


async def _pip_freeze(python_interpreter: str) -> str:
    """Effectively returns the output of <python_interpreter> -m pip freeze"""

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
    p = await asyncio.create_subprocess_exec(
        python_interpreter,
        *("-m", "pip", "freeze"),
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


async def pip_freeze_without_local_other_interpreter(python_interpreter: str) -> str:
    """
    Returns the result of calling pip freeze on the specified python_interpreter, but
    tries to exclude packages that won't be available on PyPI or otherwise won't be
    available on a different machine because they're "editable installs". The local code
    deployment should take care of the editable installs.
    """
    # TODO we have not actually implemented the portable part of this yet
    return await _pip_freeze(python_interpreter)


async def pip_freeze_without_local_current_interpreter() -> str:
    """
    Returns the result of calling pip freeze on the current interpreter, but tries to
    exclude packages that won't be available on PyPI or otherwise won't be available on
    a different machine because they're "editable installs". The local code deployment
    should take care of the editable installs.
    """
    packages_to_skip = set()
    for package in pkgutil.iter_modules():
        package_path = getattr(package.module_finder, "path", "")
        if not package_path.startswith(sys.prefix) and not package_path.startswith(
            sys.base_prefix
        ):
            packages_to_skip.add(package.name)

    # now run pip freeze
    raw_result = await _pip_freeze(sys.executable)

    processed_result = []
    for line in raw_result.splitlines():
        package_name, sep, version = line.partition("==")
        if sep != "==" or package_name not in packages_to_skip:
            processed_result.append(line)

    return "\n".join(processed_result)


_PIP_ENVIRONMENT_TIMEOUT = 10 * 60


async def get_cached_or_create_pip_environment(
    environment_hash: str,
    requirements_file_path: str,
    new_environment_path: str,
    try_get_file: Callable[[str, str], bool],
    upload_file: Callable[[str, str], None],
) -> str:
    """
    If the desired pip environment exists, does nothing. If the environment has been
    cached, creates it from the cache. Otherwise creates the environment from scratch
    and caches it. Returns the path to the newly created python interpreter.

    try_get_file(remote_file_name: str, local_file_name: str) -> bool. Tries to download
    the specified remote file to the specified local file name. Returns True if the file
    is available, False if the file is not available.

    upload_file(local_file_name: str, remote_file_name: str) -> None. Uploads the
    specified file, overwrites any existing remote file.
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
            f"{environment_hash}-{sys.version_info.major}.{sys.version_info.minor}"
            ".tar.gz"
        )
        local_cached_file = f"{new_environment_path}.tar.gz"
        download_succeeded = try_get_file(remote_cached_file_name, local_cached_file)
        if download_succeeded:
            try:
                print("Unpacking cached pip environment")
                os.makedirs(new_environment_path, exist_ok=True)
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
            finally:
                try:
                    os.remove(local_cached_file)
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    pass

        print("Creating the pip environment")
        await create_pip_environment(requirements_file_path, new_environment_path)

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
        venv_pack.pack(new_environment_path, output=local_cached_file)
        upload_file(local_cached_file, remote_cached_file_name)

        return new_environment_interpreter


async def create_pip_environment(
    requirements_file_path: str, new_environment_path: str
) -> None:
    """
    Creates a pip environment in new_environment_path. requirements_file_path should
    point to a requirements.txt file that will determine what packages are installed.

    There should usually be a filelock around this function.
    """
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

    return_code = await (
        await asyncio.create_subprocess_exec(
            os.path.join(new_environment_path, "bin", "python"),
            "-m",
            "pip",
            "install",
            "-r",
            requirements_file_path,
            env={"PIP_DISABLE_PIP_VERSION_CHECK": "1"},
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing requirements from {requirements_file_path} in "
            f"{new_environment_path} failed with return code {return_code}"
        )
