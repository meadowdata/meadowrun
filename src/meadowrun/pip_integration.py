from __future__ import annotations

import asyncio
import os
import pkgutil
import sys


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


async def create_pip_environment(
    requirements_file_path: str, new_environment_path: str
) -> str:
    """
    Creates a pip environment in new_environment_path, returns the path to the python
    executable in the newly created environment (always
    new_environment_path/bin/python). requirements_file_path should point to a
    requirements.txt file that will determine what packages are installed.

    There should usually use filelock around this function.
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

    new_environment_interpreter = os.path.join(new_environment_path, "bin", "python")

    return_code = await (
        await asyncio.create_subprocess_exec(
            new_environment_interpreter,
            "-m",
            "pip",
            "install",
            "-r",
            requirements_file_path,
        )
    ).wait()
    if return_code != 0:
        raise ValueError(
            f"Installing requirements from {requirements_file_path} in "
            f"{new_environment_path} failed with return code {return_code}"
        )

    return new_environment_interpreter
