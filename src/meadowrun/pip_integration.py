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
