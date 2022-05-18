import asyncio
import json
import os
from typing import List, Optional, Tuple


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
        # cwd=cwd,
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

    # TODO lookup whether we should specify an encoding here?
    return stdout.decode(), stderr.decode()


async def env_export(name_or_path: Optional[str] = None) -> str:
    """Runs `conda env export` on the conda environment with the given name, and returns the
    results.

    :param name: name or full path to the conda environment - if None, tries to use the
        currently activated conda environment.
    :return: Output of the conda env export command.
    """
    if name_or_path is None:
        env_path = os.environ.get("CONDA_PREFIX")
        if env_path is None:
            raise ValueError(
                "name_or_path must be given if there is no activated conda environment "
                "(CONDA_PREFIX is not set)"
            )
    else:
        out, _ = await _run(["env", "list", "--json"])
        result = json.loads(out)
        envs: List[str] = result["envs"]
        if len(envs) == 0:
            raise ValueError("No conda environments found")
        elif len(envs) == 1:
            # praise conda: the first path is always the base path
            if name_or_path != "base":
                raise ValueError(
                    f"Conda environment {name_or_path} not found - only base"
                    " environment is defined"
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
