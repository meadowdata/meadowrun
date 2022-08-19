import os.path
import pkgutil
import shutil
import string
import subprocess
from typing import Iterable

import toml
import yaml


PYPI_TO_CONDA_PACKAGE_NAME_TRANSLATION = {"kubernetes-asyncio": "kubernetes_asyncio"}


def translate_poetry_version_to_conda(v: str) -> str:
    if v[0] == "^":
        min_version = v[1:]
        version_parts = min_version.split(".")
        max_version = (
            str(int(version_parts[0]) + 1)
            + "."
            + ".".join("0" for _ in version_parts[1:])
        )
        return f">={min_version},<{max_version}"
    elif v[0] in string.digits:
        # strings like 1.21.21 need to be translated to ==1.21.21
        return f"=={v}"
    else:
        raise NotImplementedError(
            f"Version strings of the format {v} are not yet supported"
        )


def list_packages(path: str, prefix: str) -> Iterable[str]:
    for module in pkgutil.iter_modules([path]):
        yield f"{prefix}{module.name}"
        if module.ispkg:
            yield from list_packages(
                os.path.join(path, module.name), f"{prefix}{module.name}."
            )


def generate_conda_yaml() -> None:
    pyproject = toml.load("pyproject.toml")

    version = pyproject["tool"]["poetry"]["version"]

    dependencies = [
        f"{PYPI_TO_CONDA_PACKAGE_NAME_TRANSLATION.get(key, key)} "
        f"{translate_poetry_version_to_conda(value)}"
        for key, value in pyproject["tool"]["poetry"]["dependencies"].items()
    ]

    meta_yaml = {
        "package": {"name": "meadowrun", "version": version},
        "source": {"path": f"meadowrun-{version}"},
        "build": {
            "entry_points": [
                f"{key} = {value}"
                for key, value in pyproject["tool"]["poetry"]["scripts"].items()
            ],
            "noarch": "python",
            "number": "0",
            "script": "python.exe -m pip install . -vv",
            "string": "py_0",
        },
        "requirements": {"host": dependencies + ["poetry"], "run": dependencies},
        "test": {
            "commands": [
                f"{key} --help" for key in pyproject["tool"]["poetry"]["scripts"]
            ],
            "imports": list(list_packages("src", "")),
        },
        "about": {
            "dev_url": "https://github.com/meadowdata/meadowrun",
            "doc_url": "https://docs.meadowrun.io",
            "home": "https://meadowrun.io",
            "summary": (
                "Meadowrun automates the tedious details of running your python code on"
                " AWS or Azure"
            ),
        },
        "extra": {"copy_test_source_files": True, "final": True},
    }

    with open("dist/meta.yaml", "w", encoding="utf-8") as f:
        yaml.dump(meta_yaml, f)


def main() -> None:
    result = subprocess.run(["poetry", "version", "--short"], capture_output=True)
    version = result.stdout.strip().decode("utf-8")

    # build the pip package
    subprocess.run(["poetry", "build"])

    # Unpack the resulting meadowrun-<version>.tar.gz file into
    # /dist/meadowrun-<version>
    shutil.rmtree(f"dist/meadowrun-{version}", ignore_errors=True)
    subprocess.run(["tar", "xzvf", f"dist/meadowrun-{version}.tar.gz", "-C", "dist"])

    # generate the conda meta.yaml file
    generate_conda_yaml()

    # Build the conda package
    subprocess.run(
        [
            "conda",
            "build",
            "-c",
            "defaults",
            "-c",
            "conda-forge",
            "--python",
            "3.9",
            "dist",
        ],
        shell=True,
    )

    # Instructions for testing and publishing
    print(
        "To test this, you can install this package into a conda environment by running"
    )
    print(
        r"conda install meadowrun -c c:\bin\Miniconda\conda-bld -c defaults -c "
        "conda-forge"
    )
    print(r"(obviously replace c:\bin\Miniconda with your conda installation)")

    print("To publish this image, run")
    print(
        "conda activate base && anaconda upload "
        r"C:\bin\Miniconda\conda-bld\noarch\meadowrun-0.2.1-py_0.tar.bz2"
    )

    print(
        "If you're running this for the first time, you'll need to run `anaconda login`"
        " sbefore you run `anaconda upload`"
    )


if __name__ == "__main__":
    main()
