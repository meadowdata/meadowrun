from __future__ import annotations

import glob
import os
import shutil
import tempfile
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import Literal


def pack_venv(
    prefix: str,
    local_cached_file: str,
    name: Literal["pip", "poetry"],
    allow_editable_install: bool,
) -> bool:
    try:
        import venv_pack  # see note on reference in pyproject.toml
    except ImportError:
        print("Warning unable to cache pip environment because venv_pack is missing")
        return False

    # TODO we shouldn't wait for this to start running the job but we also shouldn't
    # kill the container until this finishes

    print(f"Caching the {name} environment")

    try:
        # there might be an old file from a failed attempt
        os.remove(local_cached_file)
    except OSError:
        pass

    if allow_editable_install:
        py_lib, _ = venv_pack.core.find_python_lib_include(prefix)
        site_packages = os.path.join(prefix, py_lib, "site-packages")
        cleanup_editable_install(prefix, site_packages)

    venv_pack.pack(prefix, output=local_cached_file)
    return True


def pack_conda_env(
    prefix: str, local_cached_file: str, allow_editable_install: bool
) -> bool:
    try:
        import conda_pack  # see note on reference in pyproject.toml
    except ImportError:
        print("Warning unable to cache conda environment because conda_pack is missing")
        return False

    # TODO we shouldn't wait for this to start running the job but we also shouldn't
    # kill the container until this finishes
    print("Caching the conda environment")

    try:
        # there might be an old file from a failed attempt
        os.remove(local_cached_file)
    except OSError:
        pass

    if allow_editable_install:
        site_packages = conda_pack.core.find_site_packages(prefix)
        cleanup_editable_install(prefix, os.path.join(prefix, site_packages))

    conda_pack.pack(prefix=prefix, output=local_cached_file)
    return True


def cleanup_editable_install(prefix: str, site_packages: str) -> None:
    with tempfile.TemporaryDirectory() as t:
        pth_files = glob.glob(os.path.join(site_packages, "*.pth"))
        for pth_fil in pth_files:
            dirname = os.path.dirname(pth_fil)
            temp_file = os.path.join(t, "temp.txt")
            with open(pth_fil, encoding="utf-8") as pth, open(
                temp_file,
                "w+",
                encoding="utf-8",
            ) as tmp:
                for line in pth:
                    if line.startswith("#") or line.startswith("import"):
                        tmp.write(line)
                        continue
                    line = line.rstrip()
                    if line:
                        location = os.path.normpath(os.path.join(dirname, line))
                        if location.startswith(prefix):
                            tmp.write(line)
                        else:
                            print(
                                f"Stripping editable install folder {line} from "
                                f"{pth_fil} so the environment can be packed and "
                                "cached."
                            )
            os.replace(temp_file, pth_fil)

    egg_lnk_files = glob.glob(os.path.join(site_packages, "*.egg-link"))
    for egg_link_file in egg_lnk_files:
        dirname = os.path.dirname(egg_link_file)
        temp_file = os.path.join(t, "temp.txt")
        with open(egg_link_file, encoding="utf-8") as egg_link:
            # https://setuptools.pypa.io/en/latest/deprecated/python_eggs.html#egg-links
            lines = egg_link.readlines()
            if len(lines) == 0:
                continue  # strange egg
            location = lines[0].rstrip()  # only the first line matters
            print(
                f"Copying egg-info for {egg_link_file} from "
                f"{location} so the environment can be packed and "
                "cached."
            )
            egg_infos = glob.glob(os.path.join(location, "*.egg-info"))
            for egg_info in egg_infos:
                shutil.copytree(
                    egg_info,
                    os.path.join(site_packages, os.path.basename(egg_info)),
                )
        os.remove(egg_link_file)
