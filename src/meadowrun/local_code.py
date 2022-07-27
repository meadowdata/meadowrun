import itertools
import os
from os.path import realpath, join, splitext
import sys
import uuid
import zipfile
from typing import Dict, Iterable, List, Set, Tuple


def zip_local_code(
    result_zip_dir: str,
    include_sys_path: bool = True,
    additional_python_paths: Iterable[str] = tuple(),
    additional_non_python_paths: Iterable[str] = tuple(),
    include_extensions: Iterable[str] = (".py",),
) -> Tuple[str, List[str], List[str]]:
    """
    The goal of this function is to zip up "the current local python code excluding the
    interpreter code" as well as any additional non-python files that might be needed to
    run code remotely.

    There are two types of folders/files that make it into the zip file. The first are
    "python paths". Assuming include_sys_path is True, "python paths" will be
    additional_python_paths + paths in sys.path that are NOT part of the interpreter,
    i.e. paths that start with sys.prefix, etc. The second are "non-python paths". These
    two types of paths are treated exactly the same in the zip file. The only difference
    between these two types of paths is that we keep track of them separately in the
    return value--the returned tuple will include a list of "python paths" as they
    appear in the zip file and "non-python paths" as they appear in the zip file.

    Non-python paths are useful for two reasons:
    - One is to add structure to the zip file. This function will consolidate paths that
      share a prefix. So for example, if `src/foo` and `src/bar/baz` are python paths,
      but `src` itself is not a python path, you can set `src` as a non-python path and
      your zip file will have the right structure. If you don't do this, `foo` and `baz`
      will just be top-level directories in the zip file.
    - You'll usually want to include the current working directory as a "non-python
      path" if it's not already a python path. Then when you unzip, you can set the
      working directory to the corresponding unzipped directory and relative paths will
      work as expected.

    Both python paths and non-python paths will only include *.py files (or whatever is
    specified in include_extensions).

    Args:
        result_zip_dir: directory where to put the resulting zip file.
        include_sys_path: Whether to zip paths on sys.path. Paths that are also in
            sys.prefix or sys.base_prefix are automatically excluded. Defaults to True.
        additional_python_paths: Additional python paths to zip. Defaults to empty
            tuple, i.e. no additional python paths.
        additional_non_python_paths: Additional paths to zip. Defaults to empty tuple,
            i.e. no additional non-python paths.
        include_extensions: Only files with these extensions are zipped. Defaults to
            (".py",), i.e. only python files.

    Raises:
        ValueError: If there are no paths to zip.

    Returns:
        Tuple[str, List[str], List[str]]: path to zip file in result_zip_dir, python
        paths in the zip file, non-python paths in the zip file
    """

    python_paths_to_zip, non_python_paths_to_zip = _get_paths_to_zip(
        additional_python_paths, include_sys_path, additional_non_python_paths
    )
    if not python_paths_to_zip and not non_python_paths_to_zip:
        raise ValueError("No paths to zip")

    (
        real_paths_to_zip_paths,
        python_zip_paths,
        non_python_zip_paths,
    ) = _consolidate_paths_to_zip(python_paths_to_zip, non_python_paths_to_zip)

    # Currently we always zip all the files and then compute the hash. The hash is just
    # used to avoid upload to S3. In future we could try to split the files into chunks
    # or even individually, to only upload what we need. For small-ish codebases zipping
    # and hashing is fast though, certainly relative to EC2 startup times and container
    # building, so this is not currently a bottleneck.
    zip_file_path = join(result_zip_dir, str(uuid.uuid4()) + ".zip")
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        for real_path, zip_path in real_paths_to_zip_paths.items():
            for dirpath, _, filenames in os.walk(real_path):
                for filename in filenames:
                    ext = splitext(filename)[1]
                    if ext in include_extensions:
                        full = join(dirpath, filename)
                        full_zip = full.replace(real_path, zip_path, 1)
                        zip_file.write(full, full_zip)

    return zip_file_path, python_zip_paths, non_python_zip_paths


def _get_paths_to_zip(
    python_paths_to_zip: Iterable[str],
    include_sys_path: bool,
    non_python_paths_to_zip: Iterable[str],
) -> Tuple[List[str], List[str]]:
    python_paths_to_zip = [realpath(path) for path in python_paths_to_zip]
    if include_sys_path:
        for candidate in sys.path:
            if not (
                candidate.startswith(sys.prefix)
                or candidate.startswith(sys.base_prefix)
            ):
                if candidate == "":
                    # empty path on sys.path means cwd
                    python_paths_to_zip.append(realpath(os.getcwd()))
                else:
                    python_paths_to_zip.append(realpath(candidate))
    non_python_paths_to_zip = [realpath(path) for path in non_python_paths_to_zip]
    return python_paths_to_zip, non_python_paths_to_zip


def _consolidate_paths_to_zip(
    real_python_paths_to_zip: Iterable[str], real_non_python_paths_to_zip: Iterable[str]
) -> Tuple[Dict[str, str], List[str], List[str]]:
    """Generates a minimal dictionary of real paths that need to be zipped, to the paths
    in the zip file.
    """

    # This whole rigmarole because:
    # 1. Any folders that are subfolders of folders already zipped needn't be zipped
    # 2. Need unique top-level folder name in zip file, that isn't full path
    # 3. each path in real_paths_to_zip must map to a folder in zip file.

    # remembers and updates used top-level folder name in the zip, and generates unique
    # names (keeping the original as much as possible)
    def find_unused_path(used_paths: Set[str], path_base: str) -> str:
        candidate = path_base
        for i in range(100):
            if candidate not in used_paths:
                used_paths.add(candidate)
                return candidate
            candidate = f"{path_base}_{i}"
        raise Exception(f"Gave up after 100 paths with base {path_base}")

    # sorting so shortest (i.e. superset) paths come first
    root_candidates = tuple(
        sorted(itertools.chain(real_python_paths_to_zip, real_non_python_paths_to_zip))
    )

    current_root = root_candidates[0]
    zip_path = os.path.basename(current_root)
    real_paths_to_zip_paths = {current_root: zip_path}
    real_root_paths = [current_root]
    used_zip_paths = {zip_path}

    for candidate in root_candidates:
        if candidate.startswith(current_root):
            # we have a subpath. Record it.
            real_paths_to_zip_paths[candidate] = candidate.replace(
                current_root, real_paths_to_zip_paths[current_root], 1
            )
        else:
            # we have a new root.
            current_root = candidate
            # give it a unique name in the zip.
            zip_path = find_unused_path(used_zip_paths, os.path.basename(current_root))
            real_paths_to_zip_paths[current_root] = zip_path
            real_root_paths.append(current_root)

    # same order as input paths, but paths in the zip
    zip_python_paths = [
        real_paths_to_zip_paths[real_path] for real_path in real_python_paths_to_zip
    ]
    zip_non_python_paths = [
        real_paths_to_zip_paths[real_path] for real_path in real_non_python_paths_to_zip
    ]

    # keeping just the root paths - these are the only ones that need to be zipped
    root_paths_to_zip_paths = {
        root: zip_path
        for root, zip_path in real_paths_to_zip_paths.items()
        if root in real_root_paths
    }
    return root_paths_to_zip_paths, zip_python_paths, zip_non_python_paths
