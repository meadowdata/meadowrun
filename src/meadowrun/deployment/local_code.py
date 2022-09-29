import glob
import itertools
import os
import sys
import uuid
import zipfile
from os.path import realpath, join, splitext
from typing import Iterable, List, Set, Tuple


def zip_local_code(
    result_zip_dir: str,
    include_sys_path: bool = True,
    additional_python_paths: Iterable[str] = tuple(),
    python_paths_extensions: Iterable[str] = (".py", ".pyd"),
    working_directory_globs: Iterable[str] = tuple(),
) -> Tuple[str, List[str], str]:
    """
    The goal of this function is to zip up "the current local python code excluding the
    interpreter code" as well as any specified files in the current working directory.

    There are two types of folders/files that make it into the zip file. The first are
    "python paths". Assuming include_sys_path is True, "python paths" will be
    additional_python_paths + paths in sys.path that are NOT part of the interpreter,
    i.e. paths that do NOT start with sys.prefix, etc. The second type are files
    specified by working_directory_globs which are always relative to the current
    working directory.

    python paths will only include *.py and *.pyd files (or whatever is specified in
    python_paths_extensions).

    Args:
        result_zip_dir: directory where to put the resulting zip file.
        include_sys_path: Whether to zip paths on sys.path. Paths that are also in
            sys.prefix or sys.base_prefix are automatically excluded. Defaults to True.
        additional_python_paths: Additional python paths to zip. Defaults to empty
            tuple, i.e. no additional python paths.
        python_paths_extensions: Only files with these extensions are zipped. Defaults
            to (".py", ".pyd"), i.e. only python files.
        working_directory_globs: Globs that will be passed directly to glob.glob to
            specify any file relative to the current working directory. Globs will only
            match files.

    Raises:
        ValueError: If there are no paths to zip.

    Returns:
        Tuple[str, List[str], str]: path to zip file in result_zip_dir, python paths in
        the zip file, current working directory in the zip file
    """
    current_working_directory = realpath(os.getcwd())
    non_python_paths_to_zip = [current_working_directory]
    current_working_directory_parent = os.path.dirname(current_working_directory)
    # this will hold (path_to_file, path_to_containing_dir) for every file that matches
    # a glob
    real_file_paths_to_zip = []
    for file_glob in working_directory_globs:
        for file in glob.glob(file_glob, recursive=True):
            if os.path.isfile(file):
                file_real_path = realpath(file)
                if not file_real_path.startswith(current_working_directory_parent):
                    raise ValueError(
                        "Specifying files outside of the current working directory's "
                        "parent is not supported. I.e. ../foo is fine, but ../../foo "
                        "will not work"
                    )
                dir_real_path = os.path.dirname(file_real_path)
                real_file_paths_to_zip.append((file_real_path, dir_real_path))
                non_python_paths_to_zip.append(dir_real_path)

    real_python_paths_to_zip = _get_python_paths_to_zip(
        additional_python_paths, include_sys_path
    )

    (
        python_zip_paths,
        non_python_zip_paths,
        real_python_root_paths_to_zip_paths,
    ) = _consolidate_paths_to_zip(real_python_paths_to_zip, non_python_paths_to_zip)

    # Currently we always zip all the files and then compute the hash. The hash is just
    # used to avoid upload to S3. In future we could try to split the files into chunks
    # or even individually, to only upload what we need. For small-ish codebases zipping
    # and hashing is fast though, certainly relative to EC2 startup times and container
    # building, so this is not currently a bottleneck.
    zip_file_path = join(result_zip_dir, str(uuid.uuid4()) + ".zip")
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        for real_path, zip_path in real_python_root_paths_to_zip_paths:
            for dirpath, _, filenames in os.walk(real_path):
                for filename in filenames:
                    if splitext(filename)[1].lower() in python_paths_extensions:
                        full = join(dirpath, filename)
                        full_zip = full.replace(real_path, zip_path, 1)
                        zip_file.write(full, full_zip)

        for (file_real_path, dir_real_path), dir_zip_path in zip(
            real_file_paths_to_zip, itertools.islice(non_python_zip_paths, 1, None)
        ):
            file_zip_path = file_real_path.replace(dir_real_path, dir_zip_path, 1)
            if os.path.sep != "/":
                # we have to make the sub-paths compatible with Linux which is our
                # target OS
                file_zip_path = file_zip_path.replace(os.path.sep, "/")
            zip_file.write(file_real_path, file_zip_path)

    return zip_file_path, python_zip_paths, non_python_zip_paths[0]


def _get_python_paths_to_zip(
    python_paths_to_zip: Iterable[str],
    include_sys_path: bool,
) -> List[str]:
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
    return python_paths_to_zip


def _consolidate_paths_to_zip(
    real_python_paths_to_zip: Iterable[str], real_non_python_paths_to_zip: Iterable[str]
) -> Tuple[List[str], List[str], List[Tuple[str, str]]]:
    """
    Takes some real paths, and returns the path they should be in the zip. Some example
    inputs/outputs of real paths to zip paths:
    - [a/b/c, a/b] -> [b/c, b] (don't make c a separate directory because it's already
      included in b
    - [a/b, d/b] ->  [b, b_0] (we have two b folders that we need to keep separate)

    Return values are python_zip_paths, non_python_zip_paths, python_roots

    python_zip_paths and non_python_zip paths correspond 1-to-1 with the elements in
    real_python_paths_to_zip and real_non_python_paths_to_zip.

    python_roots is a subset of zip(real_python_paths_to_zip, python_zip_paths) that are
    "roots". E.g. in the first example above, this will just return [(a/b, b)].
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
            zip_path = candidate.replace(
                current_root, real_paths_to_zip_paths[current_root], 1
            )
            if os.path.sep != "/":
                # we have to make the sub-paths compatible with Linux which is our
                # target OS
                zip_path = zip_path.replace(os.path.sep, "/")
            real_paths_to_zip_paths[candidate] = zip_path
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

    # consolidate python paths
    python_root_candidates = tuple(sorted(real_python_paths_to_zip))
    current_root = python_root_candidates[0]
    real_python_root_paths = [current_root]
    for candidate in python_root_candidates:
        if not candidate.startswith(current_root):
            # we have a new root.
            current_root = candidate
            real_python_root_paths.append(current_root)

    return (
        zip_python_paths,
        zip_non_python_paths,
        [
            (real_path, real_paths_to_zip_paths[real_path])
            for real_path in real_python_root_paths
        ],
    )
