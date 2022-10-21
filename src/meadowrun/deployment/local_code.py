import asyncio
import glob
import itertools
import os
import sys
import uuid
import zipfile
from os.path import join, realpath, splitext
from typing import Iterable, List, Set, Tuple

from meadowrun.shared import create_zipfile


def zip_local_code(
    result_zip_dir: str,
    include_sys_path: bool = True,
    additional_python_paths: Iterable[str] = tuple(),
    python_paths_extensions: Iterable[str] = (".py", ".so"),
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

    python paths will only include *.py and *.so files (or whatever is specified in
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

    # non_python_dirs will be all directories that contain one or more non-python files
    # that we want to include.
    # current_working_directory doesn't really belong here because we're not necessarily
    # going to zip anything in this directory, we just include it here so that we can
    # figure out what the current working directory will be in the resulting zip file
    non_python_dirs = [current_working_directory]

    # this will hold (path_to_file, path_to_containing_dir) for every non-python file
    # that matches a glob
    non_python_files = []

    current_working_directory_parent = os.path.dirname(current_working_directory)
    working_directory_files = (
        file
        for file_glob in working_directory_globs
        for file in glob.glob(file_glob, recursive=True)
    )
    for file_path in working_directory_files:
        if os.path.isfile(file_path):
            file_real_path = realpath(file_path)
            if not file_real_path.startswith(current_working_directory_parent):
                raise ValueError(
                    "Specifying files outside of the current working directory's parent"
                    " is not supported. I.e. ../foo is fine, but ../../foo will not "
                    "work"
                )
            dir_real_path = os.path.dirname(file_real_path)
            non_python_files.append((file_real_path, dir_real_path))
            non_python_dirs.append(dir_real_path)

    python_dirs = _get_python_dirs_to_zip(additional_python_paths, include_sys_path)

    (
        # all directories that need to be added to the python path in the zip file
        python_dirs_as_zip_paths,
        # corresponds to non_python_files such that non_python_files[i] needs to be
        # placed into the zip file in the non_python_files_as_dirs_in_zip[i+1] directory
        non_python_files_as_dirs_in_zip,
        # a set of directories such that if you os.walk all of these directories, you
        # will traverse every python_dir exactly once
        python_root_dirs_and_zip_paths,
    ) = _consolidate_paths_to_zip(python_dirs, non_python_dirs)

    zip_file_path = join(result_zip_dir, str(uuid.uuid4()) + ".zip")
    # ZIP_DEFLATED because it's the fastest. All code gets zipped every time, even when
    # it hasn't changed, so this needs to be fast above all else.
    with create_zipfile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for real_path, zip_path in python_root_dirs_and_zip_paths:
            for dirpath, _, filenames in os.walk(real_path):
                for filename in filenames:
                    if splitext(filename)[1].lower() in python_paths_extensions:
                        full = join(dirpath, filename)
                        full_zip = full.replace(real_path, zip_path, 1)
                        try:
                            zip_file.write(full, full_zip)
                        except asyncio.CancelledError:
                            raise
                        except BaseException:
                            print(
                                f"Warning, skipping file {full} that cannot be added to"
                                f" the zip file as {full_zip}"
                            )

        for (file_real_path, dir_real_path), dir_zip_path in zip(
            non_python_files, itertools.islice(non_python_files_as_dirs_in_zip, 1, None)
        ):
            file_zip_path = file_real_path.replace(dir_real_path, dir_zip_path, 1)
            if os.path.sep != "/":
                # we have to make the sub-paths compatible with Linux which is our
                # target OS
                file_zip_path = file_zip_path.replace(os.path.sep, "/")

            try:
                zip_file.write(file_real_path, file_zip_path)
            except asyncio.CancelledError:
                raise
            except BaseException:
                print(
                    f"Warning, skipping file {full} that cannot be added to the zip "
                    f"file as {full_zip}"
                )

    return zip_file_path, python_dirs_as_zip_paths, non_python_files_as_dirs_in_zip[0]


def _get_python_dirs_to_zip(
    python_dirs_to_zip: Iterable[str],
    include_sys_path: bool,
) -> List[str]:
    python_dirs_to_zip = [realpath(path) for path in python_dirs_to_zip]
    if include_sys_path:
        for candidate in sys.path:
            if not (
                candidate.startswith(sys.prefix)
                or candidate.startswith(sys.base_prefix)
            ):
                if candidate == "":
                    # empty path on sys.path means cwd
                    python_dirs_to_zip.append(realpath(os.getcwd()))
                else:
                    python_dirs_to_zip.append(realpath(candidate))
    return python_dirs_to_zip


def _consolidate_paths_to_zip(
    python_dirs: Iterable[str], non_python_dirs: Iterable[str]
) -> Tuple[List[str], List[str], List[Tuple[str, str]]]:
    """
    Takes some real paths, and returns the path they should be in the zip. Some example
    inputs/outputs of real paths to zip paths:
    - [a/b/c, a/b] -> [b/c, b] (don't make c a separate directory because it's already
      included in b
    - [a/b, d/b] ->  [b, b_0] (we have two b folders that we need to keep separate)

    Return values are python_dirs_as_zip_paths, non_python_files_as_dirs_in_zip,
    python_root_dirs_and_zip_paths

    python_dirs_as_zip_paths and non_python_files_as_dirs_in_zip paths correspond 1-to-1
    with the elements in python_dirs and non_python_dirs.

    python_root_dirs_and_zip_paths is a subset of zip(python_dirs,
    python_dirs_as_zip_paths) that are "roots". E.g. in the first example above, this
    will just return [(a/b, b)].
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
    root_candidates = tuple(sorted(itertools.chain(python_dirs, non_python_dirs)))

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
    zip_python_paths = [real_paths_to_zip_paths[real_path] for real_path in python_dirs]
    zip_non_python_paths = [
        real_paths_to_zip_paths[real_path] for real_path in non_python_dirs
    ]

    # consolidate python paths
    python_root_candidates = tuple(sorted(python_dirs))
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
