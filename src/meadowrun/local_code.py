import os
from os.path import realpath, join, splitext
import sys
import uuid
import zipfile
from typing import Dict, Iterable, List, Set, Tuple


def zip(
    working_dir: str,
    include_sys_path: bool = True,
    additional_paths: Iterable[str] = tuple(),
    extensions: Iterable[str] = (".py",),
) -> Tuple[str, List[str]]:

    paths_to_zip = _get_paths_to_zip(additional_paths, include_sys_path)
    if not paths_to_zip:
        raise ValueError("No paths to zip")

    real_paths_to_zip_paths, zip_paths = _consolidate_paths_to_zip(paths_to_zip)

    # Currently we always zip all the files and the compute the hash. The hash is just
    # used to avoid upload to S3. In future we could try to split the files into chunks
    # or even individually, to only upload what we need. For small-ish codebases zipping
    # and hashing is fast though, certainly relative to EC2 startup times and container
    # building, so this is not currently a bottleneck.
    zip_file_path = join(working_dir, str(uuid.uuid4()) + ".zip")
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        for real_path, zip_path in real_paths_to_zip_paths.items():
            for (dirpath, _, filenames) in os.walk(real_path):
                for filename in filenames:
                    ext = splitext(filename)[1]
                    if ext in extensions:
                        full = join(dirpath, filename)
                        full_zip = full.replace(real_path, zip_path, 1)
                        zip_file.write(
                            full,
                            full_zip,
                        )

    return zip_file_path, zip_paths


def _get_paths_to_zip(paths_to_zip: Iterable[str], include_sys_path: bool) -> List[str]:
    paths_to_zip = [realpath(path) for path in paths_to_zip]
    if include_sys_path:
        for candidate in sys.path:
            if not (
                candidate.startswith(sys.prefix)
                or candidate.startswith(sys.base_prefix)
            ):
                if candidate == "":
                    # empty path on sys.path means cwd
                    paths_to_zip.append(realpath(os.getcwd()))
                else:
                    paths_to_zip.append(realpath(candidate))
    return paths_to_zip


def _consolidate_paths_to_zip(
    real_paths_to_zip: Iterable[str],
) -> Tuple[Dict[str, str], List[str]]:
    """Generates a minimal dictionary of real paths that need to be zipped, to the paths
    in the zip file. Also returns a list of paths in the zip file in the same order as
    real paths were given, to allow adding those to sys.path in the right order later
    on.
    """

    # This whole rigmarole because:
    # 1. Any folders that are subfolders of folders already zipped needn't be zipped
    # 2. Need unique top-level folder name in zip file, that isn't full path
    # 3. each path in paths_to_zip must map to a folder in zip file, and original order
    #    must be maintained, so paths can be added to sys.path in the right order.

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
    root_candidates = tuple(sorted(real_paths_to_zip))

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
    zip_paths = [real_paths_to_zip_paths[real_path] for real_path in real_paths_to_zip]
    # keeping just the root paths - these are the only ones that need to be zipped
    root_paths_to_zip_paths = {
        root: zip
        for (root, zip) in real_paths_to_zip_paths.items()
        if root in real_root_paths
    }
    return root_paths_to_zip_paths, zip_paths
