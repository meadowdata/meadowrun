import os
import tempfile
import zipfile

import pytest
from meadowrun import local_code


@pytest.mark.parametrize(
    "paths,rel_to_local,rel_paths",
    [
        (  # simplest case: one root with a few subfolders
            ["/usr/lib", "/usr/lib/python", "/usr/lib/python/more"],
            {"/usr/lib": "lib"},
            ["lib", "lib/python", "lib/python/more"],
        ),
        (  # two roots each with a subfolder
            ["/usr/lib", "/usr/lib/python", "/home/foo/mdr", "/home/foo/mdr/src"],
            {"/usr/lib": "lib", "/home/foo/mdr": "mdr"},
            ["lib", "lib/python", "mdr", "mdr/src"],
        ),
        (  # two roots without subfolders, but same name
            [
                "/home/user/lib2/src",
                "/home/user/lib1/src",
            ],
            {"/home/user/lib1/src": "src", "/home/user/lib2/src": "src_0"},
            ["src_0", "src"],
        ),
        (  # two roots with subfolders, same name
            [
                "/home/user/lib2/src",
                "/home/user/lib1/src",
                "/home/user/lib1/src/foo",
                "/home/user/lib2/src/bar",
            ],
            {"/home/user/lib1/src": "src", "/home/user/lib2/src": "src_0"},
            ["src_0", "src", "src/foo", "src_0/bar"],
        ),
    ],
)
def test_consolidate_paths(paths, rel_to_local, rel_paths):
    actual_rel_to_local, actual_rel_paths = local_code._consolidate_paths_to_zip(paths)
    assert rel_to_local == actual_rel_to_local
    assert rel_paths == actual_rel_paths


def test_zip_file():
    with tempfile.TemporaryDirectory() as temp:
        result = local_code.zip(temp)
        assert result is not None
        zip_file, zip_paths = result
        with zipfile.ZipFile(zip_file) as zip:
            for file in zip.filelist:
                ext = os.path.splitext(file.filename)[1]
                assert ext == ".py"
                assert any(zip_path in file.filename for zip_path in zip_paths)
