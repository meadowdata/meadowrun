import os
import tempfile
import zipfile

import pytest
from meadowrun import local_code


@pytest.mark.parametrize(
    "python_paths,other_paths,python_root_paths,expected_python_paths,"
    "expected_other_paths",
    [
        (  # simplest case: one root with a few subfolders
            ["/usr/lib", "/usr/lib/python", "/usr/lib/python/more"],
            [],
            {"/usr/lib": "lib"},
            ["lib", "lib/python", "lib/python/more"],
            [],
        ),
        (  # two roots each with a subfolder
            ["/usr/lib", "/usr/lib/python", "/home/foo/mdr", "/home/foo/mdr/src"],
            [],
            {"/usr/lib": "lib", "/home/foo/mdr": "mdr"},
            ["lib", "lib/python", "mdr", "mdr/src"],
            [],
        ),
        (  # two roots without subfolders, but same name
            ["/home/user/lib2/src", "/home/user/lib1/src"],
            [],
            {"/home/user/lib1/src": "src", "/home/user/lib2/src": "src_0"},
            ["src_0", "src"],
            [],
        ),
        (  # two roots with subfolders, same name
            [
                "/home/user/lib2/src",
                "/home/user/lib1/src",
                "/home/user/lib1/src/foo",
                "/home/user/lib2/src/bar",
            ],
            [],
            {"/home/user/lib1/src": "src", "/home/user/lib2/src": "src_0"},
            ["src_0", "src", "src/foo", "src_0/bar"],
            [],
        ),
        # -------- The same, but with extra paths ----------
        (  # simplest case: one root with a few subfolders
            ["/usr/lib/python", "/usr/lib/python/more"],
            ["/usr/lib"],
            {"/usr/lib/python": "lib/python"},
            ["lib/python", "lib/python/more"],
            ["lib"],
        ),
        (  # two roots each with a subfolder
            ["/usr/lib", "/usr/lib/python"],
            ["/home/foo/mdr", "/home/foo/mdr/src"],
            {"/usr/lib": "lib"},
            ["lib", "lib/python"],
            ["mdr", "mdr/src"],
        ),
        (  # two roots without subfolders, but same name
            ["/home/user/lib2/src"],
            ["/home/user/lib1/src"],
            {"/home/user/lib2/src": "src_0"},
            ["src_0"],
            ["src"],
        ),
        (  # two roots with subfolders, same name
            ["/home/user/lib2/src", "/home/user/lib2/src/bar"],
            ["/home/user/lib1/src", "/home/user/lib1/src/foo"],
            {"/home/user/lib2/src": "src_0"},
            ["src_0", "src_0/bar"],
            ["src", "src/foo"],
        ),
    ],
)
def test_consolidate_paths(
    python_paths,
    other_paths,
    python_root_paths,
    expected_python_paths,
    expected_other_paths,
):
    (
        actual_python_paths,
        actual_other_paths,
        actual_python_root_paths,
    ) = local_code._consolidate_paths_to_zip(python_paths, other_paths)
    assert len(actual_python_paths) == len(python_paths)
    assert len(actual_other_paths) == len(other_paths)

    assert python_root_paths == dict(actual_python_root_paths)
    assert expected_python_paths == actual_python_paths
    assert expected_other_paths == actual_other_paths


def test_zip_file():
    with tempfile.TemporaryDirectory() as temp:
        result = local_code.zip_local_code(temp)
        assert result is not None
        zip_file, zip_python_paths, cwd = result
        with zipfile.ZipFile(zip_file) as zip:
            all_paths = zip_python_paths + [cwd]
            for file in zip.filelist:
                ext = os.path.splitext(file.filename)[1]
                assert ext == ".py"
                assert any(zip_path in file.filename for zip_path in all_paths)


@pytest.mark.parametrize(
    "available_files,working_directory,working_directory_globs,python_paths,"
    "expected_files_in_zip,expected_python_paths_in_zip,"
    "expected_working_directory_in_zip",
    [
        # root is not on python path, but subdirectories are. Python files outside of
        # the subdirectories should be ignored. Also, non-.py files are ignored
        (
            [
                "root/a/b/x1.py",
                "root/a/c/x2.py",
                "root/a/x3.py",
                "root/x4.py",
                "root/a/x5.txt",
                "root/a/b/x6.txt",
            ],
            "root",
            (),
            ["root/a/b", "root/a/c"],
            ["root/a/b/x1.py", "root/a/c/x2.py"],
            ["root/a/b", "root/a/c"],
            "root",
        ),
        # simple case of include **/*.txt files
        (
            [
                "root/a/x1.py",
                "root/a/x2.txt",
                "root/b/x3.py",
                "root/b/x4.txt",
                "root/x5.txt",
            ],
            "root",
            ["**/*.txt"],
            ["root"],
            [
                "root/a/x1.py",
                "root/a/x2.txt",
                "root/b/x3.py",
                "root/b/x4.txt",
                "root/x5.txt",
            ],
            ["root"],
            "root",
        ),
        # same as above, but only include txt files in b
        (
            [
                "root/a/x1.py",
                "root/a/x2.txt",
                "root/b/x3.py",
                "root/b/x4.txt",
                "root/x5.txt",
            ],
            "root",
            ["b/**/*.txt"],
            ["root"],
            ["root/a/x1.py", "root/b/x3.py", "root/b/x4.txt"],
            ["root"],
            "root",
        ),
        # reference files outside of root
        (
            ["root/x1.py", "root/x2.txt", "other_root/x3.txt"],
            "root",
            ["../other_root/**/*.txt"],
            ["root"],
            ["root/x1.py", "other_root/x3.txt"],
            ["root"],
            "root",
        ),
    ],
)
def test_zip_local_code(
    available_files,
    working_directory,
    working_directory_globs,
    python_paths,
    expected_files_in_zip,
    expected_python_paths_in_zip,
    expected_working_directory_in_zip,
):
    with tempfile.TemporaryDirectory() as temp_source, tempfile.TemporaryDirectory() as temp_dest:  # noqa: E501
        # create the specified files
        for file in available_files:
            file_path = os.path.join(temp_source, file)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, mode="w", encoding="utf-8") as f:
                f.write("Hello!")

        orig_dir = os.getcwd()
        try:
            os.chdir(os.path.join(temp_source, working_directory))

            (
                result_zip_file,
                python_paths_in_zip,
                working_directory_in_zip,
            ) = local_code.zip_local_code(
                temp_dest,
                False,
                additional_python_paths=[
                    os.path.join(temp_source, p) for p in python_paths
                ],
                working_directory_globs=working_directory_globs,
            )

            with zipfile.ZipFile(result_zip_file, "r") as zf:
                assert set(zf.namelist()) == set(expected_files_in_zip)

            assert python_paths_in_zip == expected_python_paths_in_zip
            assert working_directory_in_zip == expected_working_directory_in_zip
        finally:
            os.chdir(orig_dir)


def test_zip_local_code_exception():
    with pytest.raises(ValueError, match="outside of the current working directory"):
        # reference files two folders outside of root does not work
        test_zip_local_code(
            ["nested/root/x1.py", "nested/root/x2.txt", "other_root/x3.txt"],
            "nested/root",
            ["../../other_root/**/*.txt"],
            ["nested/root"],
            # this is what would need to be returned to support this scenario
            ["nested/root/x1.py", "other_root/x3.txt"],
            ["nested/root"],
            "nested/root",
        )
