from __future__ import annotations
import os

import subprocess
from pathlib import Path
from pprint import pprint
from shutil import copyfile
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Sequence, Tuple

import pytest
from meadowrun.deployment_manager import (
    _get_storage_funcs,
    compile_environment_spec_locally,
    compile_environment_spec_to_container,
    get_code_paths,
)
from meadowrun.deployment.pack_envs import cleanup_editable_install
from meadowrun.meadowrun_pb2 import (
    EnvironmentSpecInCode,
    EnvironmentType,
    GitRepoBranch,
    GitRepoCommit,
    Job,
)

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from typing_extensions import Final


_MEADOWDATA_TEST_REPO_URL: Final = str(
    (Path(__file__).parent.parent.parent / "test_repo").resolve()
)


@pytest.mark.asyncio
async def test_get_code_paths_git_repo_branch(tmp_path: Path) -> None:
    job = Job(
        git_repo_branch=GitRepoBranch(
            repo_url=_MEADOWDATA_TEST_REPO_URL,
            branch="main",
            path_to_source=".",
        )
    )
    (tmp_path / "git_repos").mkdir()
    (tmp_path / "local_copies").mkdir()
    code_paths, interpreter_spec_path, current_working_directory = await get_code_paths(
        git_repos_folder=str(tmp_path / "git_repos"),
        local_copies_folder=str(tmp_path / "local_copies"),
        job=job,
        credentials=None,
        storage_bucket_factory=None,
    )

    assert len(code_paths) == 1
    assert interpreter_spec_path is not None
    assert interpreter_spec_path == current_working_directory
    assert interpreter_spec_path in code_paths[0]


@pytest.mark.asyncio
async def test_get_code_paths_git_repo_commit(tmp_path: Path) -> None:
    commit = "50b9aad33cf35f719c8f88b048dec1567ac15bd5"
    (
        code_paths,
        interpreter_spec_path,
        current_working_directory,
    ) = await _get_git_code_paths_for_test(tmp_path, _MEADOWDATA_TEST_REPO_URL, commit)

    assert len(code_paths) == 1
    assert interpreter_spec_path is not None
    assert interpreter_spec_path == current_working_directory
    assert interpreter_spec_path in code_paths[0]
    assert commit in interpreter_spec_path


async def _get_git_code_paths_for_test(
    tmp_path: Path, repo_url: str, commit: str
) -> Tuple[Sequence[str], str, str]:
    job = Job(
        git_repo_commit=GitRepoCommit(
            repo_url=repo_url,
            commit=commit,
            path_to_source=".",
        )
    )
    (tmp_path / "git_repos").mkdir()
    (tmp_path / "local_copies").mkdir()
    code_paths, interpreter_spec_path, current_working_directory = await get_code_paths(
        git_repos_folder=str(tmp_path / "git_repos"),
        local_copies_folder=str(tmp_path / "local_copies"),
        job=job,
        credentials=None,
        storage_bucket_factory=None,
    )
    assert interpreter_spec_path is not None
    assert current_working_directory is not None
    return code_paths, interpreter_spec_path, current_working_directory


@pytest.mark.asyncio
async def test_compile_environment_spec_in_container(tmp_path: Path) -> None:
    commit = "50b9aad33cf35f719c8f88b048dec1567ac15bd5"
    (_, interpreter_spec_path, _) = await _get_git_code_paths_for_test(
        tmp_path, _MEADOWDATA_TEST_REPO_URL, commit
    )
    env_spec_in_code = EnvironmentSpecInCode(
        environment_type=EnvironmentType.ENV_TYPE_PIP,
        path_to_spec="requirements.txt",
        python_version="3.9",
    )

    server_available_container = await compile_environment_spec_to_container(
        environment_spec=env_spec_in_code,
        interpreter_spec_path=interpreter_spec_path,
        cloud=None,
    )

    pprint(server_available_container)


@pytest.mark.asyncio
async def test_compile_environment_spec_in_container_editable(tmp_path: Path) -> None:
    commit = "c200f78898244a2907920ded037969e774bd3a9f"
    (_, interpreter_spec_path, _) = await _get_git_code_paths_for_test(
        tmp_path, _MEADOWDATA_TEST_REPO_URL, commit
    )
    env_spec_in_code = EnvironmentSpecInCode(
        environment_type=EnvironmentType.ENV_TYPE_PIP,
        path_to_spec="requirements_with_editable.txt",
        python_version="3.9",
        editable_install=True,
    )

    _ = await compile_environment_spec_to_container(
        environment_spec=env_spec_in_code,
        interpreter_spec_path=interpreter_spec_path,
        cloud=None,
    )


def _get_storage_funcs_for_tests(
    tmp: Path,
) -> Tuple[
    Callable[[str, str], Awaitable[bool]], Callable[[str, str], Awaitable[None]]
]:
    storage_path = tmp / "storage"
    storage_path.mkdir(exist_ok=True)
    # env_cache/ is part of the remote_file_name (for S3-like storage) so must exist.
    (storage_path / "env_cache").mkdir(exist_ok=True)

    async def try_get_file(remote_file_name: str, local_file_name: str) -> bool:
        remote = storage_path / remote_file_name

        if not remote.exists():
            return False

        copyfile(remote, local_file_name)
        return True

    async def write_file(local_file_name: str, remote_file_name: str) -> None:
        remote = storage_path / remote_file_name

        copyfile(local_file_name, remote)

    return try_get_file, write_file


_GET_STORAGE_FUNCS = f"meadowrun.deployment_manager.{_get_storage_funcs.__qualname__}"


@pytest.fixture(scope="function")
def patch_get_storage_funcs(tmp_path: Path, mocker: MockerFixture) -> None:
    _working_folder = mocker.patch(_GET_STORAGE_FUNCS)
    _working_folder.side_effect = lambda: _get_storage_funcs_for_tests(tmp_path)


@pytest.fixture
def patch_conda_poetry_exe(mocker: MockerFixture) -> None:
    mocker.patch(
        "meadowrun.deployment.conda._CONDA_EXECUTABLE",
        new=os.environ["CONDA_EXE"],
        autospec=False,
    )
    path = os.environ["PATH"].split(os.path.pathsep)
    new_poetry = os.path.join(
        next(filter(lambda p: os.path.exists(os.path.join(p, "poetry")), path)),
        "poetry",
    )
    mocker.patch(
        "meadowrun.deployment.poetry._POETRY_PATH",
        new=new_poetry,
        autospec=False,
    )


@pytest.mark.parametrize(
    ("env_type", "path_to_spec"),
    [
        (EnvironmentType.ENV_TYPE_PIP, "requirements.txt"),
        (EnvironmentType.ENV_TYPE_CONDA, "myenv.yml"),
        (EnvironmentType.ENV_TYPE_POETRY, "poetry_with_git"),
    ],
)
@pytest.mark.asyncio
async def test_compile_environment_spec_locally(
    tmp_path: Path,
    patch_get_storage_funcs: Any,
    patch_conda_poetry_exe: Any,
    env_type: Any,
    path_to_spec: str,
) -> None:

    commit = "50b9aad33cf35f719c8f88b048dec1567ac15bd5"
    (_, interpreter_spec_path, _) = await _get_git_code_paths_for_test(
        tmp_path, _MEADOWDATA_TEST_REPO_URL, commit
    )
    env_spec_in_code = EnvironmentSpecInCode(
        environment_type=env_type,
        path_to_spec=path_to_spec,
        editable_install=False,
    )

    built_interpreters_folder = tmp_path / "built_interpreters"
    built_interpreters_folder.mkdir()
    server_available_interpreter = await compile_environment_spec_locally(
        environment_spec=env_spec_in_code,
        interpreter_spec_path=interpreter_spec_path,
        built_interpreters_folder=str(built_interpreters_folder),
    )
    pprint(server_available_interpreter)


@pytest.mark.parametrize(
    ("env_type", "path_to_spec", "main_script", "editable_install"),
    [
        (
            EnvironmentType.ENV_TYPE_PIP,
            "requirements_with_editable.txt",
            "example_package",
            True,
        ),
        (
            EnvironmentType.ENV_TYPE_PIP,
            "requirements_with_editable.txt",
            "example_package",
            False,
        ),
        (
            EnvironmentType.ENV_TYPE_CONDA,
            "myenv-editable-install.yml",
            "example_package",
            True,
        ),
        (
            EnvironmentType.ENV_TYPE_CONDA,
            "myenv-editable-install.yml",
            "example_package",
            False,
        ),
        (EnvironmentType.ENV_TYPE_POETRY, "poetry_editable", "foo_package", True),
        (EnvironmentType.ENV_TYPE_POETRY, "poetry_editable", "foo_package", False),
    ],
)
async def test_compile_environment_spec_locally_with_editable_install(
    tmp_path: Path,
    patch_get_storage_funcs: Any,
    patch_conda_poetry_exe: Any,
    env_type: Any,
    path_to_spec: str,
    main_script: str,
    editable_install: bool,
) -> None:
    commit = "d2fdbb5821950d344f39182c525398940a9623e4"
    (_, interpreter_spec_path, _) = await _get_git_code_paths_for_test(
        tmp_path, _MEADOWDATA_TEST_REPO_URL, commit
    )
    env_spec_in_code = EnvironmentSpecInCode(
        environment_type=env_type,
        path_to_spec=path_to_spec,
        editable_install=editable_install,
    )

    for i in range(2):
        built_interpreters_folder = tmp_path / f"built_interpreters_{i}"
        built_interpreters_folder.mkdir()
        server_available_interpreter = await compile_environment_spec_locally(
            environment_spec=env_spec_in_code,
            interpreter_spec_path=interpreter_spec_path,
            built_interpreters_folder=str(built_interpreters_folder),
        )
        completed = subprocess.run(
            args=[
                server_available_interpreter.interpreter_path,
                "-c",
                "import pkg_resources; "
                "print(list(pkg_resources.iter_entry_points(group='console_scripts')))",
            ],
            capture_output=True,
        )
        assert completed.returncode == 0, f"step {i}"
        if editable_install:
            # check that the entry point is registered, i.e. editable install worked.
            assert (
                f"main = {main_script}.main:main" in completed.stdout.decode()
            ), f"step {i}"
        else:
            # can't have registered the entry point.
            assert (
                f"main = {main_script}.main:main" not in completed.stdout.decode()
            ), f"step {i}"


class PthContents:
    VIRTUAL_ENV = "import _virtualenv"
    DISUTILS_PREC = (
        "import os; var = 'SETUPTOOLS_USE_DISTUTILS'; "
        "enabled = os.environ.get(var, 'local') == 'local';"
        " enabled and __import__('_distutils_hack').add_shim();"
    )
    FLAKE8 = """# Ensure these are imported before the current directory appears on path
import code
import doctest
"""
    PROTOBUF = (
        "import sys, types, os;has_mfs = sys.version_info > (3, 5);p = os.path."
        "join(sys._getframe(1).f_locals['sitedir'], *('google',));importlib = "
        "has_mfs and __import__('importlib.util');has_mfs and __import__"
        "('importlib.machinery');m = has_mfs and sys.modules.setdefault('google'"
        ", importlib.util.module_from_spec(importlib.machinery.PathFinder.find_"
        "spec('google', [os.path.dirname(p)])));m = m or sys.modules.setdefault"
        "('google', types.ModuleType('google'));mp = (m or []) and m.__dict__."
        "setdefault('__path__',[]);(p not in mp) and mp.append(p)"
    )
    EDITABLE = "/some/other/path/to/editable/install"


def test_cleanup_editable_install(tmp_path: Path) -> None:
    site_packages = tmp_path / "site-packages"
    site_packages.mkdir()
    with open(site_packages / "_virtualenv.pth", "w", encoding="utf-8") as f:
        f.write(PthContents.VIRTUAL_ENV)
    with open(site_packages / "distutils-precendence.pth", "w", encoding="utf-8") as f:
        f.write(PthContents.DISUTILS_PREC)
    with open(site_packages / "flake8_helper.pth", "w", encoding="utf-8") as f:
        f.write(PthContents.FLAKE8)
    with open(
        site_packages / "protobuf-3.20.3-py3.9-nspkg.pth", "w", encoding="utf-8"
    ) as f:
        f.write(PthContents.PROTOBUF)

    with open(site_packages / "myproject.pth", "w", encoding="utf-8") as f:
        f.write(PthContents.EDITABLE)

    cleanup_editable_install(prefix=str(tmp_path), site_packages=str(site_packages))

    with open(site_packages / "_virtualenv.pth", "r", encoding="utf-8") as f:
        assert f.read() == PthContents.VIRTUAL_ENV
    with open(site_packages / "distutils-precendence.pth", encoding="utf-8") as f:
        assert f.read() == PthContents.DISUTILS_PREC
    with open(site_packages / "flake8_helper.pth", encoding="utf-8") as f:
        assert f.read() == PthContents.FLAKE8
    with open(site_packages / "protobuf-3.20.3-py3.9-nspkg.pth", encoding="utf-8") as f:
        assert f.read() == PthContents.PROTOBUF

    with open(site_packages / "myproject.pth", encoding="utf-8") as f:
        assert f.read() == ""
