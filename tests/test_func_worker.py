import pickle
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable, Dict, Tuple

import pytest

PATH_TO_FUNC_WORKER = "src/meadowrun/func_worker/__meadowrun_func_worker.py"

if sys.platform != "linux":
    pytest.skip("Skipping linux-only tests", allow_module_level=True)


def _pickle_args(io_path: str, arguments: Tuple[Tuple, Dict]) -> None:
    with open(io_path + ".arguments", "wb") as f:
        pickle.dump(arguments, f)


def _assert_results(io_path: str, expected_state: str, expected_result: Any) -> None:
    with open(io_path + ".state", "r", encoding="utf-8") as f:
        state = f.readline()

    with open(io_path + ".result", "rb") as f:
        result = pickle.load(f)

    assert state == expected_state
    if expected_result:
        assert result == expected_result


def test_call_str_func_success(tmp_path: Path) -> None:
    python = Path(sys.executable).resolve()
    io_path = str(tmp_path / "testjob")
    _pickle_args(io_path, (("arg1", 12654), {"kw_arg1": 3.1415, "kw_arg2": "kw_arg2"}))
    proc = subprocess.run(
        [
            python,
            PATH_TO_FUNC_WORKER,
            "--io-path",
            io_path,
            "--result-highest-pickle-protocol",
            str(pickle.HIGHEST_PROTOCOL),
            "--module-name",
            "example_package.example",
            "--function-name",
            "example_function",
            "--has-pickled-arguments",
        ],
        env={"PYTHONPATH": "tests/example_user_code"},
    )
    assert proc.returncode == 0
    _assert_results(io_path, "SUCCEEDED", "arg1126543.1415kw_arg2")


def test_call_str_func_exception(tmp_path: Path) -> None:
    python = Path(sys.executable).resolve()
    io_path = str(tmp_path / "testjob")
    _pickle_args(io_path, (("arg1",), {}))
    proc = subprocess.run(
        [
            python,
            PATH_TO_FUNC_WORKER,
            "--io-path",
            io_path,
            "--result-highest-pickle-protocol",
            str(pickle.HIGHEST_PROTOCOL),
            "--module-name",
            "example_package.example",
            "--function-name",
            "example_function_raises",
            "--has-pickled-arguments",
        ],
        env={"PYTHONPATH": "tests/example_user_code"},
    )
    assert proc.returncode == 0
    _assert_results(io_path, "PYTHON_EXCEPTION", None)


def _pickle_func(io_path: str, func: Callable) -> None:
    with open(io_path + ".function", "wb") as f:
        pickle.dump(func, f)


def test_call_pickled_func_success(tmp_path: Path) -> None:
    python = Path(sys.executable).resolve()
    io_path = str(tmp_path / "testjob")
    from example_user_code.example_package.example import example_function

    _pickle_func(io_path, example_function)
    _pickle_args(io_path, (("arg1", 12654), {"kw_arg1": 3.1415, "kw_arg2": "kw_arg2"}))
    proc = subprocess.run(
        [
            python,
            PATH_TO_FUNC_WORKER,
            "--io-path",
            io_path,
            "--result-highest-pickle-protocol",
            str(pickle.HIGHEST_PROTOCOL),
            "--has-pickled-function",
            "--has-pickled-arguments",
        ],
        env={"PYTHONPATH": "tests"},
    )
    assert proc.returncode == 0
    _assert_results(io_path, "SUCCEEDED", "arg1126543.1415kw_arg2")


def test_call_pickled_func_exception(tmp_path: Path) -> None:
    python = Path(sys.executable).resolve()
    io_path = str(tmp_path / "testjob")
    from example_user_code.example_package.example import example_function_raises

    _pickle_func(io_path, example_function_raises)
    _pickle_args(io_path, (("arg1",), {}))
    proc = subprocess.run(
        [
            python,
            PATH_TO_FUNC_WORKER,
            "--io-path",
            io_path,
            "--result-highest-pickle-protocol",
            str(pickle.HIGHEST_PROTOCOL),
            "--has-pickled-function",
            "--has-pickled-arguments",
        ],
        env={"PYTHONPATH": "tests"},
    )
    assert proc.returncode == 0
    _assert_results(io_path, "PYTHON_EXCEPTION", None)
