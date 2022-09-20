import sys
import time
from typing import Any


def example_runner(arg: str) -> str:
    print(f"example_runner called with {arg}")
    time.sleep(0.1)
    return "hello " + arg


def unique_per_deployment() -> str:
    return "embedded in main repo"


def example_function(arg1: str, arg2: int, *, kw_arg1: float, kw_arg2: str) -> str:
    return arg1 + str(arg2) + str(kw_arg1) + kw_arg2


def example_function_raises(
    arg1: Any,
) -> Any:
    raise (Exception(arg1))


def tetration(x: float) -> float:
    return x**x


def crash(_: Any) -> None:
    sys.exit(123)
