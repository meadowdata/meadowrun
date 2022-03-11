import asyncio
import pickle
import time
import uuid
from typing import (
    Awaitable,
    Callable,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)

import cloudpickle

_T = TypeVar("_T")
_U = TypeVar("_U")


def _get_friendly_name(function: Callable[[_T], _U]) -> str:
    friendly_name = getattr(function, "__name__", "")
    if not friendly_name:
        friendly_name = "lambda"

    return friendly_name


def _get_id_name_function(function: Callable[[_T], _U]) -> Tuple[str, str, bytes]:
    """Returns job_id, friendly_name, pickled_function"""
    job_id = str(uuid.uuid4())

    friendly_name = _get_friendly_name(function)

    pickled_function = cloudpickle.dumps(function)
    # TODO larger functions should get copied to S3/filesystem instead of sent directly
    print(f"Size of pickled function is {len(pickled_function)}")

    return job_id, friendly_name, pickled_function
