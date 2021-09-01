"""
TODO this doesn't really belong here because "job" is a nextbeat concept, not a nextrun
 concept, but it's convenient to have it here
"""
import dataclasses
import importlib
import sys
import types

from typing import List, Callable, Any, Dict, Union, Sequence


@dataclasses.dataclass(frozen=True)
class JobRunSpecFunction:
    """A function pointer with arguments for calling the function"""

    fn: Callable[..., Any]
    args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


@dataclasses.dataclass(frozen=True)
class JobRunSpecDeployedFunction:
    """
    Equivalent to JobRunSpecFunction, but potentially in a different codebase that is
    not loaded in the current codebase.
    """

    interpreter_path: str
    # we take it on faith that interpreter_path and interpreter_version line up
    interpreter_version: (int, int, int)  # major, minor, micro
    code_paths: List[str]
    module_name: str
    function_name: str
    function_args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    function_kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


# A JobRunSpec indicates how to run a job
JobRunSpec = Union[JobRunSpecFunction, JobRunSpecDeployedFunction]


def current_version_tuple():
    """
    Gets the version tuple for JobRunSpecDeployedFunction the current interpreter for
    JobRunSpecDeployedFunction.interpreter_version
    """
    return sys.version_info.major, sys.version_info.minor, sys.version_info.micro


def convert_function_to_deployed_function(
    function: JobRunSpecFunction,
) -> JobRunSpecDeployedFunction:
    """
    TODO this should do an entire upload of the current environment, which we don't do
     right now. For now we just assume that we're on the same machine (or just have the
     same code layout) as the machine we're calling from.

    TODO there should also be a version of this function that uses cloudpickle and
     assumes that the python/cloudpickle version on both ends is identical. That
     function would support a wider variety of JobRunSpecFunctions. This version is much
     more restricted in that the other end might be running a different version of
     python, not have cloudpickle installed at all(?).
    """

    # get the code_paths

    # TODO investigate all the different ways these prefixes can be different and why
    #  and whether we need to include them in the JobRunSpecDeployedFunction. E.g. test
    #  behavior under virtualenv/poetry/conda, Linux/Windows, different python versions,
    #  etc.
    prefixes = [sys.prefix, sys.exec_prefix, sys.base_prefix, sys.base_exec_prefix]
    code_paths = [
        path
        for path in sys.path
        if all(not path.startswith(prefix) for prefix in prefixes)
    ]

    # get the module_name and function_name

    if not isinstance(function.fn, types.FunctionType):
        # TODO things like a functools.partial or a C function like list.append are
        #  callable but not python functions. We should be able to support these in the
        #  cloudpickle version of this function
        raise ValueError(
            f"Function must be a python function, not a " f"{type(function.fn)}"
        )

    # TODO none of the below concerns would not be relevant in a cloudpickle version
    if "." in function.fn.__qualname__:
        raise ValueError(
            f"Function must be a global function in a module: "
            f"{function.fn.__qualname__}"
        )
    try:
        module = importlib.import_module(function.fn.__module__)
    except ModuleNotFoundError:
        raise ValueError(
            "The specified function could not be reconstructed via "
            f"{function.fn.__module__}.{function.fn.__qualname__}. It is "
            "probably not a totally normal module-level global python "
            "function"
        )
    if getattr(module, function.fn.__qualname__, None) is not function.fn:
        raise ValueError(
            "The specified function could not be reconstructed via "
            f"{function.fn.__module__}.{function.fn.__qualname__}. It is probably not a"
            f" totally normal module-level global python function"
        )

    # now deal with arguments

    return JobRunSpecDeployedFunction(
        sys.executable,
        current_version_tuple(),
        code_paths,
        function.fn.__module__,
        function.fn.__qualname__,
        function.args,
        function.kwargs,
    )
