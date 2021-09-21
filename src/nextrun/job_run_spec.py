"""
TODO this doesn't really belong here because "job" is a nextbeat concept, not a nextrun
 concept, but it's convenient to have it here
"""
import dataclasses
import importlib
import sys
import types

from typing import List, Any, Dict, Sequence, Callable


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


def current_version_tuple():
    """
    Gets the version tuple for JobRunSpecDeployedFunction the current interpreter for
    JobRunSpecDeployedFunction.interpreter_version
    """
    return sys.version_info.major, sys.version_info.minor, sys.version_info.micro


def convert_function_to_deployed_function(
    function_pointer: Callable[..., Any],
    function_args: Sequence[Any],
    function_kwargs: Dict[str, Any],
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

    if not isinstance(function_pointer, types.FunctionType):
        # TODO things like a functools.partial or a C function like list.append are
        #  callable but not python functions. We should be able to support these in the
        #  cloudpickle version of this function
        raise ValueError(
            f"Function must be a python function, not a {type(function_pointer)}"
        )

    # TODO none of the below concerns would not be relevant in a cloudpickle version
    if "." in function_pointer.__qualname__:
        raise ValueError(
            f"Function must be a global function in a module: "
            f"{function_pointer.__qualname__}"
        )
    try:
        module = importlib.import_module(function_pointer.__module__)
    except ModuleNotFoundError:
        raise ValueError(
            "The specified function could not be reconstructed via "
            f"{function_pointer.__module__}.{function_pointer.__qualname__}. It is "
            "probably not a totally normal module-level global python function"
        )
    if getattr(module, function_pointer.__qualname__, None) is not function_pointer:
        raise ValueError(
            "The specified function could not be reconstructed via "
            f"{function_pointer.__module__}.{function_pointer.__qualname__}. It is "
            "probably not a totally normal module-level global python function"
        )

    # now deal with arguments

    return JobRunSpecDeployedFunction(
        sys.executable,
        current_version_tuple(),
        code_paths,
        function_pointer.__module__,
        function_pointer.__qualname__,
        function_args,
        function_kwargs,
    )
