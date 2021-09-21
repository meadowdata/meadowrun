import dataclasses
import importlib
import sys
import types

from typing import Any, Dict, Union, Sequence, Callable

from nextrun.nextrun_pb2 import GitRepoCommit, ServerAvailableFolder


@dataclasses.dataclass(frozen=True)
class NextRunFunction:
    module_name: str
    function_name: str
    function_args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    function_kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})


Deployment = Union[ServerAvailableFolder, GitRepoCommit]


@dataclasses.dataclass(frozen=True)
class NextRunDeployedFunction:
    """
    A function that a NextRun server is able to run. Specifies a deployment that tells a
    NextRun server where to find the codebase (and by extension the python interpreter),
    and then specifies a function in that codebase to run (including args.)
    """

    deployment: Deployment
    next_run_function: NextRunFunction


def convert_local_to_deployed_function(
    function_pointer: Callable[..., Any],
    function_args: Sequence[Any],
    function_kwargs: Dict[str, Any],
) -> NextRunDeployedFunction:
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

    return NextRunDeployedFunction(
        ServerAvailableFolder(code_paths=code_paths, interpreter_path=sys.executable),
        NextRunFunction(
            function_pointer.__module__,
            function_pointer.__qualname__,
            function_args,
            function_kwargs,
        ),
    )
