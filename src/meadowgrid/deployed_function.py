from __future__ import annotations

import dataclasses
import importlib
import sys
import types

from typing import Any, Dict, Union, Sequence, Callable, Optional

from meadowgrid.meadowgrid_pb2 import GitRepoCommit, ServerAvailableFolder


@dataclasses.dataclass(frozen=True)
class MeadowGridFunctionName:
    # module_name can have dots like outer_package.inner_package.module as usual
    module_name: str
    # the name of a module-level function in the module specified by module_name
    function_name: str


@dataclasses.dataclass(frozen=True)
class MeadowGridFunction:
    # bytes should be interpreted as a pickled function
    function_spec: Union[MeadowGridFunctionName, bytes]
    function_args: Sequence[Any] = dataclasses.field(default_factory=lambda: [])
    function_kwargs: Dict[str, Any] = dataclasses.field(default_factory=lambda: {})

    @classmethod
    def from_pickled(
        cls,
        pickled_function: bytes,
        function_args: Optional[Sequence[Any]] = None,
        function_kwargs: Optional[Dict[str, Any]] = None,
    ) -> MeadowGridFunction:
        if function_args is None:
            function_args = []
        if function_kwargs is None:
            function_kwargs = {}
        return cls(pickled_function, function_args, function_kwargs)

    @classmethod
    def from_name(
        cls,
        module_name: str,
        function_name: str,
        function_args: Optional[Sequence[Any]] = None,
        function_kwargs: Optional[Dict[str, Any]] = None,
    ) -> MeadowGridFunction:
        if function_args is None:
            function_args = []
        if function_kwargs is None:
            function_kwargs = {}
        return cls(
            MeadowGridFunctionName(module_name, function_name),
            function_args,
            function_kwargs,
        )


DeploymentTypes = (ServerAvailableFolder, GitRepoCommit)
Deployment = Union[DeploymentTypes]


@dataclasses.dataclass(frozen=True)
class MeadowGridDeployedCommand:
    """
    A command that a MeadowGrid worker can run. Specifies a deployment that tells a
    MeadowGrid server where to find the codebase (and by extension the python
    interpreter). The command is then run with the codebase as the working directory and
    the python interpreter's Scripts folder in the path. This allows you to run commands
    like `jupyter nbconvert`, `jupyter kernel`, or `papermill` if those commands/scripts
    are installed in the specified python environment.
    """

    deployment: Deployment
    command_line: Sequence[str]
    context_variables: Optional[Dict[str, Any]] = None
    environment_variables: Optional[Dict[str, str]] = None


@dataclasses.dataclass(frozen=True)
class MeadowGridDeployedFunction:
    """
    A function that a MeadowGrid server is able to run. Specifies a deployment that
    tells a MeadowGrid server where to find the codebase (and by extension the python
    interpreter), and then specifies a function in that codebase to run (including
    args.)
    """

    deployment: Deployment
    meadowgrid_function: MeadowGridFunction
    environment_variables: Optional[Dict[str, str]] = None


# maybe convert this to just use cloudpickle?
def convert_local_to_deployed_function(
    function_pointer: Callable[..., Any],
    function_args: Sequence[Any],
    function_kwargs: Dict[str, Any],
) -> MeadowGridDeployedFunction:
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

    return MeadowGridDeployedFunction(
        ServerAvailableFolder(code_paths=code_paths, interpreter_path=sys.executable),
        MeadowGridFunction.from_name(
            function_pointer.__module__,
            function_pointer.__qualname__,
            function_args,
            function_kwargs,
        ),
    )
