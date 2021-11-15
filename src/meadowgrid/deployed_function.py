from __future__ import annotations

import abc
import dataclasses
import importlib
import sys
import types
from typing import Any, Dict, Sequence, Callable, Optional, Union

from meadowgrid.docker_controller import get_latest_digest_from_registry
from meadowgrid.meadowgrid_pb2 import (
    GitRepoCommit,
    ContainerAtDigest,
    ServerAvailableContainer,
)
from meadowgrid.meadowgrid_pb2 import ServerAvailableFolder, ServerAvailableInterpreter


@dataclasses.dataclass(frozen=True)
class MeadowGridCommand:
    command_line: Sequence[str]
    context_variables: Optional[Dict[str, Any]] = None


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


Runnable = Union[MeadowGridCommand, MeadowGridFunction]

CodeDeploymentTypes = (ServerAvailableFolder, GitRepoCommit)
CodeDeployment = Union[CodeDeploymentTypes]

InterpreterDeploymentTypes = (
    ServerAvailableInterpreter,
    ContainerAtDigest,
    ServerAvailableContainer,
)
InterpreterDeployment = Union[InterpreterDeploymentTypes]


class VersionedCodeDeployment(abc.ABC):
    """
    Similar to a CodeDeployment, but instead of a single version of the code (e.g. a
    specific commit in a git repo), specifies a versioned codebase (e.g. a git repo)
    where we can e.g. get the latest, select an old version.

    TODO this interface is very incomplete
    """

    @abc.abstractmethod
    async def get_latest(self) -> CodeDeployment:
        pass


class VersionedInterpreterDeployment(abc.ABC):
    """
    Similar to InterpreterDeployment, but instead of a single version fo the interpreter
    (e.g. a specific digest in a container repository), specifies a versioned set of
    interpreters (e.g. an entire container repository) where we can e.g. get the latest
    or select an old version.

    TODO this interface is very incomplete
    """

    @abc.abstractmethod
    async def get_latest(self) -> InterpreterDeployment:
        pass


@dataclasses.dataclass(frozen=True)
class MeadowGridDeployedRunnable:
    """
    A "runnable" that a MeadowGrid worker can run. A runnable can either be a python
    function or a command line. Specifies deployments that tell a MeadowGrid worker
    where to find the codebase and interpreter. The command is then run with the
    codebase as the working directory and the python interpreter's Scripts folder in the
    path. This allows you to run commands like `jupyter nbconvert`, `jupyter kernel`, or
    `papermill` if those commands/scripts are installed in the specified python
    environment.
    """

    code_deployment: CodeDeployment
    interpreter_deployment: InterpreterDeployment
    runnable: Runnable
    environment_variables: Optional[Dict[str, str]] = None


@dataclasses.dataclass(frozen=True)
class MeadowGridVersionedDeployedRunnable:
    """
    Like MeadowGridDeployedRunnable, but code_deployment and interpreter_deployment can
    be versioned. (They can also be simple unversioned CodeDeployment or
    InterpreterDeployment.)

    TODO this interface is very incomplete
    """

    code_deployment: Union[CodeDeployment, VersionedCodeDeployment]
    interpreter_deployment: Union[InterpreterDeployment, VersionedInterpreterDeployment]
    runnable: Runnable
    environment_variables: Optional[Dict[str, str]] = None

    async def get_latest(self) -> MeadowGridDeployedRunnable:
        if isinstance(self.code_deployment, CodeDeploymentTypes):
            code_deployment = self.code_deployment
        elif isinstance(self.code_deployment, VersionedCodeDeployment):
            code_deployment = await self.code_deployment.get_latest()
        else:
            raise ValueError(
                f"Unexpected code_deployment type {type(self.code_deployment)}"
            )

        if isinstance(self.interpreter_deployment, InterpreterDeploymentTypes):
            interpreter_deployment = self.interpreter_deployment
        elif isinstance(self.code_deployment, VersionedInterpreterDeployment):
            interpreter_deployment = await self.interpreter_deployment.get_latest()
        else:
            raise ValueError(
                "Unexpected interpreter_deployment type "
                f"{type(self.interpreter_deployment)}"
            )

        return MeadowGridDeployedRunnable(
            code_deployment,
            interpreter_deployment,
            self.runnable,
            self.environment_variables,
        )


@dataclasses.dataclass(frozen=True)
class GitRepo(VersionedCodeDeployment):
    """Represents a git repo"""

    # specifies the url, will be provided to git clone, see
    # https://git-scm.com/docs/git-clone
    repo_url: str

    default_branch: str = "main"

    # specifies a path within the repo that this deployment should consider as the
    # "root" directory. None is the same as "" or "."
    path_in_repo: Optional[str] = None

    async def get_latest(self) -> CodeDeployment:
        # TODO this seems kind of silly right now, but we should move the logic for
        #  converting from a branch name to a specific commit hash to this function from
        #  _get_git_repo_commit_interpreter_and_code so we can show the user what commit
        #  we actually ran with.
        # TODO also we will add the ability to specify overrides (e.g. a specific commit
        #  or an alternate branch)
        return GitRepoCommit(
            repo_url=self.repo_url,
            # TODO this is very sketchy. Need to invest time in all of the possible
            #  revision specifications
            commit="origin/" + self.default_branch,
            path_in_repo=self.path_in_repo,
        )


@dataclasses.dataclass(frozen=True)
class ContainerRepo(VersionedInterpreterDeployment):
    """Represents a container repository"""

    # repository name, i.e. used in `docker pull [repository]:[tag]`
    repository: str

    # tag name
    tag: str = "latest"

    async def get_latest(self) -> InterpreterDeployment:
        # TODO add ability to provide credentials
        return ContainerAtDigest(
            repository=self.repository,
            digest=await get_latest_digest_from_registry(
                self.repository, self.tag, None
            ),
        )


# maybe convert this to just use cloudpickle?
def convert_local_to_deployed_function(
    function_pointer: Callable[..., Any],
    function_args: Sequence[Any],
    function_kwargs: Dict[str, Any],
) -> MeadowGridDeployedRunnable:
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

    return MeadowGridDeployedRunnable(
        ServerAvailableFolder(code_paths=code_paths),
        ServerAvailableInterpreter(interpreter_path=sys.executable),
        MeadowGridFunction.from_name(
            function_pointer.__module__,
            function_pointer.__qualname__,
            function_args,
            function_kwargs,
        ),
    )
