"""Specifies deployments for jobs"""

from __future__ import annotations

from typing import Union, cast, overload
from typing_extensions import Final, get_args

from meadowrun.credentials import (
    CredentialsDict,
    RawCredentials,
    get_docker_credentials,
)
from meadowrun.docker_controller import get_latest_digest_from_registry
from meadowrun.meadowrun_pb2 import (
    CodeZipFile,
    ContainerAtDigest,
    ContainerAtTag,
    EnvironmentSpec,
    EnvironmentSpecInCode,
    GitRepoBranch,
    GitRepoCommit,
    ServerAvailableContainer,
)
from meadowrun.meadowrun_pb2 import ServerAvailableFolder, ServerAvailableInterpreter

CodeDeployment = Union[ServerAvailableFolder, GitRepoCommit, CodeZipFile]
CodeDeploymentTypes: Final = get_args(CodeDeployment)


InterpreterDeployment = Union[
    ServerAvailableInterpreter,
    ContainerAtDigest,
    ServerAvailableContainer,
    # TODO this is kind of a VersionedInterpreterDeployment, but it can't be resolved
    # without instantiating the CodeDeployment. The whole
    # InterpreterDeployment/VersionedInterpreterDeployment distinction probably needs
    # rethinking.
    EnvironmentSpecInCode,
    EnvironmentSpec,
]
InterpreterDeploymentTypes: Final = get_args(InterpreterDeployment)


# Similar to a CodeDeployment, but instead of a single version of the code (e.g. a
# specific commit in a git repo), specifies a versioned codebase (e.g. a git repo) where
# we can e.g. get the latest, select an old version.
VersionedCodeDeployment = GitRepoBranch
VersionedCodeDeploymentTypes: Final = (VersionedCodeDeployment,)


# Similar to InterpreterDeployment, but instead of a single version of the interpreter
# (e.g. a specific digest in a container repository), specifies a versioned set of
# interpreters (e.g. an entire container repository) where we can e.g. get the latest or
# select an old version.
VersionedInterpreterDeployment = ContainerAtTag
VersionedInterpreterDeploymentTypes: Final = (VersionedCodeDeployment,)


# TODO the below methods are not currently used but they should be used by run_map so
# that we resolve to a consistent deployment before we start a grid job. E.g. you don't
# want the same ContainerAtTag resolving to different ContainerAtDigest for different
# workers.


@overload
async def get_latest_code_version(
    code: CodeDeployment,
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> CodeDeployment:
    ...


@overload
async def get_latest_code_version(
    code: VersionedCodeDeployment,
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> GitRepoCommit:
    ...


async def get_latest_code_version(
    code: Union[CodeDeployment, VersionedCodeDeployment],
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> CodeDeployment:
    if isinstance(code, CodeDeploymentTypes):
        # mypy wouldn't need a cast if we added the types explicitly instead of
        # CodeDeploymentTypes...but then we'd need to repeat that.
        return cast(CodeDeployment, code)
    elif isinstance(code, GitRepoBranch):
        # TODO this is a stub for now. We should remove logic for translating a branch
        #  into a commit in _get_git_repo_commit_interpreter_and_code and implement it
        #  here using git ls-remote
        # TODO also we will add the ability to specify overrides (e.g. a specific commit
        #  or an alternate branch)
        return GitRepoCommit(
            repo_url=code.repo_url,
            commit="origin/" + code.branch,
            path_to_source=code.path_to_source,
        )
    else:
        raise ValueError(
            f"Unknown CodeDeployment/VersionedCodeDeployment type {type(code)}"
        )


@overload
async def get_latest_interpreter_version(
    interpreter: InterpreterDeployment,
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> InterpreterDeployment:
    ...


@overload
async def get_latest_interpreter_version(
    interpreter: VersionedInterpreterDeployment,
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> ContainerAtDigest:
    ...


async def get_latest_interpreter_version(
    interpreter: Union[InterpreterDeployment, VersionedInterpreterDeployment],
    credentials: Union[CredentialsDict, RawCredentials, None],
) -> InterpreterDeployment:
    if isinstance(interpreter, InterpreterDeploymentTypes):
        return cast(InterpreterDeployment, interpreter)
    elif isinstance(interpreter, ContainerAtTag):
        if credentials is not None and not isinstance(credentials, RawCredentials):
            credentials = await get_docker_credentials(
                interpreter.repository, credentials
            )
        return ContainerAtDigest(
            repository=interpreter.repository,
            digest=await get_latest_digest_from_registry(
                interpreter.repository,
                interpreter.tag,
                credentials,
            ),
        )
    else:
        raise ValueError(
            "Unknown InterpreterDeployment/VersionedInterpreterDeployment type "
            f"{type(interpreter)}"
        )
