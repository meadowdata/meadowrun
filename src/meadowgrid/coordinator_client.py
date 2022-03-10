from __future__ import annotations

import pickle
from typing import (
    Dict,
    Iterable,
    Optional,
    Union,
)

from meadowgrid.config import (
    JOB_ID_VALID_CHARACTERS,
)
from meadowgrid.credentials import CredentialsSource, CredentialsService
from meadowgrid.deployed_function import (
    CodeDeployment,
    InterpreterDeployment,
    MeadowGridFunction,
    MeadowGridFunctionName,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowgrid.meadowgrid_pb2 import (
    AddCredentialsRequest,
    AwsSecret,
    ContainerAtDigest,
    ContainerAtTag,
    Credentials,
    GitRepoBranch,
    GitRepoCommit,
    Job,
    ProcessState,
    PyFunctionJob,
    QualifiedFunctionName,
    ServerAvailableContainer,
    ServerAvailableFile,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
    StringPair,
)

# make this enum available for users
ProcessStateEnum = ProcessState.ProcessStateEnum


def _make_valid_job_id(job_id: str) -> str:
    return "".join(c for c in job_id if c in JOB_ID_VALID_CHARACTERS)


def _string_pairs_from_dict(d: Optional[Dict[str, str]]) -> Iterable[StringPair]:
    """
    Opposite of _string_pairs_to_dict in agent.py. Helper for dicts in protobuf.
    """
    if d is not None:
        for key, value in d.items():
            yield StringPair(key=key, value=value)


def _add_deployments_to_job(
    job: Job,
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
    interpreter_deployment: Union[
        InterpreterDeployment, VersionedInterpreterDeployment
    ],
) -> None:
    """
    Think of this as job.code_deployment = code_deployment; job.interpreter_deployment =
    interpreter_deployment, but it's complicated because these are protobuf oneofs
    """
    if isinstance(code_deployment, ServerAvailableFolder):
        job.server_available_folder.CopyFrom(code_deployment)
    elif isinstance(code_deployment, GitRepoCommit):
        job.git_repo_commit.CopyFrom(code_deployment)
    elif isinstance(code_deployment, GitRepoBranch):
        job.git_repo_branch.CopyFrom(code_deployment)
    else:
        raise ValueError(f"Unknown code deployment type {type(code_deployment)}")

    if isinstance(interpreter_deployment, ServerAvailableInterpreter):
        job.server_available_interpreter.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ContainerAtDigest):
        job.container_at_digest.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ServerAvailableContainer):
        job.server_available_container.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, ContainerAtTag):
        job.container_at_tag.CopyFrom(interpreter_deployment)
    else:
        raise ValueError(
            f"Unknown interpreter deployment type {type(interpreter_deployment)}"
        )


def _pickle_protocol_for_deployed_interpreter() -> int:
    """
    This is a placeholder, the intention is to get the deployed interpreter's version
    somehow from the Deployment object or something like it and use that to determine
    what the highest pickle protocol version we can use safely is.
    """

    # TODO just hard-coding the interpreter version for now, need to actually grab it
    #  from the deployment somehow
    interpreter_version = (3, 8, 0)

    # based on documentation in
    # https://docs.python.org/3/library/pickle.html#data-stream-format
    if interpreter_version >= (3, 8, 0):
        protocol = 5
    elif interpreter_version >= (3, 4, 0):
        protocol = 4
    elif interpreter_version >= (3, 0, 0):
        protocol = 3
    else:
        # TODO support for python 2 would require dealing with the string/bytes issue
        raise NotImplementedError("We currently only support python 3")

    return min(protocol, pickle.HIGHEST_PROTOCOL)


def _create_py_function(
    meadowgrid_function: MeadowGridFunction, pickle_protocol: int
) -> PyFunctionJob:
    """
    Returns a PyFunctionJob, called by _create_py_runnable_job which creates a Job that
    has a PyFunctionJob in it.

    pickle_protocol should be the highest pickle protocol that the deployed function
    will be able to understand.
    """

    # first pickle the function arguments from job_run_spec

    # TODO add support for compressions, pickletools.optimize, possibly cloudpickle?

    # TODO also add the ability to write this to a shared location so that we don't need
    #  to pass it through the server.

    if meadowgrid_function.function_args or meadowgrid_function.function_kwargs:
        pickled_function_arguments = pickle.dumps(
            (meadowgrid_function.function_args, meadowgrid_function.function_kwargs),
            protocol=pickle_protocol,
        )
    else:
        # according to docs, None is translated to empty anyway
        pickled_function_arguments = b""

    # then, construct the PyFunctionJob
    py_function = PyFunctionJob(pickled_function_arguments=pickled_function_arguments)

    function_spec = meadowgrid_function.function_spec
    if isinstance(function_spec, MeadowGridFunctionName):
        py_function.qualified_function_name.CopyFrom(
            QualifiedFunctionName(
                module_name=function_spec.module_name,
                function_name=function_spec.function_name,
            )
        )
    elif isinstance(function_spec, bytes):
        py_function.pickled_function = function_spec
    else:
        raise ValueError(f"Unknown type of function_spec {type(function_spec)}")

    return py_function


def _add_credentials_request(
    service: CredentialsService, service_url: str, source: CredentialsSource
) -> AddCredentialsRequest:
    result = AddCredentialsRequest(
        service=Credentials.Service.Value(service),
        service_url=service_url,
    )
    if isinstance(source, AwsSecret):
        result.aws_secret.CopyFrom(source)
    elif isinstance(source, ServerAvailableFile):
        result.server_available_file.CopyFrom(source)
    else:
        raise ValueError(f"Unknown type of credentials source {type(source)}")
    return result
