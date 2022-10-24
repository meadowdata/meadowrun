from __future__ import annotations

import copy
import os
import os.path
import pickle
import shlex
import shutil
import sys
import urllib.parse
import uuid
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Tuple,
    TypeVar,
    Union,
)

import cloudpickle

from meadowrun.config import JOB_ID_VALID_CHARACTERS, MEADOWRUN_INTERPRETER
from meadowrun.deployment_spec import (
    ContainerAtDigestInterpreter,
    ContainerInterpreter,
    ContainerInterpreterBase,
    Deployment,
)
from meadowrun.docker_controller import get_registry_domain
from meadowrun.meadowrun_pb2 import (
    AwsSecretProto,
    AzureSecretProto,
    CodeZipFile,
    ContainerAtDigest,
    ContainerAtTag,
    ContainerImage,
    Credentials,
    CredentialsSourceMessage,
    EnvironmentSpec,
    EnvironmentSpecInCode,
    GitRepoBranch,
    GitRepoCommit,
    Job,
    KubernetesSecretProto,
    PyCommandJob,
    PyFunctionJob,
    QualifiedFunctionName,
    ServerAvailableContainer,
    ServerAvailableFile,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
    StringPair,
)
from meadowrun.run_job_core import (
    Host,
    JobCompletion,
    Resources,
    TaskResult,
    WaitOption,
)
from meadowrun.storage_grid_job import ensure_uploaded_incremental
from meadowrun.storage_keys import STORAGE_CODE_CACHE_PREFIX

if TYPE_CHECKING:
    from meadowrun.deployment_internal_types import (
        CodeDeployment,
        InterpreterDeployment,
        VersionedCodeDeployment,
        VersionedInterpreterDeployment,
    )
    from meadowrun.credentials import CredentialsSourceForService

_T = TypeVar("_T")
_U = TypeVar("_U")


def _credentials_source_message(
    credentials_source: CredentialsSourceForService,
) -> CredentialsSourceMessage:
    result = CredentialsSourceMessage(
        service=Credentials.Service.Value(credentials_source.service),
        service_url=credentials_source.service_url,
    )
    if isinstance(credentials_source.source, AwsSecretProto):
        result.aws_secret.CopyFrom(credentials_source.source)
    elif isinstance(credentials_source.source, AzureSecretProto):
        result.azure_secret.CopyFrom(credentials_source.source)
    elif isinstance(credentials_source.source, ServerAvailableFile):
        result.server_available_file.CopyFrom(credentials_source.source)
    elif isinstance(credentials_source.source, KubernetesSecretProto):
        result.kubernetes_secret.CopyFrom(credentials_source.source)
    else:
        raise ValueError(
            f"Unknown type of credentials source {type(credentials_source.source)}"
        )
    return result


async def _get_job_fields(
    host: Host,
    resources: Resources,
    deployment: Union[Deployment, Awaitable[Deployment], None],
    sidecar_containers: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ],
) -> Dict[str, Any]:
    """
    Compiles deployment and sidecar_containers into protobuf messages that can be added
    to the Job object. Should be used like Job(**(await _get_job_fields(...)))
    """

    # this doesn't totally belong here but it's close enough
    await host.set_defaults()

    # after this deployment will always be Deployment
    if deployment is None:
        deployment = await Deployment.mirror_local()
    elif not isinstance(deployment, Deployment):
        # TODO run this in parallel with e.g. launching instances
        deployment = await deployment

    # populate credentials_sources part 1
    credentials_sources = [
        _credentials_source_message(c) for c in deployment.credentials_sources
    ]

    # set environment_variables
    if deployment.environment_variables:
        environment_variables = [
            StringPair(key=key, value=value)
            for key, value in deployment.environment_variables.items()
        ]
    else:
        environment_variables = []

    # set interpreter
    interpreter = deployment.interpreter or ServerAvailableInterpreter(
        interpreter_path=MEADOWRUN_INTERPRETER
    )
    if resources.needs_cuda() and isinstance(
        interpreter, (EnvironmentSpec, EnvironmentSpecInCode)
    ):
        # we don't want to modify the interpreter in place
        interpreter = copy.deepcopy(interpreter)
        interpreter.additional_software["cuda"] = ""

    # set code
    code = deployment.code or ServerAvailableFolder()
    if isinstance(code, CodeZipFile):
        await _upload_code_zip_file(code, host)

    # prepare sidecar_containers
    (
        sidecar_containers_prepared,
        additional_credentials_sources,
    ) = _prepare_sidecar_containers(sidecar_containers)
    credentials_sources.extend(additional_credentials_sources)

    return {
        "result_highest_pickle_protocol": pickle.HIGHEST_PROTOCOL,
        "environment_variables": environment_variables,
        "sidecar_containers": sidecar_containers_prepared,
        "credentials_sources": credentials_sources,
        "uses_gpu": resources.uses_gpu(),
        _job_field_for_code_deployment(code): code,
        _job_field_for_interpreter_deployment(interpreter): interpreter,
    }


def _file_path_from_url(file_url: str) -> str:
    decoded_url = urllib.parse.urlparse(file_url)
    if decoded_url.scheme != "file":
        raise ValueError(f"Expected file URI: {file_url}")
    if sys.platform == "win32" and decoded_url.path.startswith("/"):
        # on Windows, file:///C:\foo turns into file_url.path = /C:\foo so we need
        # to remove the forward slash at the beginning
        file_path = decoded_url.path[1:]
    else:
        file_path = decoded_url.path
    return file_path


async def _upload_code_zip_file(code_deploy: CodeZipFile, host: Host) -> None:
    """
    Modifies code_deploy in place!

    CodeZipFile is not usable as is. mirror_local produces a CodeZipFile where the "url"
    is on the local file system. mirror_local doesn't have access to a
    host/storage_bucket, so it can't do the upload, so we "finish off" that part here
    """

    # TODO this is pretty janky. Because we modified the code_deploy in place last time,
    # we assume we can reuse the modified URL. This won't work if e.g. we want to use
    # this with a different host, or even if the uploaded zip file gets cleaned up. Both
    # of these scenarios should be unlikely, but we should support them.
    file_url = code_deploy.url
    if file_url.startswith("mdrstorage://"):
        return

    async with await host.get_storage_bucket() as storage_bucket:
        file_path = _file_path_from_url(file_url)

        key = await ensure_uploaded_incremental(
            storage_bucket, file_path, STORAGE_CODE_CACHE_PREFIX
        )
        # this "url" is a bit silly, we just want to be able to distinguish from a
        # file url. The only information we really care about is the key
        code_deploy.url = urllib.parse.urlunparse(("mdrstorage", "_", key, "", "", ""))
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)


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


def _make_valid_friendly_name(job_id: str) -> str:
    return "".join(c for c in job_id if c in JOB_ID_VALID_CHARACTERS)


def _get_friendly_name(function: Callable[[_T], _U]) -> str:
    friendly_name = getattr(function, "__name__", "")
    if not friendly_name:
        friendly_name = "lambda"

    return _make_valid_friendly_name(friendly_name)


def _prepare_ports(
    ports: Union[Iterable[str], str, Iterable[int], int, None]
) -> Optional[Sequence[str]]:
    if ports is None:
        return None
    elif isinstance(ports, int):
        return [str(ports)]
    elif isinstance(ports, str):
        return [ports]
    else:
        return [str(p) for p in ports]


def _prepare_sidecar_containers(
    container_interpreters: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ] = None,
) -> Tuple[Sequence[ContainerImage], Sequence[CredentialsSourceMessage]]:
    if container_interpreters is None:
        return [], []

    if isinstance(container_interpreters, ContainerInterpreterBase):
        container_interpreters = [container_interpreters]

    sidecar_containers = []
    credentials_sources = []
    for interpreter in container_interpreters:
        if isinstance(interpreter, ContainerAtDigestInterpreter):
            sidecar_containers.append(
                ContainerImage(
                    container_image_at_digest=ContainerAtDigest(
                        repository=interpreter.repository_name,
                        digest=interpreter.digest,
                    )
                )
            )
        elif isinstance(interpreter, ContainerInterpreter):
            sidecar_containers.append(
                ContainerImage(
                    container_image_at_tag=ContainerAtTag(
                        repository=interpreter.repository_name,
                        tag=interpreter.tag,
                    )
                )
            )
        else:
            raise ValueError(
                f"Unexpected type of sidecar_container {type(interpreter)}"
            )

        username_password_secret = interpreter._get_username_password_secret()
        if username_password_secret is not None:
            credentials_sources.append(
                _credentials_source_message(
                    username_password_secret._to_credentials_source(
                        "DOCKER",
                        get_registry_domain(interpreter._get_repository_name())[0],
                        Credentials.Type.USERNAME_PASSWORD,
                    )
                )
            )

    return sidecar_containers, credentials_sources


async def run_function(
    function: Union[Callable[..., _T], str],
    host: Host,
    resources: Optional[Resources] = None,
    deployment: Union[Deployment, Awaitable[Deployment], None] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
    sidecar_containers: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ] = None,
    ports: Union[Iterable[str], str, Iterable[int], int, None] = None,
    wait_for_result: bool = True,
) -> _T:
    """
    Runs function on a remote machine, specified by "host".

    Args:
        function: A reference to a function (e.g. `package.module.function_name`), a
            lambda, or a string like `"package.module.function_name"` (which is useful
            if the function cannot be referenced in the current environment but can be
            referenced in the deployed environment)
        host: Specifies where to run the function. See [Host][meadowrun.Host] and
            derived classes.
        resources: Specifies the resources (e.g. CPU, RAM) needed by the
            function. For some hosts, this is optional, for other hosts it is required.
            See [Resources][meadowrun.Resources].
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the environment
            (code and libraries) that are needed to run this command. This can be an
            actual Deployment object, or it can be an Awaitable that will produce a
            Deployment object. The default, None, is equivalent to
            [mirror_local][meadowrun.Deployment.mirror_local]
        args: Passed to the function like `function(*args)`
        kwargs: Passed to the function like `function(**kwargs)`
        sidecar_containers: Additional containers that will be available from the main
            job as sidecar-container-0 sidecar-container-1, etc.
        ports: A specification of ports to make available on the machine that runs this
            job. E.g. 8000, "8080-8089" (inclusive). Ports will be opened just for the
            duration of this job. Be careful as other jobs could be running on the same
            machine at the same time!
        wait_for_result: If this is set to False, we will run in "fire and forget" mode,
            which kicks off the function and doesn't wait for it to return.

    Returns:
        If wait_for_result is True (which is the default), the return value will be the
            result of calling `function`. If wait_for_result is False, the return value
            will always be None.
    """
    # first, construct the PyFunctionJob

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()

    # TODO add support for compressions, pickletools.optimize, possibly cloudpickle?
    # TODO also add the ability to write this to a shared location so that we don't need
    #  to pass it through the server.
    if args or kwargs:
        pickled_function_arguments = pickle.dumps(
            (args, kwargs), protocol=pickle_protocol
        )
    else:
        # according to docs, None is translated to empty anyway
        pickled_function_arguments = b""

    if isinstance(function, str):
        friendly_name = function
        module_name, separator, function_name = function.rpartition(".")
        if not separator:
            raise ValueError(
                f"Function must be in the form module_name.function_name: {function}"
            )
        py_function = PyFunctionJob(
            pickled_function_arguments=pickled_function_arguments,
            qualified_function_name=QualifiedFunctionName(
                module_name=module_name,
                function_name=function_name,
            ),
        )
    else:
        friendly_name = _get_friendly_name(function)
        pickled_function = cloudpickle.dumps(function)
        # TODO larger functions should get copied to S3/filesystem instead of sent
        # directly
        print(f"Size of pickled function is {len(pickled_function)}")
        py_function = PyFunctionJob(
            pickled_function_arguments=pickled_function_arguments,
            pickled_function=pickled_function,
        )

    # now create the Job and run it
    if resources is None:
        resources = Resources()

    job = Job(
        job_id=str(uuid.uuid4()),
        job_friendly_name=friendly_name,
        py_function=py_function,
        ports=_prepare_ports(ports),
        **(await _get_job_fields(host, resources, deployment, sidecar_containers)),
    )

    job_completion = await host.run_job(
        resources.to_internal(),
        job,
        WaitOption.WAIT_AND_TAIL_STDOUT if wait_for_result else WaitOption.DO_NOT_WAIT,
    )
    return job_completion.result


async def run_command(
    args: Union[str, Sequence[str]],
    host: Host,
    resources: Optional[Resources] = None,
    deployment: Union[Deployment, Awaitable[Deployment], None] = None,
    context_variables: Optional[Dict[str, Any]] = None,
    sidecar_containers: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ] = None,
    ports: Union[Iterable[str], str, Iterable[int], int, None] = None,
    wait_for_result: bool = True,
) -> JobCompletion[None]:
    """
    Runs the specified command on a remote machine

    Args:
        args: Specifies the command to run, can be a string (e.g. `"jupyter nbconvert
            --to html analysis.ipynb"`) or a list of strings (e.g. `["jupyter",
            --"nbconvert", "--to", "html", "analysis.ipynb"]`)
        host: Specifies where to run the function. See [Host][meadowrun.Host] and
            derived classes.
        resources: Specifies the resources (e.g. CPU, RAM) needed by the
            command. For some hosts, this is optional, for other hosts it is required.
            See [Resources][meadowrun.Resources].
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the environment
            (code and libraries) that are needed to run this command. This can be an
            actual Deployment object, or it can be an Awaitable that will produce a
            Deployment object. The default, None, is equivalent to
            [mirror_local][meadowrun.Deployment.mirror_local]
        context_variables: Experimental feature
        sidecar_containers: Additional containers that will be available from the main
            job as sidecar-container-0 sidecar-container-1, etc.
        ports: A specification of ports to make available on the machine that runs this
            job. E.g. 8000, "8080-8089" (inclusive). Ports will be opened just for the
            duration of this job. Be careful as other jobs could be running on the same
            machine at the same time!
        wait_for_result: If this is set to False, we will run in "fire and forget" mode,
            which kicks off the command and doesn't wait for it to return.

    Returns:
        A JobCompletion object that contains metadata about the running of the job.
    """

    # construct the PyCommandJob

    if isinstance(args, str):
        args = shlex.split(args)
    # this is kind of a silly way to get a friendly name--treat the first three
    # elements of args as if they're paths and take the last part of each path
    friendly_name = "-".join(os.path.basename(arg) for arg in args[:3])

    if context_variables:
        pickled_context_variables = pickle.dumps(
            context_variables, protocol=_pickle_protocol_for_deployed_interpreter()
        )
    else:
        pickled_context_variables = b""

    py_command = PyCommandJob(
        command_line=args, pickled_context_variables=pickled_context_variables
    )

    # create the job and run it

    if resources is None:
        resources = Resources()
    job = Job(
        job_id=str(uuid.uuid4()),
        job_friendly_name=_make_valid_friendly_name(friendly_name),
        py_command=py_command,
        ports=_prepare_ports(ports),
        **(await _get_job_fields(host, resources, deployment, sidecar_containers)),
    )

    return await host.run_job(
        resources.to_internal(),
        job,
        WaitOption.WAIT_AND_TAIL_STDOUT if wait_for_result else WaitOption.DO_NOT_WAIT,
    )


async def run_map(
    function: Callable[[_T], _U],
    args: Sequence[_T],
    host: Host,
    resources_per_task: Optional[Resources] = None,
    deployment: Union[Deployment, Awaitable[Deployment], None] = None,
    num_concurrent_tasks: Optional[int] = None,
    sidecar_containers: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ] = None,
    ports: Union[Iterable[str], str, Iterable[int], int, None] = None,
    wait_for_result: bool = True,
    max_num_task_attempts: int = 1,
    retry_with_more_memory: bool = False,
) -> Optional[Sequence[_U]]:
    """
    Equivalent to `map(function, args)`, but runs distributed and in parallel.

    Args:
        function: A reference to a function (e.g. `package.module.function_name`) or a
            lambda
        args: A list of objects, each item in the list represents a "task",
            where each "task" is an invocation of `function` on the item in the list
        resources_per_task: The resources (e.g. CPU and RAM) required to run a
            single task. For some hosts, this is optional, for other hosts it is
            required. See [Resources][meadowrun.Resources].
        host: Specifies where to get compute resources from. See [Host][meadowrun.Host]
            and derived classes.
        num_concurrent_tasks: The number of workers to launch. This can be less than or
            equal to the number of args/tasks. Will default to half the total number of
            tasks plus one, rounded down if set to None.
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the environment
            (code and libraries) that are needed to run this command. This can be an
            actual Deployment object, or it can be an Awaitable that will produce a
            Deployment object. The default, None, is equivalent to
            [mirror_local][meadowrun.Deployment.mirror_local]
        sidecar_containers: Additional containers that will be available from the main
            job as sidecar-container-0 sidecar-container-1, etc.
        ports: A specification of ports to make available on the machines that runs
            tasks for this job. E.g. 8000, "8080-8089" (inclusive). Ports will be opened
            just for the duration of this job. Be careful as other jobs could be running
            on the same machine at the same time!
        wait_for_result: If this is set to False, we will run in "fire and forget" mode,
            which kicks off the command and doesn't wait for it to return.
        max_num_task_attempts: If this is set to more than 1, tasks that fail will be
            retried. If this parameter is e.g. 3, a task that fails will be attempted a
            total of 3 times.
        retry_with_more_memory: This is an experimental feature and the API will likely
            change. If this is set to True, when a task fails, if the task at some point
            used more than 95% of the requested memory, the task will be retried with
            more memory. Each attempt will be allocated (original requested memory) *
            (attempt number).

    Returns:
        If wait_for_result is True (which is the default), the return value will be the
            result of running `function` on each of `args`. If wait_for_result is False,
            the return value will always be None.
    """

    if not num_concurrent_tasks:
        num_concurrent_tasks = len(args) // 2 + 1
    else:
        num_concurrent_tasks = min(num_concurrent_tasks, len(args))

    # prepare some variables for constructing the worker jobs

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()

    if resources_per_task is None:
        resources_per_task = Resources()
    job_fields = await _get_job_fields(
        host, resources_per_task, deployment, sidecar_containers
    )
    job_fields["job_friendly_name"] = _get_friendly_name(function)
    job_fields["ports"] = _prepare_ports(ports)

    if not wait_for_result:
        wait_option = WaitOption.DO_NOT_WAIT
    elif num_concurrent_tasks == 1:
        wait_option = WaitOption.WAIT_AND_TAIL_STDOUT
    else:
        wait_option = WaitOption.WAIT_SILENTLY

    return await host.run_map(
        function,
        args,
        resources_per_task.to_internal(),
        job_fields,
        num_concurrent_tasks,
        pickle_protocol,
        wait_option,
        max_num_task_attempts,
        retry_with_more_memory,
    )


async def run_map_as_completed(
    function: Callable[[_T], _U],
    args: Sequence[_T],
    host: Host,
    resources_per_task: Optional[Resources] = None,
    deployment: Union[Deployment, Awaitable[Deployment], None] = None,
    num_concurrent_tasks: Optional[int] = None,
    sidecar_containers: Union[
        Iterable[ContainerInterpreterBase], ContainerInterpreterBase, None
    ] = None,
    ports: Union[Iterable[str], str, Iterable[int], int, None] = None,
    max_num_task_attempts: int = 1,
    retry_with_more_memory: bool = False,
) -> AsyncIterable[TaskResult[_U]]:
    """
    Equivalent to [run_map][meadowrun.run_map], but returns results from tasks as they
    are completed as an AsyncIterable. This means that to access the results, you need
    to iterate using `async for`, and call `result_or_raise`  on the returned
    [TaskResult][meadowrun.TaskResult] objects. Usage for approximating `run_map`
    behavior is:

    ```python
    sorted_tasks = sorted(
        [task async for task in run_map_as_completed(...)],
        key=lambda t: t.task_id
    )
    results = [task.result_or_raise() for task in sorted_tasks]
    ```

    This will not have exactly the same behavior as `run_map` because `run_map` waits
    for all of the tasks to execute and then returns a list of results, ordered
    corresponding to how the `args` parameter was ordered, whereas
    `run_map_as_completed` returns results as they complete. For simple use cases.

    Args:
        function: A reference to a function (e.g. `package.module.function_name`) or a
            lambda
        args: A list of objects, each item in the list represents a "task",
            where each "task" is an invocation of `function` on the item in the list
        resources_per_task: The resources (e.g. CPU and RAM) required to run a
            single task. For some hosts, this is optional, for other hosts it is
            required. See [Resources][meadowrun.Resources].
        host: Specifies where to get compute resources from. See [Host][meadowrun.Host]
            and derived classes.
        num_concurrent_tasks: The number of workers to launch. This can be less than or
            equal to the number of args/tasks. Will default to half the total number of
            tasks plus one, rounded down if set to None.
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the environment
            (code and libraries) that are needed to be an Awaitable that will produce a
            Deployment object. The default, None, is equivalent to
            [mirror_local][meadowrun.Deployment.mirror_local]
        sidecar_containers: Additional containers that will be available from the main
            job as sidecar-container-0 sidecar-container-1, etc.
        ports: A specification of ports to make available on the machines that runs
            tasks for this job. E.g. 8000, "8080-8089" (inclusive). Ports will be opened
            just for the duration of this job. Be careful as other jobs could be running
            on the same machine at the same time!
        max_num_task_attempts: If this is set to more than 1, tasks that fail will be
            retried. If this parameter is e.g. 3, a task that fails will be attempted a
            total of 3 times.
        retry_with_more_memory: This is an experimental feature and the API will likely
            change. If this is set to True, when a task fails, if the task at some point
            used more than 95% of the requested memory, the task will be retried with
            more memory. Each attempt will be allocated (original requested memory) *
            (attempt number).

    Returns:
        An async iterable returning [TaskResult][meadowrun.TaskResult] objects.
    """

    if not num_concurrent_tasks:
        num_concurrent_tasks = len(args) // 2 + 1
    else:
        num_concurrent_tasks = min(num_concurrent_tasks, len(args))

    # prepare some variables for constructing the worker jobs
    friendly_name = _get_friendly_name(function)

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()

    if resources_per_task is None:
        resources_per_task = Resources()
    job_fields = await _get_job_fields(
        host, resources_per_task, deployment, sidecar_containers
    )
    job_fields["job_friendly_name"] = friendly_name
    job_fields["ports"] = _prepare_ports(ports)

    return host.run_map_as_completed(
        function,
        args,
        resources_per_task.to_internal(),
        job_fields,
        num_concurrent_tasks,
        pickle_protocol,
        wait_for_result=WaitOption.WAIT_SILENTLY,
        max_num_task_attempts=max_num_task_attempts,
        retry_with_more_memory=retry_with_more_memory,
    )


def _job_field_for_code_deployment(
    code_deployment: Union[CodeDeployment, VersionedCodeDeployment],
) -> str:
    """
    You can't just do Job(code_deployment=code_deployment) because of the protobuf
    oneofs. Instead you need to do
    Job(**{_job_field_for_code_deployment(code_deployment): code_deployment})
    """
    if isinstance(code_deployment, ServerAvailableFolder):
        return "server_available_folder"
    elif isinstance(code_deployment, GitRepoCommit):
        return "git_repo_commit"
    elif isinstance(code_deployment, GitRepoBranch):
        return "git_repo_branch"
    elif isinstance(code_deployment, CodeZipFile):
        return "code_zip_file"
    else:
        raise ValueError(f"Unknown code deployment type {type(code_deployment)}")


def _job_field_for_interpreter_deployment(
    interpreter_deployment: Union[InterpreterDeployment, VersionedInterpreterDeployment]
) -> str:
    """See _job_field_for_code_deployment for usage pattern"""
    if isinstance(interpreter_deployment, ServerAvailableInterpreter):
        return "server_available_interpreter"
    elif isinstance(interpreter_deployment, ContainerAtDigest):
        return "container_at_digest"
    elif isinstance(interpreter_deployment, ServerAvailableContainer):
        return "server_available_container"
    elif isinstance(interpreter_deployment, ContainerAtTag):
        return "container_at_tag"
    elif isinstance(interpreter_deployment, EnvironmentSpecInCode):
        return "environment_spec_in_code"
    elif isinstance(interpreter_deployment, EnvironmentSpec):
        return "environment_spec"
    else:
        raise ValueError(
            f"Unknown interpreter deployment type {type(interpreter_deployment)}"
        )
