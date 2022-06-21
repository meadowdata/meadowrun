from __future__ import annotations

import abc
import asyncio
import dataclasses
import os.path
import pickle
import platform
import shlex
import shutil
import sys
import tempfile
import urllib.parse
import uuid
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import cloudpickle

from meadowrun import local_code
from meadowrun.aws_integration import s3
from meadowrun.aws_integration.ec2_instance_allocation import (
    run_job_ec2_instance_registrar,
)
from meadowrun.aws_integration.grid_tasks_sqs import prepare_ec2_run_map
from meadowrun.azure_integration.azure_instance_allocation import (
    run_job_azure_vm_instance_registrar,
)
from meadowrun.azure_integration.azure_ssh_keys import get_meadowrun_vault_name
from meadowrun.azure_integration.grid_tasks_queue import prepare_azure_vm_run_map
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    get_subscription_id_sync,
)
from meadowrun.conda import env_export, try_get_current_conda_env
from meadowrun.config import JOB_ID_VALID_CHARACTERS, MEADOWRUN_INTERPRETER
from meadowrun.credentials import CredentialsSourceForService, CredentialsService
from meadowrun.deployment import (
    CodeDeployment,
    InterpreterDeployment,
    VersionedCodeDeployment,
    VersionedInterpreterDeployment,
)
from meadowrun.docker_controller import get_registry_domain
from meadowrun.meadowrun_pb2 import (
    AwsSecretProto,
    AzureSecretProto,
    CodeZipFile,
    ContainerAtDigest,
    ContainerAtTag,
    Credentials,
    CredentialsSourceMessage,
    EnvironmentSpec,
    EnvironmentSpecInCode,
    EnvironmentType,
    GitRepoBranch,
    GitRepoCommit,
    Job,
    PyCommandJob,
    PyFunctionJob,
    QualifiedFunctionName,
    ServerAvailableContainer,
    ServerAvailableFile,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
    StringPair,
)
from meadowrun.pip_integration import (
    pip_freeze_without_local_current_interpreter,
    pip_freeze_without_local_other_interpreter,
)
from meadowrun.run_job_core import CloudProviderType, Host, JobCompletion, SshHost

_T = TypeVar("_T")
_U = TypeVar("_U")


class Secret(abc.ABC):
    """
    An abstract class for specifying a secret, e.g. a username/password or an SSH key
    """

    @abc.abstractmethod
    def _to_credentials_source(
        self,
        service: CredentialsService,
        service_url: str,
        credentials_type: Credentials.Type.ValueType,
    ) -> CredentialsSourceForService:
        pass


@dataclasses.dataclass
class AwsSecret(Secret):
    """
    An AWS secret

    Attributes:
        secret_name: The name of the secret (also sometimes called the id)
    """

    secret_name: str

    def _to_credentials_source(
        self,
        service: CredentialsService,
        service_url: str,
        credentials_type: Credentials.Type.ValueType,
    ) -> CredentialsSourceForService:
        return CredentialsSourceForService(
            service=service,
            service_url=service_url,
            source=AwsSecretProto(
                credentials_type=credentials_type,
                secret_name=self.secret_name,
            ),
        )


@dataclasses.dataclass
class AzureSecret(Secret):
    """
    An Azure secret

    Attributes:
        secret_name: The name of the secret
        vault_name: The name of the Key Vault that the secret is in. Defaults to None,
            which implies the Meadowrun-managed Key Vault (mr<last 22 characters of your
            subscription id>)
    """

    secret_name: str
    vault_name: Optional[str] = None

    def _to_credentials_source(
        self,
        service: CredentialsService,
        service_url: str,
        credentials_type: Credentials.Type.ValueType,
    ) -> CredentialsSourceForService:
        if self.vault_name is None:
            vault_name = get_meadowrun_vault_name(get_subscription_id_sync())
        else:
            vault_name = self.vault_name

        return CredentialsSourceForService(
            service=service,
            service_url=service_url,
            source=AzureSecretProto(
                credentials_type=credentials_type,
                secret_name=self.secret_name,
                vault_name=vault_name,
            ),
        )


class ContainerInterpreterBase(abc.ABC):
    """An abstract base class for specifying a container as a python interpreter"""

    @abc.abstractmethod
    def get_interpreter_spec(
        self,
    ) -> Union[InterpreterDeployment, VersionedInterpreterDeployment]:
        pass

    @abc.abstractmethod
    def _get_repository_name(self) -> str:
        pass

    @abc.abstractmethod
    def _get_username_password_secret(self) -> Optional[Secret]:
        pass


@dataclasses.dataclass
class ContainerInterpreter(ContainerInterpreterBase):
    """
    Specifies a container image. The container image must be configured so that running
    `docker run -it repository_name:tag python` runs the right python interpreter.

    Attributes:
        repository_name: E.g. `python`, or `<my_azure_registry>.azurecr.io/foo`
        tag: E.g. `3.9-slim-bullseye` or `latest` (the default)
        username_password_secret: An AWS or Azure secret that has a username and
            password for connecting to the container registry (as specified or implied
            in image_name). Only needed if the image/container registry is private.
    """

    repository_name: str
    tag: str = "latest"
    username_password_secret: Optional[Secret] = None

    def get_interpreter_spec(
        self,
    ) -> Union[InterpreterDeployment, VersionedInterpreterDeployment]:
        return ContainerAtTag(repository=self.repository_name, tag=self.tag)

    def _get_repository_name(self) -> str:
        return self.repository_name

    def _get_username_password_secret(self) -> Optional[Secret]:
        return self.username_password_secret


@dataclasses.dataclass
class ContainerAtDigestInterpreter(ContainerInterpreterBase):
    """
    Like [ContainerInterpreter][meadowrun.ContainerInterpreter] but specifies a digest
    instead of a tag. Running `docker run -it repository_name@digest python` must run
    the right python interpreter.

    Attributes:
        repository_name: E.g. `python`, or `<my_azure_registry>.azurecr.io/foo`
        digest: E.g. `sha256:97725c608...`
        username_password_secret: An AWS or Azure secret that has a username and
            password for connecting to the container registry (as specified or implied
            in image_name). Only needed if the image/container registry is private.
    """

    repository_name: str
    digest: str
    username_password_secret: Optional[Secret] = None

    def get_interpreter_spec(
        self,
    ) -> Union[InterpreterDeployment, VersionedInterpreterDeployment]:
        return ContainerAtDigest(repository=self.repository_name, digest=self.digest)

    def _get_repository_name(self) -> str:
        return self.repository_name

    def _get_username_password_secret(self) -> Optional[Secret]:
        return self.username_password_secret


class InterpreterSpecFile(abc.ABC):
    """
    An abstract base class for specifying a Conda environment.yml file or a pip
    requirements.txt file
    """

    pass


@dataclasses.dataclass
class CondaEnvironmentYmlFile(InterpreterSpecFile):
    """
    Specifies an environment.yml file generated by conda env export.

    Attributes:
        path_to_yml_file: In the context of mirror_local, this is the path to a file on
            the local disk. In the context of git_repo, this is a path to a file in the
            git repo
    """

    path_to_yml_file: str


@dataclasses.dataclass
class PipRequirementsFile(InterpreterSpecFile):
    """
    Specifies a requirements.txt file generated by pip freeze.

    Attributes:
        path_to_requirements_file: In the context of mirror_local, this is the path to a
            file on the local disk. In the context of git_repo, this is a path to a file
            in the git repo
        python_version: A python version like "3.9" or "3.9.5". The version must be
            available on docker: https://hub.docker.com/_/python as
            python-<python_version>-slim-bullseye.
    """

    path_to_requirements_file: str
    python_version: str


@dataclasses.dataclass
class PoetryProjectPath(InterpreterSpecFile):
    """
    Specifies a poetry project

    Attributes:
        path_to_project: In the context of mirror_local, this is the path to a folder on
            the local disk that contains pyproject.toml and poetry.lock. In the context
            of git_repo, this is a path to a folder in the git repo (use `""` to
            indicate that pyproject.toml and poetry.lock are at the root of the repo).
        python_version: A python version like "3.9" or "3.9.5". The version must be
            available on docker: https://hub.docker.com/_/python as
            python-<python_version>-slim-bullseye. This python version must be
            compatible with the requirements in pyproject.toml2
    """

    path_to_project: str
    python_version: str


class LocalInterpreter(abc.ABC):
    """
    An abstract base class for specifying a python interpreter on the local machine
    """

    @abc.abstractmethod
    async def get_interpreter_spec(self) -> InterpreterDeployment:
        pass


@dataclasses.dataclass
class LocalCondaInterpreter(LocalInterpreter):
    """
    Specifies a locally installed conda environment

    Attributes:
        environment_name_or_path: Either the name of a conda environment (e.g.
            `my_env_name`) or the full path to the folder of a conda environment (e.g.
            `/home/user/miniconda3/envs/my_env_name`). Will be passed to conda env
            export
    """

    environment_name_or_path: str

    async def get_interpreter_spec(self) -> InterpreterDeployment:
        if platform.system() != "Linux":
            raise ValueError(
                "mirror_local from a Conda environment only works from Linux because "
                "conda environments are not cross-platform."
            )
        return EnvironmentSpec(
            environment_type=EnvironmentType.CONDA,
            spec=await env_export(self.environment_name_or_path),
        )


@dataclasses.dataclass
class LocalPipInterpreter(LocalInterpreter):
    """
    Specifies a locally available interpreter. It can be a "regular" install of a python
    interpreter, a virtualenv, or anything based on pip.

    Attributes:
        path_to_interpreter: The path to the python executable. E.g.
            `/home/user/my_virtual_env/bin/python`
        python_version: A python version like "3.9" or "3.9.5". The version must be
            available on docker: https://hub.docker.com/_/python as
            python-<python_version>-slim-bullseye.
    """

    path_to_interpreter: str
    python_version: str

    async def get_interpreter_spec(self) -> InterpreterDeployment:
        return EnvironmentSpec(
            environment_type=EnvironmentType.PIP,
            # TODO this won't work if the specified environment has editable installs
            spec=await pip_freeze_without_local_other_interpreter(
                self.path_to_interpreter
            ),
            python_version=self.python_version,
        )


async def _get_current_local_interpreter() -> InterpreterDeployment:
    # first, check if we are in a conda environment
    conda_env_spec = await try_get_current_conda_env()
    if conda_env_spec is not None:
        if platform.system() != "Linux":
            raise ValueError(
                "mirror_local from a Conda environment only works from Linux because "
                "conda environments are not cross-platform."
            )
        print("Mirroring current conda environment")
        return EnvironmentSpec(
            environment_type=EnvironmentType.CONDA, spec=conda_env_spec
        )

    # next, check if we're in a poetry environment. Unfortunately it doesn't seem like
    # there's a way to detect that we're in a poetry environment unless the current
    # working directory contains the pyproject.toml and poetry.lock files. If for smoe
    # reason this isn't the case, we'll fall through to the pip-based case, which will
    # mostly work for poetry environments.
    if os.path.isfile("pyproject.toml") and os.path.isfile("poetry.lock"):
        with open("pyproject.toml") as project_file:
            project_file_contents = project_file.read()
        with open("poetry.lock") as lock_file:
            lock_file_contents = lock_file.read()
        print("Mirroring current poetry environment")
        return EnvironmentSpec(
            environment_type=EnvironmentType.POETRY,
            spec=project_file_contents,
            spec_lock=lock_file_contents,
        )

    # if not, assume this is a pip-based environment
    print("Mirroring current pip environment")
    return EnvironmentSpec(
        environment_type=EnvironmentType.PIP,
        spec=await pip_freeze_without_local_current_interpreter(),
        python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
    )


@dataclasses.dataclass(frozen=True)
class Deployment:
    interpreter: Union[
        InterpreterDeployment, VersionedInterpreterDeployment, None
    ] = None
    code: Union[CodeDeployment, VersionedCodeDeployment, None] = None
    environment_variables: Optional[Dict[str, str]] = None
    credentials_sources: List[CredentialsSourceForService] = dataclasses.field(
        default_factory=list
    )

    @classmethod
    async def mirror_local(
        cls,
        include_sys_path: bool = True,
        additional_paths: Iterable[str] = tuple(),
        interpreter: Union[
            LocalInterpreter, InterpreterSpecFile, ContainerInterpreterBase, None
        ] = None,
        environment_variables: Optional[Dict[str, str]] = None,
    ) -> Deployment:
        """A deployment that mirrors the local environment and code.

        Args:
            include_sys_path: if True, find python code in the paths in sys.path
                (effectively "your local code"), copies them to the remote machines, and
                add them to sys.path there. Ignores any installed packages.
            additional_paths: local code paths to copy to the remote machine.
            interpreter: Specifies the environment/interpreter to use. Defaults to None
                which will detect the currently activated env. Alternatively, you can
                explicitly specify a locally installed python environment with
                [LocalCondaInterpreter][meadowrun.LocalCondaInterpreter],
                [LocalPipInterpreter][meadowrun.LocalPipInterpreter],
                [CondaEnvironmentYmlFile][meadowrun.CondaEnvironmentYmlFile],
                [PipRequirementsFile][meadowrun.PipRequirementsFile],
                [PoetryProjectPath][meadowrun.PoetryProjectPath]
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment.

        Returns:
            A `Deployment` object that can be passed to the `run_*` functions.
        """

        credentials_sources = []

        if interpreter is None:
            interpreter_spec: Union[
                InterpreterDeployment, VersionedInterpreterDeployment
            ] = await _get_current_local_interpreter()
        elif isinstance(interpreter, CondaEnvironmentYmlFile):
            with open(interpreter.path_to_yml_file) as f:
                interpreter_spec = EnvironmentSpec(
                    environment_type=EnvironmentType.CONDA,
                    spec=f.read(),
                )
        elif isinstance(interpreter, PipRequirementsFile):
            with open(interpreter.path_to_requirements_file) as f:
                interpreter_spec = EnvironmentSpec(
                    environment_type=EnvironmentType.PIP,
                    spec=f.read(),
                    python_version=interpreter.python_version,
                )
        elif isinstance(interpreter, PoetryProjectPath):
            with open(
                os.path.join(interpreter.path_to_project, "pyproject.toml"),
                "r",
                encoding="utf-8",
            ) as spec_file:
                spec = spec_file.read()
            with open(
                os.path.join(interpreter.path_to_project, "poetry.lock"),
                "r",
                encoding="utf-8",
            ) as spec_lock_file:
                spec_lock = spec_lock_file.read()
            interpreter_spec = EnvironmentSpec(
                environment_type=EnvironmentType.POETRY,
                spec=spec,
                spec_lock=spec_lock,
                python_version=interpreter.python_version,
            )
        elif isinstance(interpreter, LocalInterpreter):
            interpreter_spec = await interpreter.get_interpreter_spec()
        elif isinstance(interpreter, ContainerInterpreterBase):
            interpreter_spec = interpreter.get_interpreter_spec()
            username_password_secret = interpreter._get_username_password_secret()
            if username_password_secret is not None:
                credentials_sources.append(
                    username_password_secret._to_credentials_source(
                        "DOCKER",
                        get_registry_domain(interpreter._get_repository_name())[0],
                        Credentials.Type.USERNAME_PASSWORD,
                    )
                )
        else:
            raise ValueError(f"Unexpected type of interpreter {type(interpreter)}")

        # annoyingly, this tmp dir now gets deleted in run_local when the file
        # has been uploaded/unpacked depending on the Host implementation
        tmp_dir = tempfile.mkdtemp()
        zip_file_path, zip_paths = local_code.zip(
            tmp_dir, include_sys_path, additional_paths
        )

        url = urllib.parse.urlunparse(("file", "", zip_file_path, "", "", ""))
        code = CodeZipFile(url=url, code_paths=zip_paths)

        return cls(interpreter_spec, code, environment_variables, credentials_sources)

    @classmethod
    def git_repo(
        cls,
        repo_url: str,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
        path_to_source: Optional[str] = None,
        interpreter: Union[InterpreterSpecFile, ContainerInterpreterBase, None] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        ssh_key_secret: Optional[Secret] = None,
    ) -> Deployment:
        """
        A deployment based on a git repo.

        Args:
            repo_url: e.g. `"https://github.com/meadowdata/test_repo"`
            branch: defaults to `"main"` if neither branch nor commit are specified.
            commit: can be provided instead of branch to use a specific commit hash,
                e.g. `"d018b54"`
            path_to_source: e.g. `"src/python"` to use a subdirectory of the repo
            interpreter: specifies either a Conda environment.yml or requirements.txt
                file in the Git repo (relative to the repo, note that this IGNORES
                `path_to_source`) This file will be used to generate the environment to
                run in. See
                [CondaEnvironmentYmlFile][meadowrun.CondaEnvironmentYmlFile],
                [PipRequirementsFile][meadowrun.PipRequirementsFile], and
                [PoetryProjectPath][meadowrun.PoetryProjectPath]
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment
            ssh_key_secret: A secret that contains the contents of a private SSH key
                that has read access to `repo_url`, e.g. `AwsSecret("my_ssh_key")`. See
                How to use a private git repo for [AWS](/how_to/private_git_repo_aws)
                or [Azure](/how_to/private_git_repo_azure)

        Returns:
            A `Deployment` object that can be passed to the `run_*` functions.
        """
        if branch and commit:
            raise ValueError("Only one of branch and commit can be specified")

        if not branch and not commit:
            branch = "main"

        if branch:
            code: Union[CodeDeployment, VersionedCodeDeployment, None] = GitRepoBranch(
                repo_url=repo_url,
                branch=branch,
                # the generated protobuf types say that path_to_source must not be None,
                # but this is not actually the case, it handles None fine, which we want
                # to take advantage of
                path_to_source=cast(str, path_to_source),
            )
        else:  # commit
            # guaranteed by prior ifs, this is just for mypy
            assert commit is not None
            code = GitRepoCommit(
                repo_url=repo_url,
                commit=commit,
                # see comment above
                path_to_source=cast(str, path_to_source),
            )

        credentials_sources = []

        if interpreter is None:
            interpreter_spec: Union[
                InterpreterDeployment, VersionedInterpreterDeployment, None
            ] = None
        elif isinstance(interpreter, CondaEnvironmentYmlFile):
            interpreter_spec = EnvironmentSpecInCode(
                environment_type=EnvironmentType.CONDA,
                path_to_spec=interpreter.path_to_yml_file,
            )
        elif isinstance(interpreter, PipRequirementsFile):
            interpreter_spec = EnvironmentSpecInCode(
                environment_type=EnvironmentType.PIP,
                path_to_spec=interpreter.path_to_requirements_file,
                python_version=interpreter.python_version,
            )
        elif isinstance(interpreter, PoetryProjectPath):
            interpreter_spec = EnvironmentSpecInCode(
                environment_type=EnvironmentType.POETRY,
                path_to_spec=interpreter.path_to_project,
                python_version=interpreter.python_version,
            )
        elif isinstance(interpreter, ContainerInterpreterBase):
            interpreter_spec = interpreter.get_interpreter_spec()
            username_password_secret = interpreter._get_username_password_secret()
            if username_password_secret is not None:
                credentials_sources.append(
                    username_password_secret._to_credentials_source(
                        "DOCKER",
                        get_registry_domain(interpreter._get_repository_name())[0],
                        Credentials.Type.USERNAME_PASSWORD,
                    )
                )
        else:
            raise ValueError(f"Unexpected type of interpreter: {type(interpreter)}")

        if ssh_key_secret is not None:
            credentials_sources.append(
                ssh_key_secret._to_credentials_source(
                    "GIT", repo_url, Credentials.Type.SSH_KEY
                )
            )

        return cls(interpreter_spec, code, environment_variables, credentials_sources)


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
    else:
        raise ValueError(
            f"Unknown type of credentials source {type(credentials_source.source)}"
        )
    return result


def _add_defaults_to_deployment(
    deployment: Optional[Deployment],
) -> Tuple[
    Union[InterpreterDeployment, VersionedInterpreterDeployment],
    Union[CodeDeployment, VersionedCodeDeployment],
    Iterable[StringPair],
    List[CredentialsSourceMessage],
]:
    if deployment is None:
        return (
            ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
            ServerAvailableFolder(),
            {},
            [],
        )

    if deployment.credentials_sources:
        credentials_sources = [
            _credentials_source_message(c) for c in deployment.credentials_sources
        ]
    else:
        credentials_sources = []

    if deployment.environment_variables:
        environment_variables = [
            StringPair(key=key, value=value)
            for key, value in deployment.environment_variables.items()
        ]
    else:
        environment_variables = []

    return (
        deployment.interpreter
        or ServerAvailableInterpreter(interpreter_path=MEADOWRUN_INTERPRETER),
        deployment.code or ServerAvailableFolder(),
        environment_variables,
        credentials_sources,
    )


class _DeploymentTarget(Enum):
    LOCAL = "local"
    S3 = "s3"
    AZURE_BLOB_STORAGE = "azure blob storage"

    @staticmethod
    def from_cloud_provider(cloud_provider: CloudProviderType) -> _DeploymentTarget:
        if cloud_provider == "EC2":
            return _DeploymentTarget.S3
        elif cloud_provider == "AzureVM":
            return _DeploymentTarget.AZURE_BLOB_STORAGE
        else:
            raise ValueError(f"Unknown cloud provider {cloud_provider}")

    @staticmethod
    def from_single_host(host: Host) -> _DeploymentTarget:
        if isinstance(host, AllocCloudInstance):
            return _DeploymentTarget.from_cloud_provider(host.cloud_provider)
        else:
            return _DeploymentTarget.LOCAL

    @staticmethod
    def from_hosts(hosts: AllocCloudInstances) -> _DeploymentTarget:
        return _DeploymentTarget.from_cloud_provider(hosts.cloud_provider)


async def _prepare_code_deployment(
    code_deploy: Union[CodeDeployment, VersionedCodeDeployment],
    target: _DeploymentTarget,
) -> Union[CodeDeployment, VersionedCodeDeployment]:
    if not isinstance(code_deploy, CodeZipFile):
        return code_deploy

    if target == _DeploymentTarget.LOCAL:
        return code_deploy
    elif target == _DeploymentTarget.S3:
        # need to deploy zip file to S3, and update the CodeZipFile
        file_url = urllib.parse.urlparse(code_deploy.url)
        if file_url.scheme != "file":
            raise ValueError(f"Expected file URI: {code_deploy.url}")
        if sys.platform == "win32" and file_url.path.startswith("/"):
            # on Windows, file:///C:\foo turns into file_url.path = /C:\foo so we need
            # to remove the forward slash at the beginning
            file_path = file_url.path[1:]
        else:
            file_path = file_url.path
        bucket_name, object_name = await s3.ensure_uploaded(file_path)
        s3_url = urllib.parse.urlunparse(("s3", bucket_name, object_name, "", "", ""))
        code_deploy.url = s3_url
        shutil.rmtree(os.path.dirname(file_path), ignore_errors=True)
        return code_deploy
    elif target == _DeploymentTarget.AZURE_BLOB_STORAGE:
        raise NotImplementedError("Azure Blob Storage is not implemented yet")
    else:
        raise ValueError(f"Unexpected value for target {target}")


@dataclasses.dataclass(frozen=True)
class AllocCloudInstance(Host):
    """
    Specifies the requirements for a cloud instance (e.g. an EC2 instance or Azure VM).

    Attributes:
        logical_cpu_required:
        memory_gb_required:
        interruption_probability_threshold: Specifies what interruption probability
            percent is acceptable. E.g. `80` means that any instance type with an
            interruption probability less than 80% can be used. Use `0` to indicate that
            only on-demand instance are acceptable (i.e. do not use spot instances)
        cloud_provider: `EC2` or `AzureVM`
        region_name:
    """

    logical_cpu_required: int
    memory_gb_required: float
    interruption_probability_threshold: float
    cloud_provider: CloudProviderType
    region_name: Optional[str] = None

    async def run_job(self, job: Job) -> JobCompletion[Any]:
        if self.cloud_provider == "EC2":
            return await run_job_ec2_instance_registrar(
                job,
                self.logical_cpu_required,
                self.memory_gb_required,
                self.interruption_probability_threshold,
                self.region_name,
            )
        elif self.cloud_provider == "AzureVM":
            return await run_job_azure_vm_instance_registrar(
                job,
                self.logical_cpu_required,
                self.memory_gb_required,
                self.interruption_probability_threshold,
                self.region_name,
            )
        else:
            raise ValueError(
                f"Unexpected value for cloud_provider {self.cloud_provider}"
            )


@dataclasses.dataclass(frozen=True)
class AllocCloudInstances:
    """
    Specifies the requirements for a set of cloud instances (e.g. EC2 instances or Azure
    VMs) for running multiple workers. Also see
    [AllocCloudInstance][meadowrun.AllocCloudInstance]

    Attributes:
        logical_cpu_required_per_task:
        memory_gb_required_per_task:
        interruption_probability_threshold: See
            [AllocCloudInstance][meadowrun.AllocCloudInstance]
        cloud_provider: Either `EC2` or `AzureVM`
        num_concurrent_tasks: The number of workers to launch. In the context of a
            [run_map][meadowrun.run_map] call, this can be less than or equal to the
            number of args/tasks. Will default to half the total number of tasks if set
            to None.
        region_name:
    """

    logical_cpu_required_per_task: int
    memory_gb_required_per_task: float
    interruption_probability_threshold: float
    cloud_provider: CloudProviderType
    num_concurrent_tasks: Optional[int] = None
    region_name: Optional[str] = None


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


async def run_function(
    function: Union[Callable[..., _T], str],
    host: Host,
    deployment: Optional[Deployment] = None,
    args: Optional[Sequence[Any]] = None,
    kwargs: Optional[Dict[str, Any]] = None,
) -> _T:
    """
    Runs function on a remote machine, specified by "host".

    Args:
        function: A reference to a function (e.g. `package.module.function_name`), a
            lambda, or a string like `"package.module.function_name"` (which is useful
            if the function cannot be referenced in the current environment but can be
            referenced in the deployed environment)
        host: See [AllocCloudInstance][meadowrun.AllocCloudInstance]. Specifies what
            resources are needed to run this function
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the
            environment (code and libraries) that are needed to run this function
        args: Passed to the function like `function(*args)`
        kwargs: Passed to the function like `function(**kwargs)`

    Returns:
        The result of calling `function`
    """

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()

    # first pickle the function arguments from job_run_spec

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

    # now, construct the PyFunctionJob

    job_id = str(uuid.uuid4())
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

    # now create the Job

    (
        interpreter,
        code,
        environment_variables,
        credentials_sources,
    ) = _add_defaults_to_deployment(deployment)

    code = await _prepare_code_deployment(
        code, _DeploymentTarget.from_single_host(host)
    )

    job = Job(
        job_id=job_id,
        job_friendly_name=friendly_name,
        environment_variables=environment_variables,
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_function=py_function,
        credentials_sources=credentials_sources,
    )
    _add_deployments_to_job(job, code, interpreter)

    job_completion = await host.run_job(job)
    return job_completion.result


async def run_command(
    args: Union[str, Sequence[str]],
    host: Host,
    deployment: Optional[Deployment] = None,
    context_variables: Optional[Dict[str, Any]] = None,
) -> JobCompletion[None]:
    """
    Runs the specified command on a remote machine

    Args:
        args: Specifies the command to run, can be a string (e.g. `"jupyter nbconvert
            --to html analysis.ipynb"`) or a list of strings (e.g. `["jupyter",
            --"nbconvert", "--to", "html", "analysis.ipynb"]`)
        host: See [AllocCloudInstance][meadowrun.AllocCloudInstance]. Specifies what
            resources are needed to run this command
        deployment: See [Deployment][meadowrun.Deployment]. Specifies
            the environment (code and libraries) that are needed to run this command
        context_variables: Experimental feature

    Returns:
        A JobCompletion object that contains metadata about the running of the job.
    """

    job_id = str(uuid.uuid4())
    if isinstance(args, str):
        args = shlex.split(args)
    # this is kind of a silly way to get a friendly name--treat the first three
    # elements of args as if they're paths and take the last part of each path
    friendly_name = "-".join(os.path.basename(arg) for arg in args[:3])

    (
        interpreter,
        code,
        environment_variables,
        credentials_sources,
    ) = _add_defaults_to_deployment(deployment)

    code = await _prepare_code_deployment(
        code, _DeploymentTarget.from_single_host(host)
    )

    if context_variables:
        pickled_context_variables = pickle.dumps(
            context_variables, protocol=_pickle_protocol_for_deployed_interpreter()
        )
    else:
        pickled_context_variables = b""

    job = Job(
        job_id=job_id,
        job_friendly_name=_make_valid_friendly_name(friendly_name),
        environment_variables=environment_variables,
        result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
        py_command=PyCommandJob(
            command_line=args, pickled_context_variables=pickled_context_variables
        ),
        credentials_sources=credentials_sources,
    )
    _add_deployments_to_job(job, code, interpreter)

    return await host.run_job(job)


async def run_map(
    function: Callable[[_T], _U],
    args: Sequence[_T],
    hosts: AllocCloudInstances,
    deployment: Optional[Deployment] = None,
) -> Sequence[_U]:
    """
    Equivalent to `map(function, args)`, but runs distributed and in parallel.

    Args:
        function: A reference to a function (e.g. `package.module.function_name`) or a
            lambda
        args: A list of objects, each item in the list represents a "task",
            where each "task" is an invocation of `function` on the item in the list
        hosts: See [AllocCloudInstances][meadowrun.AllocCloudInstances]. Specifies how
            many workers to provision and what resources are needed for each worker.
        deployment: See [Deployment][meadowrun.Deployment]. Specifies the environment
            (code and libraries) that are needed to run the function

    Returns:
        Returns the result of running `function` on each of `args`
    """

    if not hosts.num_concurrent_tasks:
        num_concurrent_tasks = len(args) // 2 + 1
    else:
        num_concurrent_tasks = min(hosts.num_concurrent_tasks, len(args))

    if hosts.cloud_provider == "EC2":
        helper = await prepare_ec2_run_map(
            function,
            args,
            hosts.region_name,
            hosts.logical_cpu_required_per_task,
            hosts.memory_gb_required_per_task,
            hosts.interruption_probability_threshold,
            num_concurrent_tasks,
        )
    elif hosts.cloud_provider == "AzureVM":
        helper = await prepare_azure_vm_run_map(
            function,
            args,
            hosts.region_name,
            hosts.logical_cpu_required_per_task,
            hosts.memory_gb_required_per_task,
            hosts.interruption_probability_threshold,
            num_concurrent_tasks,
        )
    else:
        raise ValueError(f"Unexpected value for cloud_provider {hosts.cloud_provider}")

    # prepare some variables for constructing the worker jobs
    friendly_name = _get_friendly_name(function)
    (
        interpreter,
        code,
        environment_variables,
        credentials_sources,
    ) = _add_defaults_to_deployment(deployment)

    code = await _prepare_code_deployment(code, _DeploymentTarget.from_hosts(hosts))

    pickle_protocol = _pickle_protocol_for_deployed_interpreter()

    # Now we will run worker_loop jobs on the hosts we got:

    pickled_worker_function = cloudpickle.dumps(
        helper.worker_function, protocol=pickle_protocol
    )

    worker_tasks = []
    worker_id = 0
    for public_address, worker_job_ids in helper.allocated_hosts.items():
        for worker_job_id in worker_job_ids:
            job = Job(
                job_id=worker_job_id,
                job_friendly_name=friendly_name,
                environment_variables=environment_variables,
                result_highest_pickle_protocol=pickle.HIGHEST_PROTOCOL,
                py_function=PyFunctionJob(
                    pickled_function=pickled_worker_function,
                    pickled_function_arguments=pickle.dumps(
                        ([public_address, worker_id], {}), protocol=pickle_protocol
                    ),
                ),
                credentials_sources=credentials_sources,
            )
            _add_deployments_to_job(job, code, interpreter)

            worker_tasks.append(
                asyncio.create_task(
                    SshHost(
                        public_address,
                        helper.fabric_kwargs,
                        (hosts.cloud_provider, helper.region_name),
                    ).run_job(job)
                )
            )

            worker_id += 1

    # finally, wait for results:

    results = await helper.results_future

    # TODO if there's an error these workers will crash before the results_future
    # returns
    await asyncio.gather(*worker_tasks)

    return results


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
    elif isinstance(code_deployment, CodeZipFile):
        job.code_zip_file.CopyFrom(code_deployment)
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
    elif isinstance(interpreter_deployment, EnvironmentSpecInCode):
        job.environment_spec_in_code.CopyFrom(interpreter_deployment)
    elif isinstance(interpreter_deployment, EnvironmentSpec):
        job.environment_spec.CopyFrom(interpreter_deployment)
    else:
        raise ValueError(
            f"Unknown interpreter deployment type {type(interpreter_deployment)}"
        )
