from __future__ import annotations

import abc
import dataclasses
import os
import pathlib
import platform
import sys
import tempfile
import urllib.parse
from typing import (
    Optional,
    Union,
    Sequence,
    Mapping,
    Dict,
    List,
    Iterable,
    cast,
    TYPE_CHECKING,
)

from meadowrun.deployment.local_code import zip_local_code
from meadowrun.azure_integration.azure_ssh_keys import get_meadowrun_vault_name
from meadowrun.azure_integration.mgmt_functions.azure_core.azure_rest_api import (
    get_subscription_id_sync,
)
from meadowrun.deployment.conda import try_get_current_conda_env, env_export

from meadowrun.credentials import CredentialsService, CredentialsSourceForService

if TYPE_CHECKING:
    from meadowrun.deployment_internal_types import (
        InterpreterDeployment,
        VersionedInterpreterDeployment,
        CodeDeployment,
        VersionedCodeDeployment,
    )
from meadowrun.docker_controller import get_registry_domain
from meadowrun.meadowrun_pb2 import (
    AwsSecretProto,
    AzureSecretProto,
    CodeZipFile,
    ContainerAtDigest,
    ContainerAtTag,
    Credentials,
    EnvironmentSpec,
    EnvironmentSpecInCode,
    EnvironmentType,
    GitRepoBranch,
    GitRepoCommit,
    KubernetesSecretProto,
    ServerAvailableContainer,
    ServerAvailableFolder,
    ServerAvailableInterpreter,
)
from meadowrun.deployment.pip import pip_freeze_exclude_editable, get_python_version


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


@dataclasses.dataclass(frozen=True)
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


@dataclasses.dataclass(frozen=True)
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


@dataclasses.dataclass(frozen=True)
class KubernetesSecret(Secret):
    """
    A Kubernetes secret

    Attributes:
        secret_name: The name of the secret
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
            source=KubernetesSecretProto(
                credentials_type=credentials_type, secret_name=self.secret_name
            ),
        )


@dataclasses.dataclass(frozen=True)
class PreinstalledInterpreter:
    """
    Represents an interpreter that has been pre-installed on the remote machine. This is
    useful if you're using a [custom AMI](../../how_to/custom_ami).

    Attributes:
        path_to_interpreter: The path to the python executable, e.g.
            `/var/myenv/bin/python`
    """

    path_to_interpreter: str


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


@dataclasses.dataclass(frozen=True)
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
        always_use_local: If this is True, only looks for the image on the EC2 instance
            and does not try to download the image from a container registry. This will
            only work if you've preloaded the image into the AMI via [Use a custom AMI
            on AWS](../../how_to/custom_ami)
    """

    repository_name: str
    tag: str = "latest"
    username_password_secret: Optional[Secret] = None
    always_use_local: bool = False

    def get_interpreter_spec(
        self,
    ) -> Union[InterpreterDeployment, VersionedInterpreterDeployment]:
        if self.always_use_local:
            return ServerAvailableContainer(
                image_name=f"{self.repository_name}:{self.tag}"
            )
        else:
            return ContainerAtTag(repository=self.repository_name, tag=self.tag)

    def _get_repository_name(self) -> str:
        return self.repository_name

    def _get_username_password_secret(self) -> Optional[Secret]:
        return self.username_password_secret


@dataclasses.dataclass(frozen=True)
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
        always_use_local: If this is True, only looks for the image on the EC2 instance
            and does not try to download the image from a container registry. This will
            only work if you've preloaded the image into the AMI via [Use a custom AMI
            on AWS](../../how_to/custom_ami)
    """

    repository_name: str
    digest: str
    username_password_secret: Optional[Secret] = None
    always_use_local: bool = False

    def get_interpreter_spec(
        self,
    ) -> Union[InterpreterDeployment, VersionedInterpreterDeployment]:
        if self.always_use_local:
            return ServerAvailableContainer(
                image_name=f"{self.repository_name}@{self.digest}"
            )
        else:
            return ContainerAtDigest(
                repository=self.repository_name, digest=self.digest
            )

    def _get_repository_name(self) -> str:
        return self.repository_name

    def _get_username_password_secret(self) -> Optional[Secret]:
        return self.username_password_secret


def _additional_software_to_dict(
    additional_software: Union[Sequence[str], str, None],
) -> Mapping[str, str]:
    if additional_software is None:
        return {}

    if isinstance(additional_software, str):
        return {additional_software: ""}

    return {package: "" for package in additional_software}


class InterpreterSpecFile(abc.ABC):
    """
    An abstract base class for specifying a Conda environment.yml file or a pip
    requirements.txt file
    """

    pass


@dataclasses.dataclass(frozen=True)
class CondaEnvironmentYmlFile(InterpreterSpecFile):
    """
    Specifies an environment.yml file generated by conda env export.

    Attributes:
        path_to_yml_file: In the context of mirror_local, this is the path to a file on
            the local disk. In the context of git_repo, this is a path to a file in the
            git repo
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    path_to_yml_file: str
    additional_software: Union[Sequence[str], str, None] = None


@dataclasses.dataclass(frozen=True)
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
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    path_to_requirements_file: str
    python_version: str
    additional_software: Union[Sequence[str], str, None] = None


@dataclasses.dataclass(frozen=True)
class PipRequirementsString(InterpreterSpecFile):
    """
    Specifies pip packages to install as strings.

    Attributes:
        packages_to_install: A list of packages to install.
            `"\n".join(packages_to_install)` will be treated like a requirements.txt
            file
        python_version: A python version like "3.9" or "3.9.5". The version must be
            available on docker: https://hub.docker.com/_/python as
            python-<python_version>-slim-bullseye.
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    packages_to_install: List[str]
    python_version: str
    additional_software: Union[Sequence[str], str, None] = None


@dataclasses.dataclass(frozen=True)
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
            compatible with the requirements in pyproject.toml
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    path_to_project: str
    python_version: str
    additional_software: Union[Sequence[str], str, None] = None


class LocalInterpreter(abc.ABC):
    """
    An abstract base class for specifying a python interpreter on the local machine
    """

    @abc.abstractmethod
    async def get_interpreter_spec(self) -> InterpreterDeployment:
        pass


@dataclasses.dataclass(frozen=True)
class LocalCurrentInterpreter(LocalInterpreter):
    """
    Specifies the current python interpreter.

    Attributes:
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    additional_software: Union[Sequence[str], str, None] = None

    async def get_interpreter_spec(self) -> InterpreterDeployment:
        # first, check if we are in a conda environment
        conda_env_spec = await try_get_current_conda_env()
        if conda_env_spec is not None:
            if platform.system() != "Linux":
                raise ValueError(
                    "mirror_local from a Conda environment only works from Linux "
                    "because conda environments are not cross-platform."
                )
            print("Mirroring current conda environment")
            return EnvironmentSpec(
                environment_type=EnvironmentType.CONDA,
                spec=conda_env_spec,
                additional_software=_additional_software_to_dict(
                    self.additional_software
                ),
            )

        # next, check if we're in a poetry environment. Unfortunately it doesn't seem
        # like there's a way to detect that we're in a poetry environment unless the
        # current working directory or a parent contains the pyproject.toml and
        # poetry.lock files.
        cwd = pathlib.Path.cwd()
        candidates = [cwd]
        candidates.extend(cwd.parents)
        is_poetry_env = False
        for path in candidates:
            pyproject_file = path / "pyproject.toml"
            poetry_lock_file = path / "poetry.lock"

            if pyproject_file.exists() and poetry_lock_file.exists():
                is_poetry_env = True
                break

        if is_poetry_env:
            with open(pyproject_file, encoding="utf-8") as project_file:
                project_file_contents = project_file.read()
            with open(poetry_lock_file, encoding="utf-8") as lock_file:
                lock_file_contents = lock_file.read()
            print("Mirroring current poetry environment")
            return EnvironmentSpec(
                environment_type=EnvironmentType.POETRY,
                spec=project_file_contents,
                spec_lock=lock_file_contents,
                python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
                additional_software=_additional_software_to_dict(
                    self.additional_software
                ),
            )

        # last resort, assume this is a pip-based environment
        print("Mirroring current pip environment")
        return EnvironmentSpec(
            environment_type=EnvironmentType.PIP,
            spec=await pip_freeze_exclude_editable(),
            python_version=f"{sys.version_info.major}.{sys.version_info.minor}",
            additional_software=_additional_software_to_dict(self.additional_software),
        )


@dataclasses.dataclass(frozen=True)
class LocalCondaInterpreter(LocalInterpreter):
    """
    Specifies a locally installed conda environment

    Attributes:
        environment_name_or_path: Either the name of a conda environment (e.g.
            `my_env_name`) or the full path to the folder of a conda environment (e.g.
            `/home/user/miniconda3/envs/my_env_name`). Will be passed to conda env
            export
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    environment_name_or_path: str
    additional_software: Union[Sequence[str], str, None] = None

    async def get_interpreter_spec(self) -> InterpreterDeployment:
        if platform.system() != "Linux":
            raise ValueError(
                "mirror_local from a Conda environment only works from Linux because "
                "conda environments are not cross-platform."
            )
        return EnvironmentSpec(
            environment_type=EnvironmentType.CONDA,
            spec=await env_export(self.environment_name_or_path),
            additional_software=_additional_software_to_dict(self.additional_software),
        )


@dataclasses.dataclass(frozen=True)
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
        additional_software: apt packages that need to be installed to make the
            environment work
    """

    path_to_interpreter: str
    additional_software: Union[Sequence[str], str, None] = None

    async def get_interpreter_spec(self) -> InterpreterDeployment:
        return EnvironmentSpec(
            environment_type=EnvironmentType.PIP,
            # TODO this won't work if the specified environment has editable installs
            spec=await pip_freeze_exclude_editable(self.path_to_interpreter),
            python_version=await get_python_version(self.path_to_interpreter),
            additional_software=_additional_software_to_dict(self.additional_software),
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
        additional_sys_paths: Union[Iterable[str], str] = tuple(),
        include_sys_path_contents: Union[bool, Iterable[str], str] = True,
        interpreter: Union[
            LocalInterpreter,
            InterpreterSpecFile,
            ContainerInterpreterBase,
            PreinstalledInterpreter,
            None,
        ] = None,
        globs: Union[str, Iterable[str], None] = None,
        environment_variables: Optional[Dict[str, str]] = None,
    ) -> Deployment:
        """A deployment that mirrors the local environment and code.

        Args:
            include_sys_path: if `True`, syncs the `sys.path` variable to the remote
                machine. Usually used with `include_python_path_code=True` which is
                responsible for syncing the contents of the folders specified by
                `sys.path`. Excludes the paths on `sys.path` which are part of the
                interpreter, e.g. `site-packages`, as this should be taken care of by
                the `interpreter` argument. It's also possible to use
                `include_sys_path=True`, `include_python_path_code=False`, and set
                `globs` to include code in the `sys.path` directories. Setting `globs`
                to upload code and setting `include_sys_path=False` will result in the
                uploaded code not being on `sys.path` (and therefore potentially
                inaccessible) on the remote machine.
            additional_sys_paths: local code paths that will be treated as if they were
                on `sys.path` (see the `include_sys_path` parameter)
            include_sys_path_contents: If `True`, uploads the python code in
                `sys.path`/`additional_sys_paths` depending on how you set
                `include_sys_path` and `additional_sys_paths`. You can also set this
                parameter to an iterable of strings like `[".py", ".so", ".txt"]` to
                tell Meadowrun to copy the specified extensions rather than the default
                of just `.py` and `.so` files. If `sys.path=False` and
                `additional_sys_paths=None`, this parameter is ignored. If this
                parameter is `False`, you most likely want to use `globs` to explicitly
                specify the files on `sys.path` that you want to upload to the remote
                machine.
            interpreter: Specifies the environment/interpreter to use. Defaults to
                `None` which will detect the currently activated env. Alternatively, you
                can explicitly specify a locally installed python environment with
                [LocalCondaInterpreter][meadowrun.LocalCondaInterpreter],
                [LocalPipInterpreter][meadowrun.LocalPipInterpreter],
                [CondaEnvironmentYmlFile][meadowrun.CondaEnvironmentYmlFile],
                [PipRequirementsFile][meadowrun.PipRequirementsFile],
                [PoetryProjectPath][meadowrun.PoetryProjectPath]
            globs: This parameter can be used for two main purposes.

                One purpose is to include files from your current working directory that
                can then be accessed on the remote machine using relative paths. E.g.
                you can specify `"foo/bar.txt"`, and then your remote code will be able
                to access that file via the relative path `"foo/bar.txt"`. Other
                examples are: `"*.txt"` will specify txt files in your current directory
                (but not recursively). `"**/*.txt"` will specify all txt files in your
                current directory recursively (e.g. will capture both `1.txt` and
                `foo/2.txt`). `"foo/**/*.txt"` will capture all txt files in the foo
                directory. Note that most of the time, your current working directory
                will be on `sys.path` so any `.py` and `.so` files in your local working
                directory will be included by default, but any other file extensions
                will be ignored by default.

                The second purpose is to include an explicit list of files from
                `sys.path`. So for example, you can set
                `include_sys_path_contents=False` and then use this variable to pass an
                explicit list of files that you want to include from `sys.path`.

                In either case, it is okay to specify absolute paths or relative paths,
                but if `globs` specifies files that are outside of the current working
                directory's parent and outside of any paths on
                `sys.path`/`additional_sys_paths`, this function will raise an exception
                because those files will be inaccessible in the remote process.
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment.

        Returns:
            A `Deployment` object that can be passed to the `run_*` functions.
        """

        credentials_sources = []

        if interpreter is None:
            interpreter = LocalCurrentInterpreter()

        if isinstance(interpreter, CondaEnvironmentYmlFile):
            with open(interpreter.path_to_yml_file, encoding="utf-8") as f:
                interpreter_spec: Union[
                    InterpreterDeployment, VersionedInterpreterDeployment
                ] = EnvironmentSpec(
                    environment_type=EnvironmentType.CONDA,
                    spec=f.read(),
                    additional_software=_additional_software_to_dict(
                        interpreter.additional_software
                    ),
                )
        elif isinstance(interpreter, PipRequirementsFile):
            with open(interpreter.path_to_requirements_file, encoding="utf-8") as f:
                interpreter_spec = EnvironmentSpec(
                    environment_type=EnvironmentType.PIP,
                    spec=f.read(),
                    python_version=interpreter.python_version,
                    additional_software=_additional_software_to_dict(
                        interpreter.additional_software
                    ),
                )
        elif isinstance(interpreter, PipRequirementsString):
            interpreter_spec = EnvironmentSpec(
                environment_type=EnvironmentType.PIP,
                spec="\n".join(interpreter.packages_to_install),
                python_version=interpreter.python_version,
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
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
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
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
        elif isinstance(interpreter, PreinstalledInterpreter):
            interpreter_spec = ServerAvailableInterpreter(
                interpreter_path=interpreter.path_to_interpreter
            )
        else:
            raise ValueError(f"Unexpected type of interpreter {type(interpreter)}")

        if globs is None:
            globs = ()
        elif isinstance(globs, str):
            globs = [globs]

        if isinstance(additional_sys_paths, str):
            additional_sys_paths = [additional_sys_paths]

        if include_sys_path_contents is True:
            sys_path_extensions: Iterable[str] = (".py", ".so")
        elif include_sys_path_contents is False:
            sys_path_extensions = ()
        elif isinstance(include_sys_path_contents, str):
            sys_path_extensions = [include_sys_path_contents]
        else:
            sys_path_extensions = include_sys_path_contents

        # annoyingly, this tmp dir now gets deleted in run_local when the file
        # has been uploaded/unpacked depending on the Host implementation
        tmp_dir = tempfile.mkdtemp()
        zip_file_path, zip_python_paths, zip_cwd = zip_local_code(
            tmp_dir,
            include_sys_path,
            additional_sys_paths,
            sys_path_extensions,
            globs,
        )

        # see comment on _upload_code_zip_file
        url = urllib.parse.urlunparse(("file", "", zip_file_path, "", "", ""))
        code = CodeZipFile(url=url, code_paths=zip_python_paths, cwd_path=zip_cwd)

        return cls(interpreter_spec, code, environment_variables, credentials_sources)

    @classmethod
    def git_repo(
        cls,
        repo_url: str,
        branch: Optional[str] = None,
        commit: Optional[str] = None,
        path_to_source: Optional[str] = None,
        interpreter: Union[
            InterpreterSpecFile, ContainerInterpreterBase, PreinstalledInterpreter, None
        ] = None,
        environment_variables: Optional[Dict[str, str]] = None,
        ssh_key_secret: Optional[Secret] = None,
        editable_install: bool = True,
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
            editable_install: Execute editable installs in the project (e.g. -e in pip).
                If True (the default), the interpreter environment executes an install,
                which makes entry points, registered plugins and metadata work. If
                False, editable installs are filtered out.

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
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
                editable_install=editable_install,
            )
        elif isinstance(interpreter, PipRequirementsFile):
            interpreter_spec = EnvironmentSpecInCode(
                environment_type=EnvironmentType.PIP,
                path_to_spec=interpreter.path_to_requirements_file,
                python_version=interpreter.python_version,
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
                editable_install=editable_install,
            )
        elif isinstance(interpreter, PipRequirementsString):
            interpreter_spec = EnvironmentSpec(
                environment_type=EnvironmentType.PIP,
                spec="\n".join(interpreter.packages_to_install),
                python_version=interpreter.python_version,
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
            )
        elif isinstance(interpreter, PoetryProjectPath):
            interpreter_spec = EnvironmentSpecInCode(
                environment_type=EnvironmentType.POETRY,
                path_to_spec=interpreter.path_to_project,
                python_version=interpreter.python_version,
                additional_software=_additional_software_to_dict(
                    interpreter.additional_software
                ),
                editable_install=editable_install,
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
        elif isinstance(interpreter, PreinstalledInterpreter):
            interpreter_spec = ServerAvailableInterpreter(
                interpreter_path=interpreter.path_to_interpreter
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

    @classmethod
    def container_image(
        cls,
        repository: str,
        tag: str = "latest",
        username_password_secret: Optional[Secret] = None,
        environment_variables: Optional[Dict[str, str]] = None,
    ) -> Deployment:
        """
        A deployment based on a docker container image

        Arguments:
            repository: The name of the docker container image repository, e.g. `python`
                or `quay.io/minio/minio`.
            tag: Combined with repository, will be used like `{repository}:{tag}`.
                Defaults to `latest`
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment.
        """
        credentials_sources = []
        if username_password_secret is not None:
            credentials_sources.append(
                username_password_secret._to_credentials_source(
                    "DOCKER",
                    get_registry_domain(repository)[0],
                    Credentials.Type.USERNAME_PASSWORD,
                )
            )
        return cls(
            ContainerAtTag(repository=repository, tag=tag),
            ServerAvailableFolder(),
            environment_variables,
            credentials_sources,
        )

    @classmethod
    def container_image_at_digest(
        cls,
        repository: str,
        digest: str,
        username_password_secret: Optional[Secret] = None,
        environment_variables: Optional[Dict[str, str]] = None,
    ) -> Deployment:
        """
        A deployment based on a docker container image

        Arguments:
            repository: The name of the docker container image repository, e.g. `python`
                or `quay.io/minio/minio`.
            digest: Combined with repository, will be used like `{repository}@{tag}`.
                Defaults to `latest` if digest is not specified.
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment.
        """
        credentials_sources = []
        if username_password_secret is not None:
            credentials_sources.append(
                username_password_secret._to_credentials_source(
                    "DOCKER",
                    get_registry_domain(repository)[0],
                    Credentials.Type.USERNAME_PASSWORD,
                )
            )
        return cls(
            ContainerAtDigest(repository=repository, digest=digest),
            ServerAvailableFolder(),
            environment_variables,
            credentials_sources,
        )

    @classmethod
    def preinstalled_interpreter(
        cls,
        path_to_interpreter: str,
        environment_variables: Optional[Dict[str, str]] = None,
    ) -> Deployment:
        """
        A deployment for using an interpreter that is already installed on the remote
        machine. This makes sense if e.g. you're using a custom AMI.

        Arguments:
            path_to_interpreter: The path to the python executable you want to use in
                the Meadowrun AMI. This will usually only make sense if you are
                specifying a custom AMI. There is also a MEADOWRUN_INTERPRETER constant
                that you can provide here that will tell Meadowrun to use the same
                interpreter that is running the Meadowrun agent. You should only use
                this if you don't care about the version of python or what libraries are
                installed.
            environment_variables: e.g. `{"PYTHONHASHSEED": "0"}`. These environment
                variables will be set in the remote environment.
        """
        return cls(
            ServerAvailableInterpreter(interpreter_path=path_to_interpreter),
            ServerAvailableFolder(),
            environment_variables,
        )
