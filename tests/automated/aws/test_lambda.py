from typing import Tuple
from meadowrun import (
    run_function,
    AllocLambda,
    Resources,
    Deployment,
    MEADOWRUN_INTERPRETER,
)
import pytest

from meadowrun.meadowrun_pb2 import EnvironmentSpec, EnvironmentType


@pytest.mark.asyncio
async def test_server_available_interpreter() -> None:
    #
    # and that's after adding LAMBDA_TASK_ROOT to the PYTHONPATH, and not overwriting
    # PYTHONPATH in launch_non_container_job.

    # I'm not sure how Lambda launches its processes so it finds the site-packages
    # folder, but I think the best way to sidestep the issue is to NOT launch task
    # worker subprocess if the interpreter is server_Avaialble_interpreter.
    result = await run_function(
        lambda x: x**x,
        AllocLambda(),
        Resources(
            memory_gb=0.25,
        ),
        Deployment.preinstalled_interpreter(MEADOWRUN_INTERPRETER),
        args=[5],
    )

    assert result == 5**5


SPEC = """aiobotocore==2.3.3
aiohttp==3.8.1
aioitertools==0.10.0
aiosignal==1.2.0
async-timeout==4.0.2
attrs==21.4.0
bcrypt==3.2.2
boto3==1.21.21
botocore==1.24.21
certifi==2022.6.15
cffi==1.15.0
charset-normalizer==2.0.12
cloudpickle==2.1.0
cryptography==37.0.2
fabric==2.7.0
filelock==3.7.1
frozenlist==1.3.0
idna==3.3
invoke==1.7.1
jmespath==1.0.0
meadowrun==0.1.8
multidict==6.0.2
numpy==1.22.4
pandas==1.4.2
paramiko==2.11.0
pathlib2==2.3.7.post1
protobuf==3.20.1
psutil==5.9.1
pycparser==2.21
PyNaCl==1.5.0
python-dateutil==2.8.2
pytz==2022.1
requests==2.28.0
s3transfer==0.5.2
six==1.16.0
typing_extensions==4.2.0
urllib3==1.26.9
wrapt==1.14.1
yarl==1.7.2"""


@pytest.mark.asyncio
async def test_server_environment_spec() -> None:
    def remote_function(arg: str) -> Tuple[Tuple[str, str], str]:
        import importlib

        # we could just do import requests, but that messes with mypy
        pd = importlib.import_module("pandas")
        requests = importlib.import_module("requests")
        # example = importlib.import_module("example")
        return ((requests.__version__, pd.__version__), f"hello, {arg}")

    interpreter_spec = EnvironmentSpec(
        environment_type=EnvironmentType.ENV_TYPE_PIP,
        spec=SPEC,
        python_version="python3.9",
        additional_software={},
    )

    result = await run_function(
        remote_function,
        AllocLambda(),
        Resources(
            memory_gb=0.25,
        ),
        Deployment(interpreter=interpreter_spec),
        # Deployment.git_repo(
        #     repo_url="https://github.com/meadowdata/test_repo",
        #     branch="main",
        #     path_to_source="example_package",
        #     interpreter=PipRequirementsFile("requirements.txt", "3.9"),
        # ),
        args=["5"],
    )

    assert result == (("2.28.1", "1.4.2"), "hello, 5")
