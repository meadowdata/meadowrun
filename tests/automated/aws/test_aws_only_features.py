from __future__ import annotations

from typing import Tuple

import pytest
from automated.aws.test_basics import AwsHostProvider
from meadowrun import (
    Deployment,
    PipRequirementsFile,
    Resources,
    run_command,
    run_function,
)


class TestAWSOnlyFeatures(AwsHostProvider):
    """
    These tests should get moved into BasicsSuite as we add support for them on all
    platforms
    """

    @pytest.mark.asyncio
    async def test_nvidia_gpu(self) -> None:
        deployment = Deployment.git_repo(
            "https://github.com/meadowdata/test_repo",
            interpreter=PipRequirementsFile("requirements_with_torch.txt", "3.9"),
        )

        for resources in (
            Resources(1, 1, gpus=1, flags="nvidia"),
            Resources(1, 1, gpu_memory=1, flags="nvidia"),
        ):
            results: Tuple[bool, Tuple[int, int]] = await run_function(
                "gpu_tests.check_for_cuda", self.get_host(), resources, deployment
            )
            assert results[0]
            if resources.gpu_memory is not None:
                assert results[1][1] / (1000**3) > resources.gpu_memory

            job_completion = await run_command(
                "nvidia-smi", self.get_host(), resources, deployment
            )
            assert job_completion.return_code == 0

    @pytest.mark.asyncio
    async def manual_test_ports(self) -> None:
        """
        This test requires manually checking that <host>:80 returns something.

        TODO In order to make this an automated test, we'd need to allow getting the
        name of the host we're running on while we're running, and then have a
        programmatic way of killing the job (or we could just build a test container
        that exits on its own after some number of seconds)
        """
        await run_command(
            "python app.py",
            self.get_host(),
            self.get_resources_required(),
            Deployment.container_image("okteto/sample-app"),
            ports=80,
        )
