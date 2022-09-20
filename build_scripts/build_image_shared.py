from __future__ import annotations

import os
from typing import Optional, TYPE_CHECKING

import asyncssh

from meadowrun.run_job_local import MACHINE_CACHE_FOLDER

if TYPE_CHECKING:
    from meadowrun.run_job_core import CloudProviderType
from meadowrun.ssh import (
    run_and_capture,
    run_and_print,
    upload_file,
    write_text_to_file,
)


async def upload_and_configure_meadowrun(
    connection: asyncssh.SSHClientConnection,
    version: str,
    package_root_dir: str,
    cloud_provider: CloudProviderType,
    image_name: str,
    pre_command: Optional[str] = None,
) -> str:

    if pre_command:
        await connection.run(pre_command, check=True)

    # install the meadowrun package
    await upload_file(
        connection,
        os.path.join(package_root_dir, "dist", f"meadowrun-{version}-py3-none-any.whl"),
        "/var/meadowrun/",
    )
    await run_and_print(
        connection,
        "source /var/meadowrun/env/bin/activate "
        f"&& pip install /var/meadowrun/meadowrun-{version}-py3-none-any.whl",
    )
    await run_and_print(
        connection,
        f"rm /var/meadowrun/meadowrun-{version}-py3-none-any.whl",
    )

    # compile the meadowrun env to pyc - this reduces initial startup time
    await run_and_print(
        connection,
        "/var/meadowrun/env/bin/python -m compileall /var/meadowrun/env/lib",
        # returns non-zero if syntax errors are found, but compiles most files anyway
        check=False,
    )

    output = (await run_and_capture(connection, "echo $HOME")).stdout
    if output is None:
        raise ValueError("Result of echo $HOME was None")
    if isinstance(output, bytes):
        home_dir = output.decode("utf-8").strip()
    else:
        home_dir = output.strip()
    systemd_config_dir = "/etc/systemd/system/"

    # set deallocate_jobs to run via systemd timer
    await write_text_to_file(
        connection,
        "[Unit]\n"
        "Description=Deallocate Meadowrun jobs that have quit unexpectedly\n"
        "[Service]\n"
        "ExecStart=/var/meadowrun/env/bin/python -m meadowrun.deallocate_jobs --cloud "
        f"{cloud_provider} --cloud-region-name default\n"
        "StandardOutput=append:/var/meadowrun/deallocate_jobs.log\n"
        "StandardError=append:/var/meadowrun/deallocate_jobs.log\n"
        f"Environment=HOME={home_dir}\n",
        f"{home_dir}/meadowrun-deallocate-jobs.service",
    )

    await write_text_to_file(
        connection,
        "[Unit]\n"
        "Description=Deallocate Meadowrun jobs that have quit unexpectedly (timer)\n"
        "[Timer]\n"
        "OnBootSec=30\n"
        "OnUnitActiveSec=30\n"
        "AccuracySec=1\n"
        "[Install]\n"
        "WantedBy=timers.target\n",
        f"{home_dir}/meadowrun-deallocate-jobs.timer",
    )

    await run_and_print(
        connection,
        f"sudo mv {home_dir}/meadowrun-deallocate-jobs.service "
        f"{home_dir}/meadowrun-deallocate-jobs.timer {systemd_config_dir}",
    )
    await run_and_print(
        connection,
        "sudo systemctl enable --now meadowrun-deallocate-jobs.timer",
    )

    # set check_spot_eviction to run from systemd timer
    await write_text_to_file(
        connection,
        "[Unit]\n"
        "Description=Check for spot instance eviction for Meadowrun\n"
        "[Service]\n"
        "ExecStart=/var/meadowrun/env/bin/python -m meadowrun.check_spot_eviction"
        f" --cloud {cloud_provider} --cloud-region-name default\n"
        "StandardOutput=append:/var/meadowrun/check_spot_eviction.log\n"
        "StandardError=append:/var/meadowrun/check_spot_eviction.log\n"
        f"Environment=HOME={home_dir}\n",
        f"{home_dir}/meadowrun-check-spot-eviction.service",
    )

    await write_text_to_file(
        connection,
        "[Unit]\n"
        "Description=Check for spot instance eviction for Meadowrun (timer)\n"
        "[Timer]\n"
        "OnBootSec=10\n"
        "OnUnitActiveSec=10\n"
        "AccuracySec=1\n"
        "[Install]\n"
        "WantedBy=timers.target\n",
        f"{home_dir}/meadowrun-check-spot-eviction.timer",
    )

    await run_and_print(
        connection,
        f"sudo mv {home_dir}/meadowrun-check-spot-eviction.service "
        f"{home_dir}/meadowrun-check-spot-eviction.timer {systemd_config_dir}",
    )
    await run_and_print(
        connection,
        "sudo systemctl enable --now meadowrun-check-spot-eviction.timer",
    )

    # TODO move this into the base image at some point
    await run_and_print(connection, f"mkdir -p {MACHINE_CACHE_FOLDER}")
    await run_and_print(
        connection,
        "mkdir -p /var/meadowrun/job_logs /var/meadowrun/io /var/meadowrun/git_repos "
        "/var/meadowrun/local_copies /var/meadowrun/misc",
    )

    return image_name
