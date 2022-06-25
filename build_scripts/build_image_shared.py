import os
import io
from typing import Optional, cast

import fabric
import paramiko.ssh_exception

from meadowrun.run_job_core import _retry


async def upload_and_configure_meadowrun(
    connection: fabric.Connection,
    version: str,
    package_root_dir: str,
    pre_command: Optional[str] = None,
) -> None:
    # retry with a no-op until we've established a connection
    await _retry(
        lambda: connection.run("echo $HOME"),
        (
            cast(Exception, paramiko.ssh_exception.NoValidConnectionsError),
            cast(Exception, TimeoutError),
        ),
    )

    if pre_command:
        connection.run(pre_command)

    # install the meadowrun package
    connection.put(
        os.path.join(package_root_dir, "dist", f"meadowrun-{version}-py3-none-any.whl"),
        "/var/meadowrun/",
    )
    connection.run(
        "source /var/meadowrun/env/bin/activate "
        f"&& pip install /var/meadowrun/meadowrun-{version}-py3-none-any.whl"
    )
    connection.run(f"rm /var/meadowrun/meadowrun-{version}-py3-none-any.whl")

    # set deallocate_jobs to run from crontab
    crontab_line = (
        "* * * * * /var/meadowrun/env/bin/python -m meadowrun.deallocate_jobs "
        "--cloud EC2 --cloud-region-name default "
        ">> /var/meadowrun/deallocate_jobs.log 2>&1\n"
    )
    with io.StringIO(crontab_line) as sio:
        connection.put(sio, "/var/meadowrun/meadowrun_crontab")
    connection.run("crontab < /var/meadowrun/meadowrun_crontab")
    connection.run("rm /var/meadowrun/meadowrun_crontab")
