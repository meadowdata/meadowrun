import os
from typing import Optional

import asyncssh
from meadowrun.ssh import run_and_print, upload_file, write_text_to_file


async def upload_and_configure_meadowrun(
    connection: asyncssh.SSHClientConnection,
    version: str,
    package_root_dir: str,
    pre_command: Optional[str] = None,
) -> None:

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

    # set deallocate_jobs to run from crontab
    crontab_line = (
        "* * * * * /var/meadowrun/env/bin/python -m meadowrun.deallocate_jobs "
        "--cloud EC2 --cloud-region-name default "
        ">> /var/meadowrun/deallocate_jobs.log 2>&1\n"
    )
    await write_text_to_file(
        connection, crontab_line, "/var/meadowrun/meadowrun_crontab"
    )

    await run_and_print(
        connection,
        "crontab < /var/meadowrun/meadowrun_crontab",
    )
    await run_and_print(connection, "rm /var/meadowrun/meadowrun_crontab")
