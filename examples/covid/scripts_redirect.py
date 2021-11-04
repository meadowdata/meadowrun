"""
This file should not be necessary, this is a workaround until
https://github.com/python-poetry/poetry/issues/3265 is fixed
"""

import meadowrun.coordinator_main
import meadowrun.job_worker_main
import meadowflow.server.server_main


def meadowrun_coordinator_():
    meadowrun.coordinator_main.command_line_main()


def meadowrun_job_worker_():
    meadowrun.job_worker_main.command_line_main()


def meadowflow_():
    meadowflow.server.server_main.command_line_main()
