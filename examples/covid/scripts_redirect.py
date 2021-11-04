"""
This file should not be necessary, this is a workaround until
https://github.com/python-poetry/poetry/issues/3265 is fixed
"""

import meadowgrid.coordinator_main
import meadowgrid.job_worker_main
import meadowflow.server.server_main


def meadowgrid_coordinator_():
    meadowgrid.coordinator_main.command_line_main()


def meadowgrid_job_worker_():
    meadowgrid.job_worker_main.command_line_main()


def meadowflow_():
    meadowflow.server.server_main.command_line_main()
