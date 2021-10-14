"""
This file should not be necessary, this is a workaround until
https://github.com/python-poetry/poetry/issues/3265 is fixed
"""

import meadowrun.server_main
import meadowflow.server.server_main


def meadowrun_():
    meadowrun.server_main.command_line_main()


def meadowflow_():
    meadowflow.server.server_main.command_line_main()
