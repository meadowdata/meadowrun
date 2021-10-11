"""
This file should not be necessary, this is a workaround until
https://github.com/python-poetry/poetry/issues/3265 is fixed
"""

import nextrun.server_main
import nextbeat.server.server_main


def nextrun_():
    nextrun.server_main.command_line_main()


def nextbeat_():
    nextbeat.server.server_main.command_line_main()
