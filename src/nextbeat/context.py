"""
This module is for processes that are running as nextbeat jobs to access information
about the nextbeat job they are currently running as
"""
import os
import pickle
from typing import Any, Dict


_NEXTRUN_CONTEXT_VARIABLES = "NEXTRUN_CONTEXT_VARIABLES"

_variables = None


def variables() -> Dict[str, Any]:
    global _variables

    if _variables is None:
        if _NEXTRUN_CONTEXT_VARIABLES in os.environ:
            with open(os.environ[_NEXTRUN_CONTEXT_VARIABLES], "rb") as f:
                # TODO probably should provide nicer error messages on unpickling
                _variables = pickle.load(f)
        else:
            _variables = {}

    return _variables
