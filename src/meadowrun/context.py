from __future__ import annotations

import os
import pickle
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

if TYPE_CHECKING:
    from typing_extensions import Literal, Final

_UNINITIALIZED: Final = "__UNINITIALIZED__"


_MEADOWRUN_CONTEXT_VARIABLES = "MEADOWRUN_CONTEXT_VARIABLES"

_variables: Optional[Dict[str, Any]] = None


def variables() -> Dict[str, Any]:
    global _variables

    if _variables is None:
        if _MEADOWRUN_CONTEXT_VARIABLES in os.environ:
            with open(os.environ[_MEADOWRUN_CONTEXT_VARIABLES], "rb") as f:
                # TODO probably should provide nicer error messages on unpickling
                _variables = pickle.load(f)
        else:
            _variables = {}

    return _variables


_MEADOWRUN_RESULT_FILE = "MEADOWRUN_RESULT_FILE"
_MEADOWRUN_RESULT_PICKLE_PROTOCOL = "MEADOWRUN_RESULT_PICKLE_PROTOCOL"

# We use a non-None placeholder value because we want to use None to mean that these
# variables were not set
_result_request: Optional[
    Union[Literal["__UNINITIALIZED__"], Tuple[str, int]]
] = _UNINITIALIZED


def result_request() -> Optional[Tuple[str, int]]:
    """
    If this returns non-None, the result is (result file, pickle protocol). If the
    result is non-None, that means that the current process was launched by meadowrun in
    a way that meadowrun wasn't able to wrap the process to guarantee that results get
    written. As a result, we should do our best to write results to the specified file
    using the specified pickle protocol version.
    """
    global _result_request

    if _result_request == _UNINITIALIZED:
        if _MEADOWRUN_RESULT_FILE in os.environ:
            file = os.environ[_MEADOWRUN_RESULT_FILE]
            # We should never have _MEADOWGRID_RESULT_FILE without
            # _MEADOWGRID_RESULT_PICKLE_PROTOCOL which should always be an integer.
            # Instead of failing if this is the case, though, we just assume a
            # (relatively) low pickle protocol
            protocol = 3  # the fallback protocol version
            if _MEADOWRUN_RESULT_PICKLE_PROTOCOL in os.environ:
                protocol_str = os.environ[_MEADOWRUN_RESULT_PICKLE_PROTOCOL]
                try:
                    protocol = int(protocol_str)
                except ValueError:
                    # ignore error and use fallback
                    pass
            _result_request = file, protocol
        else:
            _result_request = None

    return _result_request
