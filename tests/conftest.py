import asyncio
import sys

import pytest

# See comment in meadowrun/__init__.py
if sys.platform == "win32" and sys.version_info < (3, 8):

    @pytest.fixture
    def event_loop():
        loop = asyncio.ProactorEventLoop()
        yield loop
        loop.close()
