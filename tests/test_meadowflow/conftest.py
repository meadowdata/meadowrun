import pytest
from meadowflow.event_log import EventLog


@pytest.fixture
@pytest.mark.asyncio
async def event_log():
    event_log = await EventLog()
    yield event_log
    await event_log.close()
