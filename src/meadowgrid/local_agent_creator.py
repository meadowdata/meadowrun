from __future__ import annotations

import asyncio
import pathlib
from types import TracebackType
from typing import Iterable, Tuple, Dict, List, Any, Optional, Type, Generator

import pandas as pd

import meadowgrid.agent_main
from meadowgrid.agent_creator import AgentCreator, OnDemandOrSpotType
from meadowgrid.config import MEMORY_GB, LOGICAL_CPU


def _generate_local_instance_types(
    memory_cpu_prices: Iterable[Tuple[float, float, float]]
) -> Tuple[Dict[str, Tuple[float, float]], pd.DataFrame]:
    """
    Create some instance type data. First result value is a dictionary mapping from
    instance_type -> (memory_gb, logical_cpu). Second result value is a dataframe as
    specified in agent_creator:choose_instance_types_for_job
    """
    instances_dict = {}
    records = []
    for memory_gb, logical_cpu, price in memory_cpu_prices:
        name = f"{memory_gb}gb{logical_cpu}cpu"
        instances_dict[name] = memory_gb, logical_cpu
        records.append((name, memory_gb, logical_cpu, price, 0, "on_demand"))

    return instances_dict, pd.DataFrame.from_records(
        records,
        columns=[
            "instance_type",
            "memory_gb",
            "logical_cpu",
            "price",
            "interruption_probability",
            "on_demand_or_spot",
        ],
    )


_LOCAL_INSTANCE_TYPES = _generate_local_instance_types(
    [
        (2, 1, 0.020),
        (4, 2, 0.045),
        (8, 4, 0.100),
        (4, 1, 0.030),
        (8, 2, 0.065),
        (2, 2, 0.025),
        (4, 4, 0.055),
    ]
)


class LocalAgentCreator(AgentCreator):
    """Creates local agents. For testing"""

    def __init__(self) -> None:
        # a bit awkward that this is here, but it kind of makes sense as this is
        # effectively test code that needs to live in the main meadowgrid module
        self._test_working_folder = str(
            (
                pathlib.Path(__file__).parent.parent.parent / "test_data" / "meadowgrid"
            ).resolve()
        )
        self._created_agents: List[Any] = []  # just to prevent garbage collection
        self._instance_types: Optional[pd.DataFrame] = None
        self._awaited = False

    async def __aenter__(self) -> LocalAgentCreator:
        if self._awaited:
            return self

        self._query_prices_task = asyncio.create_task(self._query_prices())
        self._awaited = True

        return self

    async def _query_prices(self) -> None:
        # just like a "real" AgentCreator, simulate taking some time to query prices for
        # the first time
        await asyncio.sleep(3)
        self._instance_types = _LOCAL_INSTANCE_TYPES[1]

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    def __await__(self) -> Generator[Any, None, LocalAgentCreator]:
        return self.__aenter__().__await__()

    async def close(self) -> None:
        self._query_prices_task.cancel()
        try:
            await self._query_prices_task
        except asyncio.exceptions.CancelledError:
            pass

    async def get_instance_types(self) -> Optional[pd.DataFrame]:
        await self._query_prices_task
        return self._instance_types

    async def launch_job_specific_agent(
        self,
        agent_id: str,
        job_id: str,
        instance_type: str,
        on_demand_or_spot: OnDemandOrSpotType,
    ) -> None:
        if instance_type not in _LOCAL_INSTANCE_TYPES[0]:
            raise ValueError(f"Unknown instance type {instance_type}")
        memory_gb, logical_cpu = _LOCAL_INSTANCE_TYPES[0][instance_type]

        # again, awkward that the coordinator host is not configurable here, it will
        # always just use the default
        agent = meadowgrid.agent_main.main_in_child_process(
            self._test_working_folder + str(len(self._created_agents)),
            {MEMORY_GB: memory_gb, LOGICAL_CPU: logical_cpu},
            agent_id=agent_id,
            job_id=job_id,
        )
        agent.__enter__()
        self._created_agents.append(agent)
        print(f"Created local agent {instance_type}")
