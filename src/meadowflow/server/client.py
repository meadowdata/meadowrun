from __future__ import annotations

import asyncio
import pickle
import time
from types import TracebackType
from typing import List, Literal, Optional, Type, cast

import grpc
import grpc.aio
from meadowflow.event_log import Event
from meadowflow.jobs import Job, JobPayload, JobRunOverrides
from meadowflow.scopes import ScopeValues
from meadowflow.server.config import DEFAULT_ADDRESS
from meadowflow.server.meadowflow_pb2 import (
    AddJobsRequest,
    EventsRequest,
    InstantiateScopesRequest,
    ManualRunRequest,
    RegisterJobRunnerRequest,
)
from meadowflow.server.meadowflow_pb2_grpc import MeadowFlowServerStub
from meadowflow.topic_names import TopicName

# TODO Figure out a better place to put _grpc_retry option.
from meadowgrid.coordinator_client import _grpc_retry_option


def _is_request_id_completed(events: List[Event[JobPayload]], request_id: str) -> bool:
    for event in events:
        if event.payload.request_id == request_id:
            if event.payload.state in ("SUCCEEDED", "CANCELLED", "FAILED"):
                return True
            else:
                # we're iterating from most recent to oldest, so if we see a
                # non-completion state, that means we have not completed
                return False
    raise ValueError(f"request_id {request_id} has no events")


_POLL_PERIOD = 0.5  # poll every 500ms


class MeadowFlowClientAsync:
    """
    The main API for meadowflow, allows users to interact with a meadowflow server. Very
    broadly, think of this as an RPC wrapper around meadowflow.scheduler.
    """

    def __init__(self, address: str = DEFAULT_ADDRESS):
        self._channel = grpc.aio.insecure_channel(
            address, options=[_grpc_retry_option("meadowflow", "MeadowFlowServer")]
        )
        self._stub = MeadowFlowServerStub(self._channel)

    async def add_jobs(self, jobs: List[Job]) -> None:
        """
        Adds jobs to the meadowflow server we're connected to

        TODO what do we want to return here, probably some errors?
        TODO use real protobuf messages instead of pickles so we can evolve the schema
        """
        await self._stub.add_jobs(
            AddJobsRequest(pickled_job_definitions=pickle.dumps(jobs))
        )

    async def instantiate_scopes(self, scopes: List[ScopeValues]) -> None:
        """
        Instantiates scopes, see Scheduler.instantiate_scope

        TODO use real protobuf messages instead of pickles so we can evolve the schema
        """
        await self._stub.instantiate_scopes(
            InstantiateScopesRequest(pickled_scopes=pickle.dumps(scopes))
        )

    async def get_events(self, topic_names: Optional[List[TopicName]]) -> List[Event]:
        """
        Gets events from the meadowflow server. If topic_names is None/empty, then all
        events will be returned. If topic_names has values, then events will be filtered
        by those topic_names.

        TODO error handling
        TODO use real protobuf messages instead of pickles so we can evolve the schema
        """
        return cast(
            List[Event],
            pickle.loads(
                (
                    await self._stub.get_events(
                        EventsRequest(pickled_topic_names=pickle.dumps(topic_names))
                    )
                ).pickled_events
            ),
        )

    async def register_job_runner(self, job_runner_type: str, address: str) -> None:
        """
        Tells the meadowflow server to create a job runner of type job_runner_type
        pointed at address.

        Currently the only supported types are "meadowgrid".

        TODO error handling, think about return type
        """
        await self._stub.register_job_runner(
            RegisterJobRunnerRequest(job_runner_type=job_runner_type, address=address)
        )

    async def manual_run(
        self,
        job_name: TopicName,
        job_run_overrides: Optional[JobRunOverrides] = None,
        wait_for_completion: bool = False,
    ) -> None:
        """
        Execute the Run Action on the specified job.

        TODO error handling, return type
        """
        response = await self._stub.manual_run(
            ManualRunRequest(
                pickled_job_name=pickle.dumps(job_name),
                pickled_job_run_overrides=pickle.dumps(job_run_overrides),
            )
        )
        if wait_for_completion:
            while not _is_request_id_completed(
                await self.get_events([job_name]), response.run_request_id
            ):
                await asyncio.sleep(_POLL_PERIOD)

    async def __aenter__(self) -> MeadowFlowClientAsync:
        await self._channel.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Optional[bool]:
        return await self._channel.__aexit__(exc_type, exc_value, traceback)  # type: ignore[no-any-return] # noqa E501


class MeadowFlowClientSync:
    """
    The non-async version of MeadowFlowClientAsync, see MeadowFlowClientAsync for
    docstrings
    """

    def __init__(self, address: str = DEFAULT_ADDRESS):
        self._channel = grpc.insecure_channel(
            address, options=[_grpc_retry_option("meadowflow", "MeadowFlowServer")]
        )
        self._stub = MeadowFlowServerStub(self._channel)

    def add_jobs(self, jobs: List[Job]) -> None:
        self._stub.add_jobs(AddJobsRequest(pickled_job_definitions=pickle.dumps(jobs)))

    def instantiate_scopes(self, scopes: List[ScopeValues]) -> None:
        self._stub.instantiate_scopes(
            InstantiateScopesRequest(pickled_scopes=pickle.dumps(scopes))
        )

    def get_events(self, topic_names: Optional[List[TopicName]]) -> List[Event]:
        return cast(
            List[Event],
            pickle.loads(
                self._stub.get_events(
                    EventsRequest(pickled_topic_names=pickle.dumps(topic_names))
                ).pickled_events
            ),
        )

    def register_job_runner(self, job_runner_type: str, address: str) -> None:
        self._stub.register_job_runner(
            RegisterJobRunnerRequest(job_runner_type=job_runner_type, address=address)
        )

    def manual_run(
        self,
        job_name: TopicName,
        job_run_overrides: Optional[JobRunOverrides] = None,
        wait_for_completion: bool = False,
    ) -> None:
        response = self._stub.manual_run(
            ManualRunRequest(
                pickled_job_name=pickle.dumps(job_name),
                pickled_job_run_overrides=pickle.dumps(job_run_overrides),
            )
        )
        if wait_for_completion:
            while not _is_request_id_completed(
                self.get_events([job_name]), response.run_request_id
            ):
                time.sleep(_POLL_PERIOD)

    def __enter__(self) -> MeadowFlowClientSync:
        self._channel.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        return self._channel.__exit__(exc_type, exc_value, traceback)  # type: ignore[no-any-return] # noqa E501
