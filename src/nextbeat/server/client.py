import pickle
from typing import List, Optional

import grpc
import grpc.aio

from nextbeat.event_log import Event
from nextbeat.scopes import ScopeValues
from nextbeat.topic_names import TopicName
from nextbeat.jobs import Job, JobRunOverrides
from nextbeat.server.config import DEFAULT_ADDRESS
from nextbeat.server.nextbeat_pb2 import (
    AddJobsRequest,
    EventsRequest,
    RegisterJobRunnerRequest,
    ManualRunRequest,
    InstantiateScopesRequest,
)
from nextbeat.server.nextbeat_pb2_grpc import NextBeatServerStub


class NextBeatClientAsync:
    """
    The main API for nextbeat, allows users to interact with a nextbeat server. Very
    broadly, think of this as an RPC wrapper around nextbeat.scheduler.
    """

    def __init__(self, address: str = DEFAULT_ADDRESS):
        self._channel = grpc.aio.insecure_channel(address)
        self._stub = NextBeatServerStub(self._channel)

    async def add_jobs(self, jobs: List[Job]) -> None:
        """
        Adds jobs to the nextbeat server we're connected to

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
        Gets events from the nextbeat server. If topic_names is None/empty, then all
        events will be returned. If topic_names has values, then events will be filtered
        by those topic_names.

        TODO error handling
        TODO use real protobuf messages instead of pickles so we can evolve the schema
        """
        return pickle.loads(
            (
                await self._stub.get_events(
                    EventsRequest(pickled_topic_names=pickle.dumps(topic_names))
                )
            ).pickled_events
        )

    async def register_job_runner(self, job_runner_type: str, address: str) -> None:
        """
        Tells the nextbeat server to create a job runner of type job_runner_type pointed
        at address.

        Currently the only supported types are "nextrun".

        # TODO error handling, think about return type
        """
        await self._stub.register_job_runner(
            RegisterJobRunnerRequest(job_runner_type=job_runner_type, address=address)
        )

    async def manual_run(
        self, job_name: TopicName, job_run_overrides: Optional[JobRunOverrides] = None
    ) -> None:
        """
        Execute the Run Action on the specified job.

        TODO error handling, return type
        """
        await self._stub.manual_run(
            ManualRunRequest(
                pickled_job_name=pickle.dumps(job_name),
                pickled_job_run_overrides=pickle.dumps(job_run_overrides),
            )
        )

    async def __aenter__(self):
        await self._channel.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._channel.__aexit__(exc_type, exc_val, exc_tb)


class NextBeatClientSync:
    """
    The non-async version of NextBeatClientAsync, see NextBeatClientAsync for docstrings
    """

    def __init__(self, address: str = DEFAULT_ADDRESS):
        self._channel = grpc.insecure_channel(address)
        self._stub = NextBeatServerStub(self._channel)

    def add_jobs(self, jobs: List[Job]) -> None:
        self._stub.add_jobs(AddJobsRequest(pickled_job_definitions=pickle.dumps(jobs)))

    def instantiate_scopes(self, scopes: List[ScopeValues]) -> None:
        self._stub.instantiate_scopes(
            InstantiateScopesRequest(pickled_scopes=pickle.dumps(scopes))
        )

    def get_events(self, topic_names: Optional[List[TopicName]]) -> List[Event]:
        return pickle.loads(
            self._stub.get_events(
                EventsRequest(pickled_topic_names=pickle.dumps(topic_names))
            ).pickled_events
        )

    def register_job_runner(self, job_runner_type: str, address: str) -> None:
        self._stub.register_job_runner(
            RegisterJobRunnerRequest(job_runner_type=job_runner_type, address=address)
        )

    def manual_run(
        self, job_name: TopicName, job_run_overrides: Optional[JobRunOverrides] = None
    ) -> None:
        self._stub.manual_run(
            ManualRunRequest(
                pickled_job_name=pickle.dumps(job_name),
                pickled_job_run_overrides=pickle.dumps(job_run_overrides),
            )
        )

    def __enter__(self):
        self._channel.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._channel.__exit__(exc_type, exc_val, exc_tb)
