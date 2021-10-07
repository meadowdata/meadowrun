import functools
import pickle

import grpc.aio

from nextbeat.nextrun_job_runner import NextRunJobRunner
from nextbeat.scheduler import Scheduler
from nextbeat.server.config import DEFAULT_HOST, DEFAULT_PORT
from nextbeat.server.nextbeat_pb2 import (
    AddJobsRequest,
    AddJobsResponse,
    EventsRequest,
    Events,
    ManualRunRequest,
    ManualRunResponse,
    RegisterJobRunnerRequest,
    RegisterJobRunnerResponse,
    InstantiateScopesRequest,
    InstantiateScopesResponse,
)
from nextbeat.server.nextbeat_pb2_grpc import (
    NextBeatServerServicer,
    add_NextBeatServerServicer_to_server,
)


class NextBeatServerHandler(NextBeatServerServicer):
    """See docstrings on NextBeatClientAsync"""

    def __init__(self, scheduler: Scheduler):
        self._scheduler = scheduler

    async def add_jobs(
        self, request: AddJobsRequest, context: grpc.aio.ServicerContext
    ) -> AddJobsResponse:
        self._scheduler.add_jobs(pickle.loads(request.pickled_job_definitions))
        return AddJobsResponse(status="success")

    async def instantiate_scopes(
        self, request: InstantiateScopesRequest, context: grpc.aio.ServicerContext
    ) -> InstantiateScopesResponse:
        for scope in pickle.loads(request.pickled_scopes):
            self._scheduler.instantiate_scope(scope)
        return InstantiateScopesResponse()

    async def get_events(
        self, request: EventsRequest, context: grpc.aio.ServicerContext
    ) -> Events:
        topic_names = pickle.loads(request.pickled_topic_names)

        if len(topic_names) == 0:
            return Events(
                pickled_events=pickle.dumps(self._scheduler._event_log._event_log)
            )
        else:
            return Events(
                pickled_events=pickle.dumps(
                    [e for t in topic_names for e in self._scheduler.events_of(t)]
                )
            )

    async def register_job_runner(
        self, request: RegisterJobRunnerRequest, context: grpc.aio.ServicerContext
    ) -> RegisterJobRunnerResponse:
        # TODO some resemblance to jobs._JOB_RUNNER_TYPES here
        if request.job_runner_type == "nextrun":
            job_runner_constructor = functools.partial(
                NextRunJobRunner, address=request.address
            )
        elif request.job_runner_type == "local":
            # TODO figure out how to configure the local job runner
            raise ValueError("local job runners cannot be registered dynamically")
        else:
            raise ValueError(f"Unrecognized job runner type {request.job_runner_type}")

        self._scheduler.register_job_runner_on_event_loop(job_runner_constructor)

        return RegisterJobRunnerResponse()

    async def manual_run(
        self, request: ManualRunRequest, context: grpc.aio.ServicerContext
    ) -> ManualRunResponse:
        await self._scheduler.manual_run_on_event_loop(
            pickle.loads(request.pickled_job_name),
            pickle.loads(request.pickled_job_run_overrides),
        )
        return ManualRunResponse()


async def start_nextbeat_server(
    scheduler: Scheduler, host: str = DEFAULT_HOST, port: str = DEFAULT_PORT
) -> None:
    server = grpc.aio.server()
    add_NextBeatServerServicer_to_server(NextBeatServerHandler(scheduler), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        # Shuts down the server with 0 seconds of grace period. During the grace period,
        # the server won't accept new connections and allow existing RPCs to continue
        # within the grace period.
        await server.stop(0)
