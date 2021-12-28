import functools
import pickle

import grpc.aio

from meadowflow.meadowgrid_job_runner import MeadowGridJobRunner
from meadowflow.scheduler import Scheduler
from meadowflow.server.config import DEFAULT_HOST, DEFAULT_PORT
from meadowflow.server.meadowflow_pb2 import (
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
from meadowflow.server.meadowflow_pb2_grpc import (
    MeadowFlowServerServicer,
    add_MeadowFlowServerServicer_to_server,
)


class MeadowFlowServerHandler(MeadowFlowServerServicer):
    """See docstrings on MeadowFlowClientAsync"""

    def __init__(self, scheduler: Scheduler):
        self._scheduler = scheduler

    async def add_jobs(  # type: ignore[override]
        self, request: AddJobsRequest, context: grpc.aio.ServicerContext
    ) -> AddJobsResponse:
        self._scheduler.add_jobs(pickle.loads(request.pickled_job_definitions))
        return AddJobsResponse(status="success")

    async def instantiate_scopes(  # type: ignore[override]
        self, request: InstantiateScopesRequest, context: grpc.aio.ServicerContext
    ) -> InstantiateScopesResponse:
        for scope in pickle.loads(request.pickled_scopes):
            self._scheduler.instantiate_scope(scope)
        return InstantiateScopesResponse()

    async def get_events(  # type: ignore[override]
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

    async def register_job_runner(  # type: ignore[override]
        self, request: RegisterJobRunnerRequest, context: grpc.aio.ServicerContext
    ) -> RegisterJobRunnerResponse:
        # TODO some resemblance to jobs._JOB_RUNNER_TYPES here
        if request.job_runner_type == "meadowgrid":
            job_runner_constructor = functools.partial(
                MeadowGridJobRunner, address=request.address
            )
        elif request.job_runner_type == "local":
            # TODO figure out how to configure the local job runner
            raise ValueError("local job runners cannot be registered dynamically")
        else:
            raise ValueError(f"Unrecognized job runner type {request.job_runner_type}")

        self._scheduler.register_job_runner_on_event_loop(job_runner_constructor)

        return RegisterJobRunnerResponse()

    async def manual_run(  # type: ignore[override]
        self, request: ManualRunRequest, context: grpc.aio.ServicerContext
    ) -> ManualRunResponse:
        run_request_id = await self._scheduler.manual_run_on_event_loop(
            pickle.loads(request.pickled_job_name),
            pickle.loads(request.pickled_job_run_overrides),
        )
        return ManualRunResponse(run_request_id=run_request_id)


async def start_meadowflow_server(
    scheduler: Scheduler, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT
) -> None:
    server = grpc.aio.server()
    add_MeadowFlowServerServicer_to_server(MeadowFlowServerHandler(scheduler), server)
    server.add_insecure_port(f"{host}:{port}")
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        # Shuts down the server with 5 seconds of grace period. During the grace period,
        # the server won't accept new connections and allow existing RPCs to continue
        # within the grace period.
        await server.stop(5)
