"""
This module contains functionality for interacting with Kubernetes that theoretically
has nothing to do with Meadowrun. I.e. you can think of this as a slightly higher-level
version of the kubernetes_asyncio module.
"""

from __future__ import annotations

import asyncio
import base64
import enum
import sys
import time
import traceback
from typing import Callable, List, Dict, Tuple, Optional

import aiohttp
from kubernetes_asyncio import client as kubernetes_client, watch as kubernetes_watch

from meadowrun.run_job_core import WaitOption


# kubernetes_asyncio.watch.Watch looks at the docstring to figure out if the function
# passed to Watch takes a follow or watch argument. This results in the wrong value for
# read_namespaced_pod_log, so we just monkey-patch the function here
_orig_get_watch_argument_name = kubernetes_watch.Watch.get_watch_argument_name


def _new_get_watch_argument_name(watch: kubernetes_watch.Watch, func: Callable) -> str:
    if getattr(func, "__name__") == "read_namespaced_pod_log":
        return "follow"
    return _orig_get_watch_argument_name(watch, func)


kubernetes_watch.watch.Watch.get_watch_argument_name = _new_get_watch_argument_name


async def get_pods_for_job(
    core_api: kubernetes_client.CoreV1Api,
    kubernetes_namespace: str,
    job_name: str,
    pod_generate_names: List[str],
) -> List[kubernetes_client.V1Pod]:
    """
    When you launch a Kubernetes job, one or more pods get created. In our case, we
    should only ever get one pod for regular jobs, and one pod for each index in indexed
    completion jobs, because we always set parallelism = completions, and we've
    configured 0 retries.

    For regular jobs, the pod created will be named <job_name>-<random string>. The
    pod's metadata has a generate_name, which will be equal to <job_name>-.

    For indexed completion jobs, there will be a pod for each index with a generate_name
    of <job_name>-<index>-.

    pod_generate_names should have the list of generate_names that we expect to see for
    the specified job.

    Returns a pod object corresponding to each pod_generate_names, in the same order as
    pod_generate_names.
    """

    pod_generate_names_set = set(pod_generate_names)
    results: Dict[str, kubernetes_client.V1Pod] = {}
    i = 0

    while True:
        pods = await core_api.list_namespaced_pod(
            kubernetes_namespace, label_selector=f"job-name={job_name}"
        )
        for pod in pods.items:
            generate_name = pod.metadata.generate_name

            if generate_name not in pod_generate_names_set:
                raise ValueError(
                    f"Unexpected pod {pod.metadata.name} with generate name "
                    f"{generate_name} found"
                )

            if (
                generate_name in results
                and results[generate_name].metadata.name != pod.metadata.name
            ):
                # TODO we may need to change this if we add e.g. retries
                raise ValueError(
                    "Unexpected multiple pods with the same generate name "
                    f"{generate_name} found: {results[generate_name].metadata.name}, "
                    f"{pod.metadata.name}"
                )

            results[generate_name] = pod

        if len(results) >= len(pod_generate_names):
            break

        if i > 15:
            raise TimeoutError(
                "Waited >15s, but pods with the following generate names were not "
                "created: "
                + ", ".join(p for p in pod_generate_names if p not in results)
            )

        if i == 0:
            print(f"Waiting for pods to be created for the job {job_name}")

        await asyncio.sleep(1.0)

        i += 1

    return [results[generate_name] for generate_name in pod_generate_names]


def get_main_container_state(
    pod: kubernetes_client.V1Pod, job_name: str
) -> Tuple[Optional[kubernetes_client.V1ContainerState], Optional[str]]:
    # first get main container state
    container_statuses = pod.status.container_statuses

    if container_statuses is None or len(container_statuses) == 0:
        main_container_state = None
    else:
        main_container_statuses = [s for s in container_statuses if s.name == "main"]
        if len(main_container_statuses) == 0:
            raise ValueError(
                f"The job {job_name} has a pod {pod.metadata.name} but there is no "
                "`main` container"
            )
        if len(main_container_statuses) > 1:
            raise ValueError(
                f"The job {job_name} has a pod {pod.metadata.name} but there is more "
                "than one `main` container"
            )
        main_container_state = main_container_statuses[0].state

    # then get the latest condition's reason, this is where Kubernetes will tell us that
    # e.g. the pod is unschedulable
    if pod.status.conditions:
        latest_condition = pod.status.conditions[-1]
        result_builder = []
        if latest_condition.reason:
            result_builder.append(latest_condition.reason)
        if latest_condition.message:
            result_builder.append(latest_condition.message)
        latest_condition_reason = ", ".join(result_builder)
    else:
        latest_condition_reason = None

    return main_container_state, latest_condition_reason


# This is how long we'll wait for a pod that is pulling an image
_WAIT_FOR_POD_PULLING_IMAGE_SECS = 60 * 7
# This is how long we'll wait for a pod to that isn't running yet and is in the
# "unschedulable" state. We want to allow some time for a pod autoscaler to kick in
_WAIT_FOR_POD_UNSCHEDULABLE_SECS = 60 * 7
# This is how long we'll wait for a pod to start running for any state other than the
# ones described above
_WAIT_FOR_POD_RUNNING_OTHER_SECS = 15


class PodState(enum.Enum):
    PULLING_IMAGE = 1
    UNSCHEDULABLE = 2
    OTHER_HAS_NOT_RUN = 3
    STARTED_RUNNING = 4


def _get_pod_state(pod: kubernetes_client.V1Pod, job_name: str) -> Tuple[PodState, str]:
    """
    Returns the internal representation of the pod state (PodState) and a string to
    present to the user about the state of the pod
    """
    main_container_state, latest_condition_reason = get_main_container_state(
        pod, job_name
    )
    if main_container_state is not None and (
        main_container_state.running is not None
        or main_container_state.terminated is not None
    ):
        return PodState.STARTED_RUNNING, ""

    pod_state = PodState.OTHER_HAS_NOT_RUN

    additional_info_builder = [":"]
    if main_container_state is not None and main_container_state.waiting is not None:
        waiting_reason = str(main_container_state.waiting.reason)
        additional_info_builder.append(waiting_reason)

        if main_container_state.waiting.message is not None:
            additional_info_builder.append(str(main_container_state.waiting.message))
        elif (
            main_container_state.waiting.reason == "ContainerCreating"
            and pod_state == PodState.OTHER_HAS_NOT_RUN
        ):
            # TODO this isn't exactly right, there's a separate Event (different API)
            # that would tell us conclusively that we're waiting for the image pull, but
            # this logic seems to be correct most of the time. We should maybe read
            # those events, but at some point we may get what we need in this field
            # waiting.reason field:
            # https://github.com/kubernetes/kubernetes/issues/19077
            pod_state = PodState.PULLING_IMAGE
            additional_info_builder.append("(pulling image)")
    if latest_condition_reason:
        additional_info_builder.append(latest_condition_reason)
        if latest_condition_reason.startswith("Unschedulable"):
            pod_state = PodState.UNSCHEDULABLE

    if len(additional_info_builder) == 1:
        additional_info = ""
    else:
        additional_info = " ".join(additional_info_builder)

    return pod_state, additional_info


async def wait_for_pod_running(
    core_api: kubernetes_client.CoreV1Api,
    job_name: str,
    kubernetes_namespace: str,
    pod: kubernetes_client.V1Pod,
) -> kubernetes_client.V1Pod:
    pod_name = pod.metadata.name

    # The first step is to wait for the pod to start running, because we can't stream
    # logs until the pod is in a running state. The happy path is that our pod is in the
    # "waiting" state because we're either waiting for the image to get pulled or we're
    # waiting for nodes to become available to run our job. In that case, we'll wait up
    # to 7 minutes.
    #
    # The unhappy path is that something has gone wrong which Kubernetes expresses as
    # waiting infinitely, rather than a failure. E.g. if our image spec is invalid. In
    # that case we'll only wait 15 seconds, as it doesn't make sense to expect that that
    # would change.

    seconds_waited = 0
    prev_additional_info = None
    prev_pod_state = PodState.OTHER_HAS_NOT_RUN
    wait_until = _WAIT_FOR_POD_RUNNING_OTHER_SECS
    pod_state, additional_info = _get_pod_state(pod, job_name)
    while pod_state != PodState.STARTED_RUNNING:
        if additional_info != prev_additional_info:
            print(f"Waiting for pod {pod_name} to start running{additional_info}")
            prev_additional_info = additional_info

        await asyncio.sleep(1.0)  # this is the polling interval
        seconds_waited += 1
        if pod_state != prev_pod_state:
            # every time we "change state", extend/shorten how much time we will wait
            # based on the new state
            prev_pod_state = prev_pod_state
            if pod_state == PodState.PULLING_IMAGE:
                wait_until = seconds_waited + _WAIT_FOR_POD_PULLING_IMAGE_SECS
            elif pod_state == PodState.UNSCHEDULABLE:
                wait_until = seconds_waited + _WAIT_FOR_POD_UNSCHEDULABLE_SECS
            else:
                wait_until = seconds_waited + _WAIT_FOR_POD_RUNNING_OTHER_SECS

        if seconds_waited > wait_until:
            raise TimeoutError(
                f"Waited >{seconds_waited} seconds for the container of job {job_name} "
                "in pod {pod_name} to start running"
            )

        pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        pod_state, additional_info = _get_pod_state(pod, job_name)

    return pod


async def stream_pod_logs(
    core_api: kubernetes_client.CoreV1Api,
    kubernetes_namespace: str,
    pod_name: str,
    job_name: str,
) -> int:
    while True:
        await _stream_pod_log_helper(core_api, kubernetes_namespace, pod_name)

        try:
            return await wait_for_pod_exit(
                core_api, job_name, kubernetes_namespace, pod_name, 15
            )
        except TimeoutError:
            # this can happen sometimes if e.g. there was a network connectivity issue
            # while streaming the logs
            print(
                "Log stream stopped, waited for the container to exit for 15 seconds, "
                "but the container is still running. Restarting the streaming of the "
                "logs"
            )


async def _stream_pod_log_helper(
    core_api: kubernetes_client.CoreV1Api, kubernetes_namespace: str, pod_name: str
) -> None:
    try:
        async with kubernetes_watch.Watch() as w:
            async for line in w.stream(
                core_api.read_namespaced_pod_log,
                name=pod_name,
                namespace=kubernetes_namespace,
            ):
                print(line, end="")
    except asyncio.CancelledError:
        raise
    except BaseException:
        print("Unable to stream logs:\n" + traceback.format_exc())


async def wait_for_pod_exit(
    core_api: kubernetes_client.CoreV1Api,
    job_name: str,
    kubernetes_namespace: str,
    pod_name: str,
    timeout_seconds: int,
) -> int:
    # Once this stream ends, we know the pod is completed, but sometimes it takes some
    # time for Kubernetes to report that the pod has completed. So we poll until the pod
    # is reported as terminated.

    pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
    main_container_state, _ = get_main_container_state(pod, job_name)
    t0 = time.time()
    while main_container_state is None or main_container_state.running is not None:
        await asyncio.sleep(1.0)
        if time.time() > t0 + timeout_seconds:
            raise TimeoutError(
                f"The job {job_name} timed out, the pod {pod_name} has been running "
                f"for {timeout_seconds} seconds"
            )
        pod = await core_api.read_namespaced_pod_status(pod_name, kubernetes_namespace)
        main_container_state, _ = get_main_container_state(pod, job_name)

    return main_container_state.terminated.exit_code


async def wait_for_pod(
    core_api: kubernetes_client.CoreV1Api,
    job_name: str,
    kubernetes_namespace: str,
    pod: kubernetes_client.V1Pod,
    wait_for_result: WaitOption,
) -> int:
    """
    This function waits for the specified pod to start running, streams the logs from
    that pod into our local stdout, and then waits for the pod to terminate. Then we
    return the exit code of the pod.
    """
    await wait_for_pod_running(core_api, job_name, kubernetes_namespace, pod)
    if wait_for_result == WaitOption.DO_NOT_WAIT:
        # TODO maybe return None instead? Currently this code path is not used, requires
        # support in the caller
        return 0

    if wait_for_result == WaitOption.WAIT_AND_TAIL_STDOUT:
        return await stream_pod_logs(
            core_api, kubernetes_namespace, pod.metadata.name, job_name
        )
    else:
        # TODO this timeout should be configurable and the default should be smaller
        # than 2 days
        wait_for_pod_exit_timeout_seconds = 60 * 60 * 24 * 2
        return await wait_for_pod_exit(
            core_api,
            job_name,
            kubernetes_namespace,
            pod.metadata.name,
            wait_for_pod_exit_timeout_seconds,
        )


async def get_kubernetes_secret(
    kubernetes_namespace: str, secret_name: str
) -> Dict[str, str]:
    async with kubernetes_client.ApiClient() as api_client:
        core_api = kubernetes_client.CoreV1Api(api_client)
        result = await core_api.read_namespaced_secret(
            secret_name, kubernetes_namespace
        )

    return {
        key: base64.b64decode(value).decode("utf-8")
        for key, value in result.data.items()
    }


async def run_command_on_pod(
    pod_name: str,
    kubernetes_namespace: str,
    command: List[str],
    ws_core_api: kubernetes_client.CoreV1Api,
) -> str:
    """Returns the output of running command"""
    return await ws_core_api.connect_post_namespaced_pod_exec(
        name=pod_name,
        namespace=kubernetes_namespace,
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )


async def run_command_on_pod_and_stream(
    pod_name: str,
    kubernetes_namespace: str,
    command: List[str],
    ws_core_api: kubernetes_client.CoreV1Api,
) -> None:
    """
    Runs the specified command and streams remote stdout and stderr to the local stdout
    """
    result = await ws_core_api.connect_post_namespaced_pod_exec(
        name=pod_name,
        namespace=kubernetes_namespace,
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    async for message in result:
        # this is pretty janky--ideally kubernetes_asyncio would provide this in their
        # API but it doesn't seem to exist
        if (
            message.type == aiohttp.WSMsgType.BINARY
            and len(message.data) > 1
            and message.data[0] == 1
        ):
            sys.stdout.buffer.write(message.data[1:])


def get_main_container_is_ready(pod: kubernetes_client.V1Pod) -> bool:
    main_container_statuses = [
        container
        for container in pod.status.container_statuses
        if container.name == "main"
    ]
    if len(main_container_statuses) == 0:
        print(f"Unexpected: pod {pod.metadata.name} has no main container, skipping")
        return False
    elif len(main_container_statuses) > 1:
        print(
            f"Unexpected: pod {pod.metadata.name} has more than one main container, "
            "skipping"
        )
        return False
    else:
        return main_container_statuses[0].ready


async def set_main_container_ready(
    core_api: kubernetes_client.CoreV1Api, pod: kubernetes_client.V1Pod, is_ready: bool
) -> None:
    main_containers = [
        container
        for container in pod.status.container_statuses
        if container.name == "main"
    ]
    if len(main_containers) != 1:
        raise ValueError(
            "Unexpected number of container with the name 'main': "
            f"{len(main_containers)}"
        )

    main_container_state = main_containers[0]
    main_container_state.ready = is_ready

    await core_api.patch_namespaced_pod_status(
        pod.metadata.name,
        pod.metadata.namespace,
        kubernetes_client.V1Pod(
            status=kubernetes_client.V1PodStatus(
                container_statuses=[main_container_state]
            )
        ),
    )
