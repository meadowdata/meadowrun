"""
This module provides functions for interacting with Docker. This uses aiodocker to
interact with the docker API (https://docs.docker.com/engine/api/), and also uses
aiohttp to make direct calls to the Docker Registry API
(https://docs.docker.com/registry/spec/api/).

We'll use terminology consistent with Docker:
- repository is e.g. "python", "percona/percona-server-mongodb",
  "gcr.io/kaniko-project/executor",
  "012345678910.dkr.ecr.us-east-2.amazonaws.com/code1". A repository has many images,
  and each image can have one or more tags.
- tag is e.g. "latest", "3.9-slim-buster" (for python), "1.21.3-amd64" (for
  public.ecr.aws/nginx) etc. A tag refers to one image at any moment, but the image it
  points to can change.
- digest is usually something like
  sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22955b46d4 and always
  refers to one image, and will always refer to a single image.
- A registry is identified by a domain, e.g. registry-1.docker.io, gcr.io,
  012345678910.dkr.ecr.us-east-2.amazonaws.com and holds one or more repositories.

Normal usage is e.g. `docker pull [repository]:[tag]` or `docker pull
[repository]:[digest]`.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
import tarfile
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Tuple, Optional, List, Dict, Iterable, Any

from meadowrun._vendor import aiodocker

if TYPE_CHECKING:
    from meadowrun._vendor.aiodocker import containers as aiodocker_containers
from meadowrun._vendor.aiodocker import exceptions as aiodocker_exceptions

import aiohttp

import meadowrun.credentials

# When no registry domain is specified, docker assumes this as the registry domain.
# I.e. `docker pull python:latest` is equivalent to `docker pull
# registry-1.docker.io/python:latest`. This is also referred to as DockerHub.
_DEFAULT_REGISTRY_DOMAIN = "registry-1.docker.io"
# With the default (DockerHub) registry, if there's no "/" in the repository name,
# docker assumes this as the prefix. I.e. `docker pull python:latest` is equivalent to
# `docker pull library/python:latest` and also to `docker pull
# registry-1.docker.io/library/python:latest`.
_DEFAULT_DOCKERHUB_REPO_PREFIX = "library"
# The Docker Registry manifests API returns different content depending on the Accept
# header, the options are described at
# https://docs.docker.com/registry/spec/manifest-v2-2/ We want to select the types in
# order that are used by the docker client to compute the digest (this does not appear
# to be documented).
# TODO investigate the docker code to confirm this is correct/complete
_MANIFEST_ACCEPT_HEADER_FOR_DIGEST = (
    "application/vnd.docker.distribution.manifest.list.v2+json, "
    "application/vnd.docker.distribution.manifest.v2+json"
)


def get_registry_domain(repository: str) -> Tuple[str, str]:
    """
    Takes a repository name and returns the registry domain name for that repository and
    the repository name to use within that registry. This is so that we can use the
    Docker Registry HTTP API, documented at https://docs.docker.com/registry/spec/api/

    See module-level docstring for examples of repository. The format for how
    repository strings are parsed does not seem to be documented other than in the
    code. This StackOverflow answer is a good guide:
    https://stackoverflow.com/questions/37861791/how-are-docker-image-names-parsed
    This function is essentially a slight modification of splitDockerDomain.

    Examples of expected input/output:
        - python -> (https://registry-1.docker.io, library/python)
        - percona/percona-server-mongodb -> (https://registry-1.docker.io,
          percona/percona-server-mongodb)
        - gcr.io/kaniko-project/executor -> (https://gcr.io, kaniko-project/executor)
        - 012345678910.dkr.ecr.us-east-2.amazonaws.com/code1 ->
          (https://012345678910.dkr.ecr.us-east-2.amazonaws.com, code1)

    Another confusing aspect is that there are multiple docker domains associated with
    DockerHub:
    - hub.docker.com has DockerHub-specific APIs. This is NOT the standard Docker
      Registry HTTP API.
    - registry-1.docker.io seems to be the canonical domain name for the Docker
      Registry HTTP API.
    - index.docker.io is an old domain name, and seems to work the same way
    - docker.io also seems to work for `docker pull`, but does not work with the
      Docker Registry HTTP API.
    Some more information at
    https://stackoverflow.com/questions/56193110/how-can-i-use-docker-registry-http-api-v2-to-obtain-a-list-of-all-repositories-i
    """
    domain, slash, remainder = repository.partition("/")
    # for docker, registry domains must have a ., a :, or be "localhost"
    if (not slash) or (
        "." not in domain and ":" not in domain and domain != "localhost"
    ):
        domain = _DEFAULT_REGISTRY_DOMAIN
        remainder = repository
    else:
        # always use the canonical DockerHub domain
        if domain in ("index.docker.io", "docker.io"):
            domain = _DEFAULT_REGISTRY_DOMAIN

    # add the library/ prefix if needed
    if domain == _DEFAULT_REGISTRY_DOMAIN and "/" not in remainder:
        remainder = f"{_DEFAULT_DOCKERHUB_REPO_PREFIX}/{remainder}"
    return domain, remainder


async def get_latest_digest_from_registry(
    repository: str,
    tag: str,
    credentials: Optional[meadowrun.credentials.RawCredentials],
) -> str:
    """
    Queries the Docker Registry HTTP API to get the current digest of the specified
    repository:tag. The output of this function should always match the output of
    `docker inspect --format='{{.RepoDigests}}' [repository]:[tag]` AFTER calling
    `docker pull [repository]:[tag]`. Example output is something like
    sha256:76eaa9e5bd357d6983a88ddc9c4545ef4ad64c50f84f081ba952c7ed08e3bdd6. Note that
    this hash is also part of the output when `docker pull` is run.

    This function gets the latest version of that hash without pulling the image first.
    The docker command line/client does not provide this capability
    (https://stackoverflow.com/questions/56178911/how-to-obtain-docker-image-digest-from-tag/56178979#56178979),
    so we have to resort to the Docker Registry HTTP API.

    Note that we could also use skopeo for this: https://github.com/containers/skopeo
    The functionality we're implementing is exactly the same as `skopeo inspect
    [repository]:[tag]` and then reading the "digest" field.
    """

    # The correct implementation is to read the manifest for a repository/tag and
    # compute the sha256 hash of the content of the manifest.
    #
    # At the time of writing this comment, the accepted answers on the first couple
    # Google results on this topic were out of date, incomplete, or incorrect:
    #
    # https://stackoverflow.com/questions/39375421/can-i-get-an-image-digest-without-downloading-the-image/39376254#39376254
    # https://stackoverflow.com/questions/41808763/how-to-determine-the-docker-image-id-for-a-tag-via-docker-hub-api/41830007#41830007
    # https://ops.tips/blog/inspecting-docker-image-without-pull/
    #
    # Part of the confusion seems to be that there are many different digests for the
    # sub-parts of the image (e.g. for different architectures, different layers,
    # etc.). We only care about the digest that we get from `docker inspect` (as
    # described above) because this is what we'll use to figure out if we have the
    # latest version of the image or not. Correctness can be easily verified using the
    # docker command line as described above.
    #
    # Reading the docker-content-digest header in the response is an alternative to
    # computing the hash ourselves, but this header is not in the responses from AWS
    # ECR.

    registry_domain, repository = get_registry_domain(repository)

    manifests_url = f"https://{registry_domain}/v2/{repository}/manifests/{tag}"
    headers = {"Accept": _MANIFEST_ACCEPT_HEADER_FOR_DIGEST}

    if credentials is None:
        basic_auth = None
    elif isinstance(credentials, meadowrun.credentials.UsernamePassword):
        basic_auth = aiohttp.BasicAuth(credentials.username, credentials.password)
    else:
        raise ValueError(f"Unexpected type of credentials {type(credentials)}")

    manifests_text: Optional[bytes] = None

    # First, try requesting the manifest without any authentication. It might work, and
    # if it doesn't, the response will tell us how the authentication should work.
    # TODO add logic to "remember" which repositories require what kinds of
    #  authentication, as well as potentially tokens as well
    async with aiohttp.request("GET", manifests_url, headers=headers) as response:
        if response.ok:
            manifests_text = await response.read()
        else:
            # Regardless of the type of error, try again with authentication as long as
            # we have a www-authenticate header. response.headers is case insensitive:
            if "www-authenticate" not in response.headers:
                # we don't know how to authenticate, so just propagate the error
                response.raise_for_status()

            authentication_header = response.headers["www-authenticate"]
            authentication_header_error_message = (
                "Don't know how to interpret authentication header "
                + authentication_header
            )
            scheme, space, auth_params = authentication_header.partition(" ")
            if not space:
                raise ValueError(authentication_header_error_message)
            elif scheme.lower() == "basic":
                if basic_auth is None:
                    raise ValueError(
                        f"Basic auth is required to access {manifests_url} but no "
                        "username/password was provided"
                    )
                # We've already set the basic_auth variable above, so we'll leave it as
                # is so that it gets used directly in the next request for the manifest
                # below.
            elif scheme.lower() == "bearer":
                # For bearer auth, we need to request a token. Parsing the
                # www-authenticate header should tell us everything we need to know to
                # construct the request for the token. Example of a www-authenticate
                # header is
                # `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/python:pull"` # noqa: E501
                auth_params_parsed = urllib.request.parse_keqv_list(
                    urllib.request.parse_http_list(auth_params)
                )

                # a bit hacky, but we're going to use auth_params to create the query
                # string, so we remove realm from it because that's the only one we
                # don't need
                # TODO should this be case insensitive?
                realm = auth_params_parsed["realm"]
                del auth_params_parsed["realm"]
                token_request_url = (
                    f"{realm}?{urllib.parse.urlencode(auth_params_parsed)}"
                )

                # Even if no username_password was provided (i.e. basic_auth is None)
                # it's worth trying this. E.g. DockerHub requires an anonymous token for
                # public repositories
                async with aiohttp.request(
                    "GET", token_request_url, auth=basic_auth
                ) as token_response:
                    if not token_response.ok:
                        token_response.raise_for_status()

                    # TODO should this be case insensitive?
                    token = (await token_response.json())["token"]

                    # Now we add the Bearer token to headers which will get used in the
                    # next request for the manifest. We also need to unset basic_auth as
                    # we've used that to get the token, and it should not be used in
                    # subsequent requests.
                    headers["Authorization"] = f"Bearer {token}"
                    basic_auth = None
            else:
                raise ValueError(authentication_header_error_message)

            # now exactly one of basic_auth or headers["Authorization"] should be set
            async with aiohttp.request(
                "GET", manifests_url, headers=headers, auth=basic_auth
            ) as response_authenticated:
                if not response_authenticated.ok:
                    response_authenticated.raise_for_status()

                manifests_text = await response_authenticated.read()

    if not manifests_text:
        raise ValueError(
            "Programming error: manifests_text should not be None/empty string"
        )

    # compute the digest from the manifest text
    digest = hashlib.sha256(manifests_text).hexdigest()
    return f"sha256:{digest}"

    # TODO test case where image doesn't exist


async def _does_digest_exist_locally(image: str) -> bool:
    # This seems to be the best option for asking if an image with the specified image
    # exists locally--call inspect and then see whether we get a 404 or not. We assume
    # that calling list would be less efficient when there are many images.
    try:
        async with aiodocker.Docker() as client:
            await client.images.inspect(image)
        return True
    except aiodocker.DockerError as e:
        if e.status == 404:
            return False
        raise


async def pull_image(
    image: str, credentials: Optional[meadowrun.credentials.RawCredentials]
) -> None:
    """
    Pulls the requested image. Relatively thin wrapper around
    aiodocker.Docker.images.pull
    """

    # Not clear why, but it's much faster to do this check--you would think that docker
    # does or should do this when run/pull is called, but it doesn't seem to. If
    # client.images.pull on an image that already exists becomes significantly faster in
    # the future, we could remove this entire if block
    if not await _does_digest_exist_locally(image):

        # construct the auth parameter as aiodocker expects it
        if credentials is None:
            auth = None
        elif isinstance(credentials, meadowrun.credentials.UsernamePassword):
            domain, _ = get_registry_domain(image)
            auth = {
                "username": credentials.username,
                "password": credentials.password,
                "serveraddress": domain,
            }
        else:
            raise ValueError(f"Unexpected type of credentials {type(credentials)}")

        # pull the image
        print(f"Pulling docker image {image}")
        async with aiodocker.Docker() as client:
            statuses = await client.images.pull(image, auth=auth)

        # double check just in case that the digest is actually there
        if not await _does_digest_exist_locally(image):
            raise ValueError(
                f"Tried to pull {image} but was not successful. "
                "Docker status messages: " + "\n".join(statuses)
            )
        else:
            print(f"Successfully pulled docker image {image}")


async def build_image(
    files: Iterable[Tuple[str, str]], tag: str, buildargs: Dict[str, str]
) -> None:
    """
    Builds a docker image locally. files is an iterable of (local file path, path in
    docker workspace). buildargs is a docker concept. The resulting image will be tagged
    with tag.
    """
    with io.BytesIO() as tar_file_bytes:
        with tarfile.TarFile(fileobj=tar_file_bytes, mode="w") as tar_file:
            for local_path, archive_path in files:
                tar_file.add(local_path, archive_path)

            tar_file_bytes.seek(0)

            async with aiodocker.Docker() as client:
                async for line in client.images.build(
                    fileobj=tar_file_bytes,
                    tag=tag,
                    buildargs=buildargs,
                    # aiodocker requires that we provide an encoding, but it doesn't
                    # seem to actually be necessary
                    encoding="none",
                    stream=True,
                ):
                    if "stream" in line:
                        print(line["stream"], end="")
                    if "errorDetail" in line:
                        error_detail = line["errorDetail"]
                        try:
                            error_detail_json = json.loads(error_detail)
                            code = error_detail_json.get("code")
                            if code == 137:
                                raise ValueError(
                                    "Error building docker image: ran out of memory, "
                                    "please provision an instance that has more memory"
                                )
                        except Exception:
                            pass
                        raise ValueError(
                            f"Error building docker image: f{error_detail}"
                        )


async def push_image(
    image: str, credentials: Optional[meadowrun.credentials.RawCredentials]
) -> None:
    """Equivalent to calling docker push [image]"""
    async with aiodocker.Docker() as client:
        # construct the auth parameter as aiodocker expects it
        if credentials is None:
            auth = None
        elif isinstance(credentials, meadowrun.credentials.UsernamePassword):
            domain, _ = get_registry_domain(image)
            auth = {
                "username": credentials.username,
                "password": credentials.password,
                "serveraddress": domain,
            }
        else:
            raise ValueError(f"Unexpected type of credentials {type(credentials)}")

        prev_status = None
        async for line in client.images.push(image, auth=auth, stream=True):
            if "status" in line:
                if line["status"] != prev_status:
                    print(line["status"])
                    prev_status = line["status"]
            if "errorDetail" in line:
                raise ValueError(f"Error building docker image: f{line['errorDetail']}")


def expand_ports(ports: Iterable[str]) -> Iterable[str]:
    for port in ports:
        if "-" in port:
            split = port.split("-")
            if len(split) != 2:
                raise ValueError(f"Bad port specification {port}")
            for i in range(int(split[0]), int(split[1]) + 1):
                yield str(i)
        else:
            yield port


async def run_container(
    client: Optional[aiodocker.Docker],
    image: str,
    cmd: Optional[List[str]],
    environment_variables: Dict[str, str],
    working_directory: Optional[str],
    binds: List[Tuple[str, str]],
    ports: List[str],
    extra_hosts: List[Tuple[str, str]],
    all_gpus: bool,
) -> Tuple[aiodocker_containers.DockerContainer, aiodocker.Docker]:
    """
    Runs a docker container. Examples of parameters:
    - image: python:latest
    - cmd: ["python", "/path/to/test.py"]
    - environment_variables: {"PYTHONHASHSEED": 25}
    - binds: [("/path/on/host1", "/path/in/container1"), ...]

    Returns when the container has successfully been launched. Returns a DockerContainer
    which has a wait method for waiting until the container completes, and a Docker
    client which needs to be closed.

    Usage example:

    container, client = await run(...)
    try:
        # the container has been launched
        await container.wait()
        # now the container has finished running
    finally:
        await client.__aexit__(None, None, None)
    """

    if client is None:
        client = await aiodocker.Docker().__aenter__()

    # Now actually run the container. For documentation on the config object:
    # https://docs.docker.com/engine/api/v1.41/#operation/ContainerCreate

    config: Dict[str, Any] = {
        "Image": image,
        "Env": [f"{key}={value}" for key, value in environment_variables.items()],
        "HostConfig": {
            "Binds": [
                f"{path_on_host}:{path_in_container}"
                for path_on_host, path_in_container in binds
            ],
            # Docker for Windows and Mac automatically enable containers to access the
            # host with host.docker.internal, but we need to add this flag for Linux
            # machines to use this (not supported in Linux at all before Docker v20.10).
            # This is mostly for tests, so we include it and it shouldn't cause problems
            # even if it doesn't work. See more at agent._prepare_py_grid's discussion
            # of replacing localhost for the coordinator address. Also:
            # https://stackoverflow.com/questions/31324981/how-to-access-host-port-from-docker-container/43541732#43541732
            "ExtraHosts": ["host.docker.internal:host-gateway"]
            + [f"{host_name}:{ip}" for host_name, ip in extra_hosts],
            # Ideally we would have some sort of "AutoRemove after 5 minutes". With
            # AutoRemove turned on, containers can get deleted before we are able to
            # read off their exit codes.
            # "AutoRemove": True,
        },
    }

    if cmd is not None:
        config["Cmd"] = cmd

    if working_directory is not None:
        config["WorkingDir"] = working_directory

    # unfortunately PortBindings doesn't support port ranges so we expand them manually
    # here
    expanded_ports = list(expand_ports(ports))

    if expanded_ports:
        config["HostConfig"]["PortBindings"] = {
            f"{port}/tcp": [{"HostPort": port}] for port in expanded_ports
        }
        config["ExposedPorts"] = {f"{port}/tcp": {} for port in expanded_ports}

    if all_gpus:
        # it would be convenient if we could just always set this, but setting this when
        # there are no GPUs will cause the container creation to fail
        config["HostConfig"]["DeviceRequests"] = [
            {"Count": -1, "Capabilities": [["gpu"]]}
        ]

    container = await client.containers.run(config)

    return container, client


async def remove_container(container: aiodocker_containers.DockerContainer) -> None:
    # https://docs.docker.com/engine/api/v1.41/#tag/Container/operation/ContainerDelete
    try:
        await container.delete(v=True, force=True)
    except aiodocker_exceptions.DockerError as e:
        # 404 means the container doesn't exist, which is fine, as we're trying to
        # delete it anyways
        if e.status != 404:
            raise


async def get_image_environment_variables_and_working_dir(
    image: str,
) -> Tuple[Optional[List[str]], str]:
    """
    Returns environment variables and working directory as set in the specified image.
    The image must be available locally. Environment variables will be a list of strings
    like ["PATH=/foo/bar", "PYTHON_VERSION=3.9.7"]. working_dir will be a string.
    """
    async with aiodocker.Docker() as client:
        container_config = (await client.images.inspect(image))["ContainerConfig"]
        return container_config["Env"], container_config["WorkingDir"]


async def delete_image(image: str) -> None:
    """Deletes the specified image, used for testing"""
    try:
        async with aiodocker.Docker() as client:
            await client.images.delete(image, force=True)
    except aiodocker.DockerError as e:
        # ignore failures saying the image doesn't exist
        if e.status != 404:
            raise


async def delete_images_from_repository(repository: str) -> None:
    """
    Deletes all images repository@<digest> or a tag repository:<tag>. Warning: deletes
    images even if there are other labels pointing to those images. Used for testing.
    """
    async with aiodocker.Docker() as client:
        delete_tasks = []
        for image in await client.images.list():
            if (
                image["RepoDigests"]
                and any(
                    digest.startswith(f"{repository}@")
                    for digest in image["RepoDigests"]
                )
            ) or (
                image["RepoTags"]
                and any(tag.startswith(f"{repository}:") for tag in image["RepoTags"])
            ):
                image_id = image["Id"]
                print(f"Will delete image id: {image_id}")
                delete_tasks.append(delete_image(image_id))

        if delete_tasks:
            await asyncio.wait(delete_tasks)
