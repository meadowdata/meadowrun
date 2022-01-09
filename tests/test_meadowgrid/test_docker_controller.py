# type: ignore
"""
These functions are hard to test because they need to test authentication and interact
with arbitrary Docker Registry HTTP API servers that may have slightly different
implementations. We would need to fully mock a Docker Registry HTTP API in order to test
these functions, which we have not done.

As a result, these tests are no-ops, and are manual test instructions rather than
automated tests.
"""

import asyncio

import aiodocker

from meadowgrid.docker_controller import (
    get_latest_digest_from_registry,
    _does_digest_exist_locally,
    pull_image,
)


def manual_test_get_latest_digest():
    async def run():
        # this should raise a 401 unauthorized
        print(
            await get_latest_digest_from_registry(
                "ulldhrajstopidoiklud/hopefully_this_will_never_exist", "latest", None
            )
        )

        # The results of these should all be compared against the output of skopeo
        # inspect [repository]:[tag] OR docker inspect [repository]:[tag] or docker
        # images --digests AFTER calling docker pull [repository]:[tag]

        # public, no auth at all
        print(
            await get_latest_digest_from_registry(
                "gcr.io/kaniko-project/executor", "latest", None
            )
        )
        # public, requires anonymous token
        print(await get_latest_digest_from_registry("python", "3.9-slim-buster", None))
        # private, requires token. Also, doesn't have a manifest list. Requires
        # uploading an image to DockerHub called test1
        print(
            await get_latest_digest_from_registry(
                "[your username]/test1",
                "latest",
                ("[your username]", "[your password]"),
            )
        )
        # private, requires basic auth. Requires uploading an image to AWS ECR called
        # test1
        print(
            await get_latest_digest_from_registry(
                "012345678910.dkr.ecr.us-east-2.amazonaws.com/test1",
                "latest",
                ("AWS", "`aws ecr get-login-password --region [region]`"),
            )
        )

    asyncio.run(run())


def manual_test_does_digest_exist_locally():
    async def run():
        async with aiodocker.Docker() as client:
            # test a digest that exists
            print(
                await _does_digest_exist_locally(
                    client,
                    "python",
                    "sha256:76eaa9e5bd357d6983a88ddc9c4545ef4ad64c50f84f081ba952c7ed08e3bdd6",  # noqa: E501
                )
            )
            # and a digest that does not exist for a repository that does exist
            print(
                await _does_digest_exist_locally(
                    client,
                    "python",
                    "sha256:4157d139faf3ec4c3742c2980d3fd3675608dbf75384a756f8dc0e825e54d492",  # noqa: E501
                )
            )
            # test a digest that is no longer associated with any tags
            print(
                await _does_digest_exist_locally(
                    client,
                    "gcr.io/kaniko-project/executor",
                    "sha256:8504bde9a9a8c9c4e9a4fe659703d265697a36ff13607b7669a4caa4407baa52",  # noqa: E501
                )
            )

    asyncio.run(run())


def manual_test_pull_digest():
    async def run():
        # digest that exists, no authentication required
        await pull_image(
            "python@sha256:76eaa9e5bd357d6983a88ddc9c4545ef4ad64c50f84f081ba952c7ed08e3bdd6",  # noqa: E501
            None,
        )
        # digest that does not exist
        await pull_image("python@aklsdjlaskd", None)
        # get an old version that is no longer associated with any tags
        await pull_image(
            "gcr.io/kaniko-project/executor@sha256:8504bde9a9a8c9c4e9a4fe659703d265697a36ff13607b7669a4caa4407baa52",  # noqa: E501
            None,
        )
        # with authentication at DockerHub
        await pull_image(
            "[your username]/test1@sha256:4157d139faf3ec4c3742c2980d3fd3675608dbf75384a756f8dc0e825e54d492",  # noqa: E501
            ("[your username]", "[your password]"),
        )
        # with authentication at AWS ECR
        await pull_image(
            "012345678910.dkr.ecr.us-east-2.amazonaws.com/test1@sha256:9c5098aa89084bfe4a22c690e045c34b03618e6560a890872fae2305a6f49da3",  # noqa: E501
            ("AWS", "`aws ecr get-login-password --region [region]`"),
        )

    asyncio.run(run())
