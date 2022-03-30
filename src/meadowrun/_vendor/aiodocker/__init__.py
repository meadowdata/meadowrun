# Copied from https://pypi.org/project/aiodocker/0.21.0/
# Requirements are python >= 3.6 aiohttp >= 3.6, typing-extensions >= 3.6.5
# There's currently no conda package for this so we vendorize it

from .docker import Docker
from .exceptions import DockerContainerError, DockerError


__version__ = "0.21.0"


__all__ = ("Docker", "DockerError", "DockerContainerError")
