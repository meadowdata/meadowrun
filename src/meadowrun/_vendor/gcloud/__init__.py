"""
Copied from https://github.com/talkiq/gcloud-aio See sub-packages for version
information
"""

try:
    import pkg_resources

    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil

    __path__ = pkgutil.extend_path(__path__, __name__)  # type: ignore[has-type]
