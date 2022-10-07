from __future__ import annotations

import enum


class EnvironmentSpecPrerequisites(enum.Flag):
    NONE = 0
    GIT = enum.auto()
    GOOGLE_AUTH = enum.auto()
    # AZURE_AUTH = enum.auto()  # not implemented yet


GOOGLE_AUTH_PACKAGE = "keyrings.google-artifactregistry-auth"
