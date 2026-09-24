"""Shared pytest fixtures."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from forgejo_lxd_runner.server import BackendPluginService


@pytest.fixture
def service() -> BackendPluginService:
    """A fresh service instance."""
    return BackendPluginService()


@pytest.fixture
def context() -> MagicMock:
    """A gRPC servicer context stub."""
    return MagicMock(spec=grpc.ServicerContext)
