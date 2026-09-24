"""Unit tests for ``BackendPluginService.Capabilities``."""

from __future__ import annotations

from unittest.mock import MagicMock

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def test_capabilities_returns_backend_name(
    service: BackendPluginService, context: MagicMock
) -> None:
    resp = service.Capabilities(plugin_pb2.CapabilitiesRequest(), context)
    assert resp.name == "lxd"
    context.abort.assert_not_called()
