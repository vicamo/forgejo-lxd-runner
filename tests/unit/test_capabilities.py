"""Unit tests for ``BackendPluginService.Capabilities``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


@pytest.mark.parametrize("name", ["lxd", "incus", "runner-a", "prod-lxd"])
def test_capabilities_echoes_configured_name(context: MagicMock, name: str) -> None:
    """The name advertised over Capabilities is whatever ``--name`` supplied.

    Runner labels reference the plugin via ``<label>:<name>://<arg>``, so the
    Capabilities response must echo the exact name the operator configured —
    not the ``lxd`` default baked into the class.
    """
    service = BackendPluginService(name=name)
    resp = service.Capabilities(plugin_pb2.CapabilitiesRequest(), context)
    assert resp.name == name
    context.abort.assert_not_called()


def test_capabilities_default_name_is_default(
    service: BackendPluginService, context: MagicMock
) -> None:
    resp = service.Capabilities(plugin_pb2.CapabilitiesRequest(), context)
    assert resp.name == BackendPluginService.DEFAULT_NAME
    context.abort.assert_not_called()
