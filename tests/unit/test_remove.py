"""Unit tests for ``BackendPluginService.Remove``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pylxd.exceptions import NotFound

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env

_STATUS_RUNNING = 103
_STATUS_STOPPED = 102


@pytest.fixture
def registered(service: BackendPluginService) -> None:
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001


def test_remove_unknown_env_is_noop(
    service: BackendPluginService, context: MagicMock, mock_pylxd_client: MagicMock
) -> None:
    service.Remove(plugin_pb2.RemoveRequest(environment_id="ghost"), context)
    mock_pylxd_client.instances.get.assert_not_called()
    context.abort.assert_not_called()


def test_remove_missing_lxd_instance_is_noop(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    registered: None,
) -> None:
    mock_pylxd_client.instances.get.side_effect = NotFound(MagicMock())
    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)
    context.abort.assert_not_called()
    # The env is dropped from the map even when LXD has already collected it.
    assert "job-1" not in service._envs  # noqa: SLF001


def test_remove_stops_running_instance_then_deletes(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    registered: None,
) -> None:
    inst = MagicMock(status_code=_STATUS_RUNNING)
    mock_pylxd_client.instances.get.return_value = inst

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    inst.stop.assert_called_once_with(force=True, wait=True)
    inst.delete.assert_called_once_with(wait=True)


def test_remove_skips_stop_when_already_stopped(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    registered: None,
) -> None:
    inst = MagicMock(status_code=_STATUS_STOPPED)
    mock_pylxd_client.instances.get.return_value = inst

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    inst.stop.assert_not_called()
    inst.delete.assert_called_once_with(wait=True)
