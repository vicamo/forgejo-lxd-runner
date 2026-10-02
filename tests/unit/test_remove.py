"""Unit tests for ``BackendPluginService.Remove``.

Remove's job is to look up the environment record, delegate the daemon
dance to ``BackendClient.remove_instance``, and translate the two
error classes we care about (``httpx.HTTPError``, ``BackendOperationError``)
into ``INTERNAL``. The state-probe / stop / delete / 404-tolerance logic
lives in the client — see ``test_client_remove_instance.py``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import httpx
import pytest

from forgejo_lxd_runner.client import BackendOperationError
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env


@pytest.fixture
def registered(service: BackendPluginService) -> None:
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001


def test_remove_unknown_env_is_noop(
    service: BackendPluginService, context: MagicMock, mock_backend_client: MagicMock
) -> None:
    service.Remove(plugin_pb2.RemoveRequest(environment_id="ghost"), context)
    mock_backend_client.remove_instance.assert_not_called()
    context.abort.assert_not_called()


def test_remove_delegates_to_client(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    mock_backend_client.remove_instance.assert_called_once_with("job-1")
    context.abort.assert_not_called()
    assert "job-1" not in service._envs  # noqa: SLF001


def test_remove_env_is_dropped_before_client_call(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
) -> None:
    """The ``_envs`` entry must be popped even if the daemon call raises,
    otherwise a stuck env pins the map forever."""
    mock_backend_client.remove_instance.side_effect = httpx.ConnectError("boom")

    with pytest.raises(aborted):
        service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    assert "job-1" not in service._envs  # noqa: SLF001


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("boom"),
        BackendOperationError("stop failed"),
    ],
)
def test_remove_maps_client_errors_to_internal(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
    exc: Exception,
) -> None:
    mock_backend_client.remove_instance.side_effect = exc

    with pytest.raises(aborted) as excinfo:
        service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    assert excinfo.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
