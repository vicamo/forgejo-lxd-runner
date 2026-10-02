"""Unit tests for ``BackendPluginService.Start``."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import httpx
import pytest

from forgejo_lxd_runner.client import BackendOperationError
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env

_STATUS_RUNNING = 103
_STATUS_STOPPED = 102


@pytest.fixture
def registered(service: BackendPluginService) -> None:
    """Register ``job-1`` in the service's env map so ``_lookup`` succeeds."""
    service._envs["job-1"] = _Env(instance_name="job-1")  # noqa: SLF001


def _drain(stream: object) -> list[plugin_pb2.StartOutput]:
    return list(stream)  # type: ignore[arg-type]


def test_start_accepts_running_instance(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    """The happy path: Create left the instance running, Start confirms it."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    mock_backend_client.get_instance_state.assert_called_once_with("job-1")
    assert len(outs) == 1
    assert outs[0].WhichOneof("Output") == "start_complete"
    context.abort.assert_not_called()


def test_start_aborts_when_instance_not_running(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
) -> None:
    """A stopped instance is a broken precondition, not something to fix.

    Create launches with ``start: true``; an instance that is not running
    by the time Start arrives means something outside the plugin stopped
    it. Bringing it back up would paper over that, and the job would run
    against an environment whose state nobody can account for.
    """
    mock_backend_client.get_instance_state.return_value = {
        "status_code": _STATUS_STOPPED,
        "status": "Stopped",
    }

    with pytest.raises(aborted) as excinfo:
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert excinfo.value.code == grpc.StatusCode.FAILED_PRECONDITION  # type: ignore[attr-defined]


def test_start_unknown_environment_id_aborts_not_found(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    aborted: type[Exception],
) -> None:
    with pytest.raises(aborted) as excinfo:
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="ghost"), context))

    assert excinfo.value.code == grpc.StatusCode.NOT_FOUND  # type: ignore[attr-defined]
    mock_backend_client.get_instance_state.assert_not_called()


def test_start_maps_http_error_to_internal(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
) -> None:
    mock_backend_client.get_instance_state.side_effect = httpx.ConnectError("boom")

    with pytest.raises(aborted) as excinfo:
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert excinfo.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]


def test_start_maps_operation_error_to_internal(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
) -> None:
    """A daemon-side failure reading the state is an internal error."""
    mock_backend_client.get_instance_state.side_effect = BackendOperationError("daemon is unwell")

    with pytest.raises(aborted) as excinfo:
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert excinfo.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
