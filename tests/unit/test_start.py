"""Unit tests for ``BackendPluginService.Start``."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import httpx
import pytest

from forgejo_lxd_runner.client import BackendOperationError
from forgejo_lxd_runner.executor import ContainerExecutor, HostExecutor
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env

_STATUS_RUNNING = 103
_STATUS_STOPPED = 102


@pytest.fixture
def registered(service: BackendPluginService, mock_backend_client: MagicMock) -> None:
    """Register ``job-1`` in the service's env map so ``_lookup`` succeeds."""
    service._envs["job-1"] = _Env(
        instance_name="job-1",
        executor=HostExecutor(client=mock_backend_client, instance="job-1"),
    )  # noqa: SLF001


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


# ---------------------------------------------------------------------------
# Job containers — `jobs.<id>.container.image`


@pytest.fixture
def registered_with_container(
    service: BackendPluginService, mock_backend_client: MagicMock
) -> None:
    """Register the env Create leaves behind for a job with a container.

    Create resolves the executor and creates the container, so by Start
    the executor is a ContainerExecutor with its container named.
    """
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=ContainerExecutor(
            client=mock_backend_client,
            instance="job-1",
            image="node:20",
            runtime="docker",
            container="job-1-job",
        ),
        job_image="node:20",
    )


def test_start_without_an_image_never_probes_for_a_runtime(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    """The common case must not pay for a feature it does not use."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}

    _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    mock_backend_client.exec_capture.assert_not_called()
    context.abort.assert_not_called()


def test_start_starts_the_job_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    """Create already pulled and created it; Start only starts it."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.return_value = (0, "job-1-job", "")

    _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    context.abort.assert_not_called()

    commands = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert commands == [["docker", "start", "job-1-job"]]


def test_start_reports_a_container_that_will_not_start(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    """The image already pulled, so a failure here is not workflow input."""
    context.abort.side_effect = RuntimeError("aborted")
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.return_value = (1, "", "no such container")

    with pytest.raises(RuntimeError, match="aborted"):
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert context.abort.call_args.args[0] == grpc.StatusCode.INTERNAL
