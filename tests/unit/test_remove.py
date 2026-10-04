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
from forgejo_lxd_runner.executor import ContainerExecutor, HostExecutor, ServiceSet
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _Env


@pytest.fixture
def registered(service: BackendPluginService, mock_backend_client: MagicMock) -> None:
    service._envs["job-1"] = _Env(
        instance_name="job-1",
        executor=HostExecutor(client=mock_backend_client, instance="job-1"),
    )  # noqa: SLF001


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
    ("exc", "grpc_code"),
    [
        (httpx.ConnectError("boom"), grpc.StatusCode.INTERNAL),
        (BackendOperationError("stop failed"), grpc.StatusCode.INVALID_ARGUMENT),
    ],
)
def test_remove_maps_client_errors_to_grpc_status(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
    exc: Exception,
    grpc_code: grpc.StatusCode,
) -> None:
    mock_backend_client.remove_instance.side_effect = exc

    with pytest.raises(aborted) as excinfo:
        service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    assert excinfo.value.code == grpc_code  # type: ignore[attr-defined]


@pytest.fixture
def registered_with_container(
    service: BackendPluginService, mock_backend_client: MagicMock
) -> None:
    """The env Create leaves behind for a job that asked for an image."""
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=ContainerExecutor(
            client=mock_backend_client,
            instance="job-1",
            image="node:20",
            runtime="docker",
            container="job-1-job",
        ),
    )


def test_remove_tears_the_container_down_before_the_instance(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    calls: list[str] = []
    mock_backend_client.exec_capture.side_effect = lambda *a, **k: (
        calls.append("container"),
        (0, "", ""),
    )[1]
    mock_backend_client.remove_instance.side_effect = lambda *a, **k: calls.append("instance")

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    assert calls == ["container", "instance"]
    assert mock_backend_client.exec_capture.call_args.args[1] == [
        "docker",
        "rm",
        "--force",
        "job-1-job",
    ]
    context.abort.assert_not_called()


def test_remove_deletes_the_instance_when_the_container_will_not_die(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    """The instance is the real resource; a stuck container must not keep it."""
    mock_backend_client.exec_capture.side_effect = httpx.ConnectError("boom")

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    mock_backend_client.remove_instance.assert_called_once_with("job-1")
    context.abort.assert_not_called()


def test_remove_without_a_container_touches_only_the_instance(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    mock_backend_client.exec_capture.assert_not_called()
    mock_backend_client.remove_instance.assert_called_once_with("job-1")


def test_remove_deletes_the_network_after_the_instance(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The instance references the network, so it must go first."""
    calls: list[str] = []
    mock_backend_client.remove_instance.side_effect = lambda *a, **k: calls.append("instance")
    mock_backend_client.remove_network.side_effect = lambda *a, **k: calls.append("network")
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=HostExecutor(client=mock_backend_client, instance="job-1"),
        network="flr-abc123",
    )

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    assert calls == ["instance", "network"]
    mock_backend_client.remove_network.assert_called_once_with("flr-abc123")
    context.abort.assert_not_called()


def test_remove_without_a_network_skips_the_network_call(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    """An env that never got a network (empty name) deletes nothing extra."""
    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    mock_backend_client.remove_network.assert_not_called()


def test_remove_survives_a_network_delete_failure(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The instance is already gone, so a stuck network must not fail Remove."""
    mock_backend_client.remove_network.side_effect = httpx.ConnectError("boom")
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=HostExecutor(client=mock_backend_client, instance="job-1"),
        network="flr-abc123",
    )

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    context.abort.assert_not_called()


def test_remove_tears_services_down_after_the_job_container(
    service: BackendPluginService, context: MagicMock, mock_backend_client: MagicMock
) -> None:
    """The job container has to leave the network before the network can go."""
    mock_backend_client.exec_capture.return_value = (0, "", "")
    services = ServiceSet(
        client=mock_backend_client,
        instance="job-1",
        runtime="docker",
        network="job-1",
    )
    services.containers.append("job-1-redis")
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=ContainerExecutor(
            client=mock_backend_client,
            instance="job-1",
            image="alpine",
            runtime="docker",
            container="job-1-job",
        ),
        services=services,
    )

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    calls = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    job_rm = calls.index(["docker", "rm", "--force", "job-1-job"])
    svc_rm = calls.index(["docker", "rm", "--force", "job-1-redis"])
    net_rm = calls.index(["docker", "network", "rm", "job-1"])
    assert job_rm < svc_rm < net_rm
    mock_backend_client.remove_instance.assert_called_once_with("job-1")


def test_remove_without_services_touches_no_network(
    service: BackendPluginService, context: MagicMock, mock_backend_client: MagicMock
) -> None:
    mock_backend_client.exec_capture.return_value = (0, "", "")
    service._envs["job-1"] = _Env(  # noqa: SLF001
        instance_name="job-1",
        executor=HostExecutor(client=mock_backend_client, instance="job-1"),
    )

    service.Remove(plugin_pb2.RemoveRequest(environment_id="job-1"), context)

    calls = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert not any("network" in c for c in calls)
