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
    mock_backend_client.exec_capture.return_value = (0, "PATH=/usr/bin\n", "")

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


def test_start_maps_operation_error_to_invalid_argument(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
    aborted: type[Exception],
) -> None:
    """A ``BackendOperationError`` from LXD is a "the daemon said no" surface --
    typically a bad image / config / conflict -- so it maps to
    ``INVALID_ARGUMENT`` per the error-map policy, not ``INTERNAL``.
    Locking the mapping in here so a future error-map tweak surfaces this
    behaviour rather than silently changing the runner's retry decision.
    """
    mock_backend_client.get_instance_state.side_effect = BackendOperationError("daemon said no")

    with pytest.raises(aborted) as excinfo:
        _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert excinfo.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


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
    mock_backend_client.exec_capture.return_value = (0, "PATH=/usr/bin\n", "")

    _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    # Only the environment probe, run bare on the instance: no runtime
    # is looked for and nothing is started.
    commands = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert commands == [["env"]]
    context.abort.assert_not_called()


def test_start_starts_the_job_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    """Create already pulled and created it; Start only starts it."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.side_effect = [
        (0, "job-1-job", ""),  # docker start
        (0, "PATH=/usr/bin:/bin\n", ""),  # docker exec env
    ]

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    context.abort.assert_not_called()
    assert outs[0].start_complete.image_env == {"PATH": "/usr/bin:/bin"}

    # The env probe runs in the container, and only after it is started.
    commands = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert commands == [
        ["docker", "start", "job-1-job"],
        ["docker", "exec", "job-1-job", "env"],
    ]


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


def test_start_reports_the_container_environment(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered_with_container: None,  # noqa: ARG001
) -> None:
    """A busybox image bakes no ENV, yet its container still has PATH.

    Inspecting the image would hand the runner nothing and it would
    fall back to a guessed PATH.
    """
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.side_effect = [
        (0, "", ""),  # start
        (0, "PATH=/baked:/usr/bin:/bin\nHOME=/root\n", ""),  # env
    ]

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert dict(outs[0].start_complete.image_env) == {
        "PATH": "/baked:/usr/bin:/bin",
        "HOME": "/root",
    }
    probe = mock_backend_client.exec_capture.call_args_list[-1].args[1]
    assert probe == ["docker", "exec", "job-1-job", "env"]
    assert "inspect" not in probe


def test_start_reports_the_instance_environment_without_a_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    """Same question, same answer: whatever `env` reports where steps run."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.return_value = (0, "PATH=/usr/bin:/bin\nLANG=C\n", "")

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert dict(outs[0].start_complete.image_env) == {"PATH": "/usr/bin:/bin", "LANG": "C"}
    assert mock_backend_client.exec_capture.call_args.args[1] == ["env"]


def test_start_keeps_an_environment_value_containing_an_equals_sign(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.return_value = (0, "OPTS=a=1,b=2\n", "")

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert dict(outs[0].start_complete.image_env) == {"OPTS": "a=1,b=2"}


def test_start_survives_a_failed_environment_probe(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    registered: None,  # noqa: ARG001
) -> None:
    """Losing env discovery is not worth losing the job over."""
    mock_backend_client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}
    mock_backend_client.exec_capture.return_value = (1, "", "boom")

    outs = _drain(service.Start(plugin_pb2.StartRequest(environment_id="job-1"), context))

    assert outs[0].WhichOneof("Output") == "start_complete"
    assert dict(outs[0].start_complete.image_env) == {}
    context.abort.assert_not_called()
