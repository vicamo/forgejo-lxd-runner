"""Unit tests for the Create timeout wiring.

Covers ``environment_timeout`` from the runner, ``max_environment_timeout``
from the plugin config, their interaction, and the DEADLINE_EXCEEDED path
that best-effort tears down the partial instance and its network.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf.duration_pb2 import Duration

from forgejo_lxd_runner.client import BackendOperationTimeout
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu:24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def _duration(seconds: float) -> Duration:
    d = Duration()
    d.FromNanoseconds(int(seconds * 1e9))
    return d


# ---------------------------------------------------------------------
# effective timeout combination
# ---------------------------------------------------------------------


@pytest.mark.parametrize(
    ("runner_seconds", "plugin_cap", "expected"),
    [
        (0, None, None),  # neither set -> wait forever
        (30, None, 30.0),  # only runner
        (0, 60.0, 60.0),  # only plugin cap
        (30, 60.0, 30.0),  # runner tighter
        (120, 60.0, 60.0),  # plugin cap tighter
        (0, 0.0, None),  # explicit zero cap disables
    ],
)
def test_effective_create_timeout(
    runner_seconds: int, plugin_cap: float | None, expected: float | None
) -> None:
    service = BackendPluginService(max_environment_timeout=plugin_cap)
    kw: dict[str, object] = {}
    if runner_seconds:
        kw["environment_timeout"] = _duration(runner_seconds)
    got = service._effective_create_timeout(_req(**kw))  # noqa: SLF001
    assert got == expected


# ---------------------------------------------------------------------
# Create integration: timeout aborts with DEADLINE_EXCEEDED
# ---------------------------------------------------------------------


def test_create_times_out_and_cleans_up(
    context: MagicMock,
    mock_backend_client: MagicMock,
    aborted: type[Exception],
) -> None:
    service = BackendPluginService(max_environment_timeout=0.05)
    mock_backend_client.launch_instance.side_effect = BackendOperationTimeout(0.05)

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)

    assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[attr-defined]
    # Best-effort cleanup removes the partial instance and its bridge.
    mock_backend_client.remove_instance.assert_called_once_with("job-1", project=None)
    mock_backend_client.remove_network.assert_called_once()
    assert "job-1" not in service._envs  # noqa: SLF001


def test_create_passes_the_effective_timeout_to_launch(
    context: MagicMock,
    mock_backend_client: MagicMock,
    aborted: type[Exception],
) -> None:
    """The runner's tighter deadline is what launch_instance waits on."""
    service = BackendPluginService(max_environment_timeout=10.0)
    mock_backend_client.launch_instance.side_effect = BackendOperationTimeout(0.05)

    with pytest.raises(aborted) as exc:
        service.Create(_req(environment_timeout=_duration(0.05)), context)

    assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[attr-defined]
    launch_kwargs = mock_backend_client.launch_instance.call_args_list[0].kwargs
    assert launch_kwargs["timeout"] == pytest.approx(0.05)


def test_create_without_any_timeout_waits_forever(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """No runner deadline and no plugin cap -> launch_instance(timeout=None)."""
    service.Create(_req(), context)

    launch_kwargs = mock_backend_client.launch_instance.call_args_list[0].kwargs
    assert launch_kwargs["timeout"] is None
