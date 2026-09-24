"""Unit tests for the Create timeout wiring.

Covers ``environment_timeout`` from the runner, ``max_environment_timeout``
from the plugin config, their interaction, and the DEADLINE_EXCEEDED path
that best-effort deletes the partial instance.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf.duration_pb2 import Duration
from pylxd.exceptions import NotFound

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import (
    BackendPluginService,
    _run_with_timeout,
    _TimedOut,
)


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu/24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def _duration(seconds: float) -> Duration:
    d = Duration()
    d.FromNanoseconds(int(seconds * 1e9))
    return d


def _make_instance() -> MagicMock:
    inst = MagicMock(name="lxd_instance")
    inst.name = "job-1"
    inst.architecture = "x86_64"
    inst.expanded_config = {"image.os": "ubuntu"}
    return inst


# ---------------------------------------------------------------------
# _run_with_timeout
# ---------------------------------------------------------------------


def test_run_with_timeout_none_runs_inline() -> None:
    assert _run_with_timeout(lambda: 42, None) == 42


def test_run_with_timeout_returns_result_within_budget() -> None:
    assert _run_with_timeout(lambda: "ok", 1.0) == "ok"


def test_run_with_timeout_raises_on_deadline() -> None:
    with pytest.raises(_TimedOut) as exc:
        _run_with_timeout(lambda: time.sleep(2), 0.05)
    assert exc.value.timeout == 0.05


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
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
) -> None:
    service = BackendPluginService(max_environment_timeout=0.05)

    def slow_create(*_a: object, **_kw: object) -> MagicMock:
        time.sleep(1)
        return _make_instance()

    mock_pylxd_client.instances.create.side_effect = slow_create
    # Cleanup path finds a lingering instance and deletes it.
    lingering = _make_instance()
    lingering.status_code = 100  # not running
    mock_pylxd_client.instances.get.return_value = lingering

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)

    assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[attr-defined]
    lingering.delete.assert_called_once_with(wait=True)
    assert "job-1" not in service._envs  # noqa: SLF001


def test_create_timeout_cleanup_survives_missing_instance(
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
) -> None:
    """LXD hadn't created the instance yet — cleanup lookup 404s, no crash."""
    service = BackendPluginService(max_environment_timeout=0.05)

    def slow_create(*_a: object, **_kw: object) -> MagicMock:
        time.sleep(1)
        return _make_instance()

    mock_pylxd_client.instances.create.side_effect = slow_create
    mock_pylxd_client.instances.get.side_effect = NotFound(
        MagicMock(json=lambda: {"error": "not found"}, status_code=404)
    )

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)
    assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[attr-defined]


def test_create_runner_timeout_wins_when_smaller(
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
) -> None:
    service = BackendPluginService(max_environment_timeout=10.0)

    def slow_create(*_a: object, **_kw: object) -> MagicMock:
        time.sleep(1)
        return _make_instance()

    mock_pylxd_client.instances.create.side_effect = slow_create
    mock_pylxd_client.instances.get.side_effect = NotFound(
        MagicMock(json=lambda: {"error": "not found"}, status_code=404)
    )

    with pytest.raises(aborted) as exc:
        service.Create(_req(environment_timeout=_duration(0.05)), context)
    assert exc.value.code == grpc.StatusCode.DEADLINE_EXCEEDED  # type: ignore[attr-defined]
