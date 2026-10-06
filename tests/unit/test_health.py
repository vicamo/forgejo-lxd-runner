"""Unit tests for the LXD health service."""

from __future__ import annotations

from unittest.mock import MagicMock

from grpc_health.v1 import health_pb2

from forgejo_lxd_runner.health import HealthService


def _service_with_client(client: MagicMock) -> MagicMock:
    service = MagicMock()
    service._client = client
    return service


def _ok_response() -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    return resp


def _status(hs: HealthService, name: str) -> int:
    resp = hs.Check(health_pb2.HealthCheckRequest(service=name), context=MagicMock())
    return resp.status


def test_probe_success_publishes_serving() -> None:
    client = MagicMock()
    client.request.return_value = _ok_response()

    hs = HealthService(_service_with_client(client), interval=0)
    hs._publish(hs._probe())

    client.request.assert_called_once_with("GET", "/1.0")
    for name in ("", "plugin.v1alpha.BackendPlugin"):
        assert _status(hs, name) == health_pb2.HealthCheckResponse.SERVING


def test_probe_failure_publishes_not_serving() -> None:
    client = MagicMock()
    client.request.side_effect = RuntimeError("boom")

    hs = HealthService(_service_with_client(client), interval=0)
    hs._publish(hs._probe())

    for name in ("", "plugin.v1alpha.BackendPlugin"):
        assert _status(hs, name) == health_pb2.HealthCheckResponse.NOT_SERVING


def test_status_transitions_flip_both_ways() -> None:
    client = MagicMock()
    client.request.return_value = _ok_response()
    hs = HealthService(_service_with_client(client), interval=0)

    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.SERVING

    client.request.side_effect = RuntimeError("gone")
    client.request.return_value = None
    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.NOT_SERVING

    client.request.side_effect = None
    client.request.return_value = _ok_response()
    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.SERVING


def test_zero_interval_disables_run_loop() -> None:
    """``interval=0`` starts no thread."""
    hs = HealthService(_service_with_client(MagicMock()), interval=0)
    hs.start()
    assert hs._thread is None


def test_on_status_callback_fires_with_probe_result() -> None:
    """An ``on_status`` hook (used to feed the metrics gauge) sees every probe."""
    seen: list[bool] = []
    client = MagicMock()
    client.request.return_value = _ok_response()
    hs = HealthService(
        _service_with_client(client),
        interval=0,
        on_status=seen.append,
    )

    hs._publish(hs._probe())
    client.request.side_effect = RuntimeError("gone")
    hs._publish(hs._probe())

    assert seen == [True, False]
