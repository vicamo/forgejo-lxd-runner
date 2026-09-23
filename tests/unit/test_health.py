"""Unit tests for the LXD health service."""

from __future__ import annotations

from unittest.mock import MagicMock

from grpc_health.v1 import health_pb2

from forgejo_lxd_runner.health import HealthService


def _service_with_client(client: MagicMock) -> MagicMock:
    service = MagicMock()
    service._client_for.return_value = client
    return service


def _status(hs: HealthService, name: str) -> int:
    resp = hs.Check(health_pb2.HealthCheckRequest(service=name), context=MagicMock())
    return resp.status


def test_probe_success_publishes_serving() -> None:
    client = MagicMock()
    client.api = {"1.0": MagicMock()}

    hs = HealthService(_service_with_client(client), interval=0)
    hs._publish(hs._probe())

    for name in ("", "plugin.v1alpha.BackendPlugin"):
        assert _status(hs, name) == health_pb2.HealthCheckResponse.SERVING


def test_probe_failure_publishes_not_serving() -> None:
    client = MagicMock()
    client.api = {"1.0": MagicMock()}
    client.api["1.0"].get.side_effect = RuntimeError("boom")

    hs = HealthService(_service_with_client(client), interval=0)
    hs._publish(hs._probe())

    for name in ("", "plugin.v1alpha.BackendPlugin"):
        assert _status(hs, name) == health_pb2.HealthCheckResponse.NOT_SERVING


def test_status_transitions_flip_both_ways() -> None:
    client = MagicMock()
    client.api = {"1.0": MagicMock()}
    hs = HealthService(_service_with_client(client), interval=0)

    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.SERVING

    client.api["1.0"].get.side_effect = RuntimeError("gone")
    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.NOT_SERVING

    client.api["1.0"].get.side_effect = None
    hs._publish(hs._probe())
    assert _status(hs, "") == health_pb2.HealthCheckResponse.SERVING


def test_zero_interval_disables_run_loop() -> None:
    """``interval=0`` starts no thread."""
    hs = HealthService(_service_with_client(MagicMock()), interval=0)
    hs.start()
    assert hs._thread is None
