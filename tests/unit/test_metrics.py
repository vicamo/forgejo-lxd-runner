"""Unit tests for the opt-in Prometheus metrics endpoint."""

from __future__ import annotations

from concurrent import futures
from unittest.mock import MagicMock

import grpc
import pytest
from prometheus_client import generate_latest

from forgejo_lxd_runner.metrics import Metrics, _code_name


def _scrape(metrics: Metrics) -> str:
    return generate_latest(metrics.registry).decode()


def test_gauges_expose_reachability_and_active_count() -> None:
    metrics = Metrics()
    metrics.set_lxd_reachable(True)
    metrics.track_active_environments(lambda: 4)

    body = _scrape(metrics)
    assert "forgejo_lxd_runner_lxd_reachable 1.0" in body
    assert "forgejo_lxd_runner_active_environments 4.0" in body


def test_active_environments_is_evaluated_at_scrape_time() -> None:
    metrics = Metrics()
    state = {"n": 1}
    metrics.track_active_environments(lambda: state["n"])

    assert "forgejo_lxd_runner_active_environments 1.0" in _scrape(metrics)
    state["n"] = 7
    assert "forgejo_lxd_runner_active_environments 7.0" in _scrape(metrics)


def test_reachability_flips_to_zero() -> None:
    metrics = Metrics()
    metrics.set_lxd_reachable(False)
    assert "forgejo_lxd_runner_lxd_reachable 0.0" in _scrape(metrics)


def test_two_instances_do_not_collide() -> None:
    """Each Metrics owns a private registry -- constructing twice is safe."""
    Metrics()
    Metrics()  # would raise Duplicated timeseries on a shared registry


def test_code_name_maps_unset_context_to_ok() -> None:
    context = MagicMock(spec=grpc.ServicerContext)
    context.code.return_value = None
    assert _code_name(context) == "OK"


def test_code_name_reads_the_aborted_code() -> None:
    context = MagicMock(spec=grpc.ServicerContext)
    context.code.return_value = grpc.StatusCode.NOT_FOUND
    assert _code_name(context) == "NOT_FOUND"


# ----------------------------------------------------------------------
# Interceptor over a real in-process server -- the only way to exercise
# the handler-wrapping across call shapes and the abort path.
# ----------------------------------------------------------------------
def _server_with(metrics: Metrics) -> tuple[grpc.Server, int]:
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=2),
        interceptors=(metrics.interceptor(),),
    )

    def ok(request: bytes, context: grpc.ServicerContext) -> bytes:
        return b"ok"

    def boom(request: bytes, context: grpc.ServicerContext) -> bytes:
        context.abort(grpc.StatusCode.NOT_FOUND, "gone")
        raise AssertionError("unreachable")

    rpcs = {
        "Ok": grpc.unary_unary_rpc_method_handler(ok),
        "Boom": grpc.unary_unary_rpc_method_handler(boom),
    }
    handler = grpc.method_handlers_generic_handler("t.Svc", rpcs)
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, port


def test_interceptor_counts_success_and_abort_by_code() -> None:
    metrics = Metrics()
    server, port = _server_with(metrics)
    try:
        channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        channel.unary_unary("/t.Svc/Ok")(b"")
        with pytest.raises(grpc.RpcError):
            channel.unary_unary("/t.Svc/Boom")(b"")

        body = _scrape(metrics)
        assert 'forgejo_lxd_runner_rpc_requests_total{code="OK",method="Ok"} 1.0' in body
        assert 'forgejo_lxd_runner_rpc_requests_total{code="NOT_FOUND",method="Boom"} 1.0' in body
    finally:
        server.stop(0)
