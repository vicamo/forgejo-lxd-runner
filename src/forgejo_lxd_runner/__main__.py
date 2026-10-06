"""CLI entry point: start the BackendPlugin gRPC server."""

from __future__ import annotations

import argparse
import logging
import signal
import threading
from concurrent import futures
from typing import Any

import grpc
from grpc_health.v1 import health_pb2_grpc

from . import __version__
from .health import HealthService
from .server import BackendPluginService

log = logging.getLogger("forgejo_lxd_runner")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="forgejo-lxd-runner")
    p.add_argument(
        "--name",
        default=BackendPluginService.DEFAULT_NAME,
        help=(
            "Backend name returned by Capabilities and referenced by the "
            "runner's label scheme (<label>:<name>://<arg>). Change it when "
            "running multiple plugin processes with different connection "
            "settings so each is addressable independently. Default: %(default)s."
        ),
    )
    p.add_argument(
        "--address",
        default="unix:///run/forgejo-lxd-runner.sock",
        help="gRPC bind address (e.g. unix:///path/to.sock or 127.0.0.1:50051).",
    )
    p.add_argument("--workers", type=int, default=16, help="Thread-pool size.")
    p.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Root logger level (case-insensitive). Default: INFO.",
    )
    p.add_argument(
        "--health-check-interval",
        type=float,
        default=HealthService.DEFAULT_INTERVAL,
        help=(
            "Seconds between LXD health probes. The result is reflected into "
            "the standard grpc.health.v1 status. 0 disables the poller."
        ),
    )
    p.add_argument(
        "--max-environment-timeout",
        type=float,
        default=0.0,
        help=(
            "Upper bound (seconds) on how long Create will wait for LXD to "
            "provision an instance. 0 disables the cap; the runner-supplied "
            "environment_timeout is honoured as-is. When both are set the "
            "smaller wins."
        ),
    )
    p.add_argument(
        "--instance-name-prefix",
        default="",
        help=(
            "String prepended to every LXD instance name. The runner's "
            "environment_id is unchanged; only the LXD-side name is "
            "namespaced. Default empty (1:1 mapping). Set this when "
            "multiple daemons share one LXD project."
        ),
    )
    p.add_argument(
        "--endpoint",
        default=None,
        help=(
            "Remote LXD/Incus HTTPS endpoint (https://host:port). When set, "
            "the daemon is reached over mutual TLS using --client-cert and "
            "--client-key instead of a local Unix socket. Default: autodetect "
            "the local socket. The daemon's server certificate is verified "
            "against the system trust store."
        ),
    )
    p.add_argument(
        "--client-cert",
        default=None,
        help=(
            "Path to the PEM client certificate for mutual-TLS auth against "
            "--endpoint. Required with --endpoint."
        ),
    )
    p.add_argument(
        "--client-key",
        default=None,
        help=(
            "Path to the PEM client private key for mutual-TLS auth against "
            "--endpoint. Required with --endpoint."
        ),
    )
    p.add_argument(
        "--tls-server-cert",
        default=None,
        help=(
            "Path to a PEM certificate used to verify the remote daemon's "
            "server certificate (pin a self-signed cert — the LXD/Incus "
            "default). Only meaningful with --endpoint. Default: verify "
            "against the system trust store."
        ),
    )
    p.add_argument(
        "--metrics-address",
        default=None,
        help=(
            "Enable a Prometheus metrics HTTP endpoint on this host:port "
            "(e.g. 127.0.0.1:9095). A bare :port binds loopback only. "
            "Default: disabled. Requires the optional prometheus-client "
            "dependency."
        ),
    )
    p.add_argument(
        "--cluster-target",
        default=None,
        help=(
            "Default cluster member to place instances on (?target= at create "
            "time) when the daemon is clustered. A per-label 'cluster-target' "
            "backend option overrides this. Default: let the cluster schedule."
        ),
    )
    return p


def serve(
    address: str,
    workers: int,
    name: str = BackendPluginService.DEFAULT_NAME,
    health_check_interval: float = 10.0,
    max_environment_timeout: float = 0.0,
    instance_name_prefix: str = "",
    endpoint: str | None = None,
    client_cert: str | None = None,
    client_key: str | None = None,
    server_cert: str | None = None,
    cluster_target: str | None = None,
    metrics_address: str | None = None,
) -> None:
    from .proto.plugin.v1alpha import plugin_pb2_grpc

    log.info("forgejo-lxd-runner %s starting", __version__)

    metrics = None
    interceptors: list[grpc.ServerInterceptor[Any, Any]] = []
    if metrics_address is not None:
        from .metrics import Metrics

        metrics = Metrics()
        interceptors = [metrics.interceptor()]

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=workers),
        interceptors=interceptors,
    )

    backend_service = BackendPluginService(
        name=name,
        max_environment_timeout=max_environment_timeout,
        instance_name_prefix=instance_name_prefix,
        endpoint=endpoint,
        client_cert=client_cert,
        client_key=client_key,
        server_cert=server_cert,
        cluster_target=cluster_target,
    )
    plugin_pb2_grpc.add_BackendPluginServicer_to_server(backend_service, server)  # type: ignore[no-untyped-call]

    on_status = metrics.set_lxd_reachable if metrics is not None else None
    health_service = HealthService(
        backend_service,
        interval=health_check_interval,
        on_status=on_status,
    )
    health_pb2_grpc.add_HealthServicer_to_server(health_service, server)

    server.add_insecure_port(address)
    server.start()
    log.info("forgejo-lxd-runner %r listening on %s", name, address)

    if metrics is not None and metrics_address is not None:
        metrics.track_active_environments(backend_service.environment_count)
        metrics.serve(metrics_address)

    health_service.start()

    # One shutdown path: a signal sets the event, the main thread wakes
    # and drains the server with a grace period. ``server.stop`` is only
    # ever called here, after a signal -- never speculatively at startup.
    stop = threading.Event()

    def _handle(_signum: int, _frame: object) -> None:
        log.info("shutting down")
        stop.set()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)

    stop.wait()
    health_service.stop()
    server.stop(grace=5).wait()


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    serve(
        args.address,
        args.workers,
        args.name,
        args.health_check_interval,
        args.max_environment_timeout,
        args.instance_name_prefix,
        args.endpoint,
        args.client_cert,
        args.client_key,
        args.tls_server_cert,
        args.cluster_target,
        args.metrics_address,
    )


if __name__ == "__main__":
    main()
