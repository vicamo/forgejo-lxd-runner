"""CLI entry point: start the BackendPlugin gRPC server."""

from __future__ import annotations

import argparse
import logging
import signal
from concurrent import futures

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
        "--health-check-interval",
        type=float,
        default=HealthService.DEFAULT_INTERVAL,
        help=(
            "Seconds between LXD health probes. The result is reflected into "
            "the standard grpc.health.v1 status. 0 disables the poller."
        ),
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        type=str.upper,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Root logger level (case-insensitive). Default: INFO.",
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
    return p


def serve(
    address: str,
    workers: int,
    name: str = BackendPluginService.DEFAULT_NAME,
    health_check_interval: float = 10.0,
    max_environment_timeout: float = 0.0,
    instance_name_prefix: str = "",
) -> None:
    from .proto.plugin.v1alpha import plugin_pb2_grpc

    log.info("forgejo-lxd-runner %s starting", __version__)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))

    backend_service = BackendPluginService(
        name=name,
        max_environment_timeout=max_environment_timeout,
        instance_name_prefix=instance_name_prefix,
    )
    plugin_pb2_grpc.add_BackendPluginServicer_to_server(backend_service, server)  # type: ignore[no-untyped-call]

    health_service = HealthService(backend_service, interval=health_check_interval)
    health_pb2_grpc.add_HealthServicer_to_server(health_service, server)

    server.add_insecure_port(address)
    server.start()
    log.info("forgejo-lxd-runner %r listening on %s", name, address)

    health_service.start()

    stop = server.stop(grace=5)

    def _handle(_signum: int, _frame: object) -> None:
        log.info("shutting down")
        health_service.stop()
        stop.set() if hasattr(stop, "set") else server.stop(grace=5)

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    server.wait_for_termination()


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
    )


if __name__ == "__main__":
    main()
