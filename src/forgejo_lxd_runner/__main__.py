"""CLI entry point: start the BackendPlugin gRPC server."""

from __future__ import annotations

import argparse
import logging
import signal
from concurrent import futures

import grpc
from grpc_health.v1 import health_pb2_grpc

from .health import HealthService
from .server import BackendPluginService

log = logging.getLogger("forgejo_lxd_runner")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="forgejo-lxd-runner")
    p.add_argument(
        "--address",
        default="unix:///run/forgejo-lxd-runner.sock",
        help="gRPC bind address (e.g. unix:///path/to.sock or 127.0.0.1:50051).",
    )
    p.add_argument("--workers", type=int, default=16, help="Thread-pool size.")
    p.add_argument("--log-level", default="INFO")
    return p


def serve(address: str, workers: int) -> None:
    from .proto.plugin.v1alpha import plugin_pb2_grpc

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))

    backend_service = BackendPluginService()
    plugin_pb2_grpc.add_BackendPluginServicer_to_server(backend_service, server)  # type: ignore[no-untyped-call]

    health_service = HealthService()
    health_pb2_grpc.add_HealthServicer_to_server(health_service, server)

    server.add_insecure_port(address)
    server.start()
    log.info("forgejo-lxd-runner listening on %s", address)

    stop = server.stop(grace=5)

    def _handle(_signum: int, _frame: object) -> None:
        log.info("shutting down")
        stop.set() if hasattr(stop, "set") else server.stop(grace=5)

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    server.wait_for_termination()


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    serve(args.address, args.workers)


if __name__ == "__main__":
    main()
