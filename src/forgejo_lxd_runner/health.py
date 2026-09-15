"""Health-check gRPC service skeleton."""

from __future__ import annotations

from grpc_health.v1 import health


class HealthService(health.HealthServicer):
    """Plugin health servicer; skeleton — implementation lands in later commits."""
