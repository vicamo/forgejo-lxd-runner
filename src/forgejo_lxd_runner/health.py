"""Health-check gRPC service reflecting LXD reachability."""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from grpc_health.v1 import health, health_pb2

if TYPE_CHECKING:
    from .server import BackendPluginService

log = logging.getLogger(__name__)

# gRPC service name the plugin registers under; the empty overall-status name
# is always published alongside so generic clients get an answer too.
_BACKEND_SERVICE_NAME = "plugin.v1alpha.BackendPlugin"


class HealthService(health.HealthServicer):
    """``grpc.health.v1`` servicer that reflects LXD reachability.

    A daemon thread wakes every ``interval`` seconds, pings LXD's ``/1.0``
    endpoint through the backend service's cached client, and publishes
    ``SERVING`` / ``NOT_SERVING`` under both the overall-status name (``""``)
    and ``plugin.v1alpha.BackendPlugin`` — the two names ``grpc_health_probe``
    and generic gRPC health clients ask about.

    ``Check`` and ``Watch`` are inherited from :class:`HealthServicer` and
    read whatever the poller last published via :meth:`set`.
    """

    DEFAULT_INTERVAL = 10.0

    def __init__(
        self,
        service: BackendPluginService,
        interval: float = DEFAULT_INTERVAL,
    ) -> None:
        super().__init__()
        self._service = service
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        # Track the last status we published so we only log transitions,
        # not every successful probe.
        self._last: int | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the polling thread. No-op if ``interval <= 0``."""
        if self._interval <= 0 or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="HealthService-poller",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        """Signal the polling thread to exit and join it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    # ------------------------------------------------------------------
    # Probe loop
    # ------------------------------------------------------------------
    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._publish(self._probe())
            # Event.wait returns True when set — that's our exit signal.
            if self._stop_event.wait(self._interval):
                return

    def _probe(self) -> bool:
        try:
            # ``/1.0`` is LXD's canonical liveness endpoint — it's what
            # ``pylxd.Client()`` itself hits to authenticate, so it's the
            # cheapest "is LXD alive?" round-trip we can make.
            self._service._client.api["1.0"].get()  # noqa: SLF001
        except Exception:  # noqa: BLE001
            log.warning("LXD health probe failed", exc_info=True)
            return False
        return True

    def _publish(self, serving: bool) -> None:
        status = (
            health_pb2.HealthCheckResponse.SERVING
            if serving
            else health_pb2.HealthCheckResponse.NOT_SERVING
        )
        if status != self._last:
            log.info("HealthService status: %s", "SERVING" if serving else "NOT_SERVING")
            self._last = status
        self.set("", status)
        self.set(_BACKEND_SERVICE_NAME, status)
