"""Health-check gRPC service reflecting LXD reachability."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
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
        on_status: Callable[[bool], None] | None = None,
    ) -> None:
        super().__init__()
        self._service = service
        self._interval = interval
        self._on_status = on_status
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
            # ``/1.0`` is the daemon's canonical liveness endpoint —
            # cheap, unauthenticated, and it's the same URL the client
            # itself hits to populate ``server_info``. We bypass that
            # property here because it caches; a health probe must
            # round-trip every time.
            resp = self._service._client.request("GET", "/1.0")  # noqa: SLF001
            resp.raise_for_status()
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
        if self._on_status is not None:
            self._on_status(serving)
