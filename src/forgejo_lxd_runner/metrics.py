"""Prometheus metrics: opt-in HTTP exposition for operator monitoring.

This module is imported only when ``--metrics-address`` is set, so the
``prometheus_client`` dependency stays optional -- a daemon started without
the flag never imports it. See ``docs/usage.md`` for the exposed series.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Any

import grpc
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

if TYPE_CHECKING:
    from grpc import HandlerCallDetails, RpcMethodHandler

log = logging.getLogger(__name__)

_METRIC_PREFIX = "forgejo_lxd_runner"


def _code_name(context: grpc.ServicerContext) -> str:
    """Status code set on ``context``, as a label value.

    A handler that returns normally leaves the code unset -- that is the
    ``OK`` case. ``context.abort`` records the gRPC code before raising, and
    it is still readable from the ``finally`` block that calls this.
    """
    try:
        code = context.code()  # type: ignore[attr-defined]  # set by abort(); grpc stub omits it
    except Exception:  # noqa: BLE001 — a context without a code is just OK
        code = None
    if code is None:
        return "OK"
    name: str = code.name
    return name


class Metrics:
    """Holds the collectors and the machinery that feeds them.

    Owns a private :class:`CollectorRegistry` rather than the global default
    so repeated construction (tests, multiple daemons in one process) never
    raises ``Duplicated timeseries``.
    """

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.rpc_requests = Counter(
            f"{_METRIC_PREFIX}_rpc_requests_total",
            "gRPC RPCs handled, by method and resulting status code.",
            ["method", "code"],
            registry=self.registry,
        )
        self.lxd_reachable = Gauge(
            f"{_METRIC_PREFIX}_lxd_reachable",
            "1 if the last LXD health probe succeeded, 0 otherwise.",
            registry=self.registry,
        )
        self.active_environments = Gauge(
            f"{_METRIC_PREFIX}_active_environments",
            "Environments currently tracked by the backend.",
            registry=self.registry,
        )

    def track_active_environments(self, count: Callable[[], int]) -> None:
        """Evaluate ``count`` at scrape time for the active-env gauge."""
        self.active_environments.set_function(count)

    def set_lxd_reachable(self, serving: bool) -> None:
        """Reflect a health-probe result into the reachability gauge."""
        self.lxd_reachable.set(1 if serving else 0)

    def interceptor(self) -> grpc.ServerInterceptor[Any, Any]:
        """Server interceptor that counts every RPC by method and code."""
        return _MetricsInterceptor(self.rpc_requests)

    def serve(self, address: str) -> None:
        """Start the metrics HTTP endpoint on ``host:port``.

        A bare ``:port`` (or a port with no host) binds loopback only --
        metrics are operator-facing and should not be world-exposed by
        default; widen the bind explicitly when a remote Prometheus needs it.
        """
        host, _, port = address.rpartition(":")
        start_http_server(int(port), addr=host or "127.0.0.1", registry=self.registry)
        log.info("metrics endpoint listening on %s", address)


class _MetricsInterceptor(grpc.ServerInterceptor):  # type: ignore[type-arg]  # not subscriptable at runtime
    """Counts RPCs on completion, covering every gRPC call shape."""

    def __init__(self, counter: Counter) -> None:
        self._counter = counter

    def intercept_service(
        self,
        continuation: Callable[[HandlerCallDetails], RpcMethodHandler[Any, Any] | None],
        handler_call_details: HandlerCallDetails,
    ) -> RpcMethodHandler[Any, Any] | None:
        handler = continuation(handler_call_details)
        if handler is None:
            return handler
        method = handler_call_details.method.rsplit("/", 1)[-1]

        behavior: Any
        factory: Any
        if handler.unary_unary is not None:
            behavior = handler.unary_unary
            factory = grpc.unary_unary_rpc_method_handler
            response_streaming = False
        elif handler.unary_stream is not None:
            behavior = handler.unary_stream
            factory = grpc.unary_stream_rpc_method_handler
            response_streaming = True
        elif handler.stream_unary is not None:
            behavior = handler.stream_unary
            factory = grpc.stream_unary_rpc_method_handler
            response_streaming = False
        else:
            behavior = handler.stream_stream
            factory = grpc.stream_stream_rpc_method_handler
            response_streaming = True
        assert behavior is not None  # noqa: S101 — one of the four shapes is always set

        instrumented = factory(
            self._instrument(behavior, method, response_streaming=response_streaming),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
        return instrumented  # type: ignore[no-any-return]  # factory is Any over the 4 call shapes

    def _instrument(
        self,
        behavior: Callable[..., Any],
        method: str,
        *,
        response_streaming: bool,
    ) -> Callable[..., Any]:
        counter = self._counter

        def record(context: grpc.ServicerContext) -> None:
            counter.labels(method=method, code=_code_name(context)).inc()

        if response_streaming:

            def stream_wrapper(request: Any, context: grpc.ServicerContext) -> Iterator[Any]:
                try:
                    yield from behavior(request, context)
                finally:
                    # Runs once the stream drains or the handler aborts --
                    # the code the client observes is set by then.
                    record(context)

            return stream_wrapper

        def unary_wrapper(request: Any, context: grpc.ServicerContext) -> Any:
            try:
                return behavior(request, context)
            finally:
                record(context)

        return unary_wrapper
