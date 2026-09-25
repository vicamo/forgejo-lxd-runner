"""Shared pytest fixtures.

Two layers are wired here:

* ``mock_backend_client`` — a ``MagicMock`` standing in for the
  ``BackendClient`` constructor, auto-patched so
  ``BackendPluginService()`` never touches a real LXD or Incus socket.
  Use this for unit tests that only care about the service's own logic.

* ``plugin_stub`` — a real gRPC channel to a real in-process
  ``BackendPluginService`` bound to a unix socket in a tmp dir. Uses the
  mocked BackendClient underneath. Use this for tests that exercise
  streaming, status-code translation, or anything else that only
  surfaces over the wire.
"""

from __future__ import annotations

import threading
from concurrent import futures
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import grpc
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2_grpc
from forgejo_lxd_runner.server import BackendPluginService

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


# --- unit-level fixtures -------------------------------------------------


@pytest.fixture(autouse=True)
def mock_backend_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch ``BackendClient`` used by ``server.py`` and return the instance.

    Auto-used: any unit test that constructs ``BackendPluginService`` —
    whether via the ``service`` fixture or ``BackendPluginService(...)``
    directly — gets the mocked client without asking. Tests that need a
    real socket live under ``tests/e2e/``.
    """
    client = MagicMock(name="BackendClient()")
    factory = MagicMock(name="BackendClient", return_value=client)
    monkeypatch.setattr("forgejo_lxd_runner.server.BackendClient", factory)
    return client


@pytest.fixture
def service(mock_backend_client: MagicMock) -> BackendPluginService:  # noqa: ARG001
    """A fresh service instance with BackendClient mocked out."""
    return BackendPluginService()


@pytest.fixture
def context() -> MagicMock:
    """A gRPC servicer context stub.

    ``abort`` is wired to raise so tests can assert on the status code
    with ``pytest.raises`` — matching real gRPC behaviour, where ``abort``
    never returns.
    """
    ctx = MagicMock(spec=grpc.ServicerContext)

    def _abort(code: grpc.StatusCode, details: str) -> None:
        raise _Aborted(code, details)

    ctx.abort.side_effect = _abort
    return ctx


class _Aborted(Exception):
    """Raised by the fake context to mimic ``ServicerContext.abort``."""

    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__(f"{code.name}: {details}")
        self.code = code
        self.details = details


@pytest.fixture
def aborted() -> type[_Aborted]:
    """Exception type raised by the fake ``context.abort``."""
    return _Aborted


# --- gRPC-level fixtures -------------------------------------------------


@pytest.fixture
def plugin_stub(
    tmp_path: Path,
    mock_backend_client: MagicMock,  # noqa: ARG001 — auto-mocks BackendClient
) -> Iterator[plugin_pb2_grpc.BackendPluginStub]:
    """Start the real service on a unix socket; yield a real client stub."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    plugin_pb2_grpc.add_BackendPluginServicer_to_server(BackendPluginService(), server)

    socket_path = tmp_path / "plugin.sock"
    address = f"unix://{socket_path}"
    server.add_insecure_port(address)
    server.start()

    channel = grpc.insecure_channel(address)
    stub = plugin_pb2_grpc.BackendPluginStub(channel)

    # Wait until the socket is reachable before handing the stub over.
    ready = threading.Event()

    def _wait_ready() -> None:
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
        finally:
            ready.set()

    threading.Thread(target=_wait_ready, daemon=True).start()
    ready.wait(timeout=5)

    try:
        yield stub
    finally:
        channel.close()
        server.stop(grace=0)
