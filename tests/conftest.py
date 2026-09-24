"""Shared pytest fixtures.

Two layers are wired here:

* ``mock_pylxd_client`` — a ``MagicMock`` standing in for ``pylxd.Client``,
  auto-patched so ``BackendPluginService()`` never touches a real LXD.
  Use this for unit tests that only care about the service's own logic.

* ``plugin_stub`` — a real gRPC channel to a real in-process
  ``BackendPluginService`` bound to a unix socket in a tmp dir. Uses the
  mocked pylxd underneath. Use this for tests that exercise streaming,
  status-code translation, or anything else that only surfaces over the
  wire.
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


@pytest.fixture
def mock_pylxd_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Patch ``pylxd.Client`` used by ``server.py`` and return the instance."""
    client = MagicMock(name="pylxd.Client()")
    factory = MagicMock(name="pylxd.Client", return_value=client)
    monkeypatch.setattr("forgejo_lxd_runner.server.pylxd.Client", factory)
    return client


@pytest.fixture
def service(mock_pylxd_client: MagicMock) -> BackendPluginService:
    """A fresh service instance with pylxd mocked out."""
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
    mock_pylxd_client: MagicMock,  # noqa: ARG001 — auto-mocks pylxd
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
