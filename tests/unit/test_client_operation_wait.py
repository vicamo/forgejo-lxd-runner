"""Unit tests for ``BackendClient.operation_wait`` polling and timeout.

``operation_wait`` polls ``/1.0/operations/<uuid>/wait`` in fixed windows.
``/wait`` is a *synchronous* endpoint: each poll returns the standard
response envelope. A ``type="error"`` envelope means the operation failed
(the message is on the envelope, ``metadata`` is null); otherwise the
operation record rides in ``metadata``, where ``status_code`` 200 means
Success and a non-terminal code (the daemon reports a running operation as
``103``) means the window elapsed with the op still in flight. A
non-terminal code keeps the loop going until the op finishes or the
caller's overall ``timeout`` deadline is reached.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from forgejo_lxd_runner.client import (
    _OP_POLL_READ_MARGIN,
    _OP_POLL_WINDOW,
    BackendClient,
    BackendOperationError,
    BackendOperationTimeout,
)


def _envelope(metadata: dict[str, Any] | None, *, error: str | None = None) -> MagicMock:
    """Build a fake ``/wait`` HTTP response carrying a daemon envelope."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    if error is not None:
        resp.json.return_value = {"type": "error", "error": error, "metadata": None}
    else:
        resp.json.return_value = {"type": "sync", "metadata": metadata}
    return resp


@pytest.fixture
def client() -> BackendClient:
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.request = MagicMock()  # type: ignore[method-assign]
    return c


def test_success_returns_the_record(client: BackendClient) -> None:
    client.request.return_value = _envelope({"status_code": 200, "id": "op-1"})  # type: ignore[attr-defined]

    assert client.operation_wait({"id": "op-1"}) == {"status_code": 200, "id": "op-1"}


def test_failed_op_raises_operation_error(client: BackendClient) -> None:
    """A failed op is signalled by the envelope (``type="error"``), not the record."""
    client.request.return_value = _envelope(None, error="no such image")  # type: ignore[attr-defined]

    with pytest.raises(BackendOperationError, match="no such image"):
        client.operation_wait({"id": "op-1"})


def test_each_poll_uses_a_fixed_bounded_window(client: BackendClient) -> None:
    """The window sent to the daemon and the HTTP read budget are fixed,
    independent of how long the whole operation takes."""
    client.request.return_value = _envelope({"status_code": 200})  # type: ignore[attr-defined]

    client.operation_wait({"id": "op-1"})

    _, kwargs = client.request.call_args  # type: ignore[attr-defined]
    assert kwargs["params"] == {"timeout": _OP_POLL_WINDOW}
    assert isinstance(kwargs["timeout"], httpx.Timeout)
    assert kwargs["timeout"].read == _OP_POLL_WINDOW + _OP_POLL_READ_MARGIN


def test_a_running_op_keeps_polling_until_it_finishes(client: BackendClient) -> None:
    """A non-terminal code is proof the op is still alive -> loop, don't give up.

    This is the cold-image-pull case: the operation outlasts a single
    window, so the daemon returns ``103`` (Running) repeatedly before
    ``200``. The daemon uses ``103`` for a running operation, not ``101``.
    """
    client.request.side_effect = [  # type: ignore[attr-defined]
        _envelope({"status_code": 103}),
        _envelope({"status_code": 103}),
        _envelope({"status_code": 200, "id": "op-1"}),
    ]

    result = client.operation_wait({"id": "op-1"})

    assert result == {"status_code": 200, "id": "op-1"}
    assert client.request.call_count == 3  # type: ignore[attr-defined]


def test_wait_without_a_timeout_loops_indefinitely(client: BackendClient) -> None:
    """``timeout=None`` never gives up on a still-running op."""
    client.request.side_effect = [  # type: ignore[attr-defined]
        _envelope({"status_code": 103}),
        _envelope({"status_code": 103}),
        _envelope({"status_code": 103}),
        _envelope({"status_code": 200}),
    ]

    assert client.operation_wait({"id": "op-1"}) == {"status_code": 200}


def test_overall_timeout_raises_when_the_op_never_finishes(
    client: BackendClient,
) -> None:
    """A perpetually-running op is bounded by the caller's wall-clock timeout."""
    client.request.return_value = _envelope({"status_code": 103})  # type: ignore[attr-defined]

    with pytest.raises(BackendOperationTimeout) as exc:
        client.operation_wait({"id": "op-1"}, timeout=0.01)

    assert exc.value.timeout == 0.01


def test_final_poll_window_shrinks_to_the_deadline(client: BackendClient) -> None:
    """With less than a full window left, we ask the daemon for only the
    remaining time so we never overshoot the caller's deadline."""
    client.request.return_value = _envelope({"status_code": 200})  # type: ignore[attr-defined]

    client.operation_wait({"id": "op-1"}, timeout=3.0)

    _, kwargs = client.request.call_args  # type: ignore[attr-defined]
    assert kwargs["params"]["timeout"] <= 3
    assert kwargs["params"]["timeout"] >= 1
