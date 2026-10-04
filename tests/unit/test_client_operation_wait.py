"""Unit tests for ``BackendClient.operation_wait`` timeout handling.

The daemon returns 200 with the operation record even when ``?timeout=``
elapses; a still-running op carries ``status_code=101``. Under an explicit
timeout that case becomes ``BackendOperationTimeout``; without one it is
impossible (the wait blocks until the op finishes) and must pass through.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from forgejo_lxd_runner.client import (
    BackendClient,
    BackendOperationError,
    BackendOperationTimeout,
)


@pytest.fixture
def client() -> BackendClient:
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.call = MagicMock()  # type: ignore[method-assign]
    return c


def test_running_op_under_a_timeout_raises(client: BackendClient) -> None:
    client.call.return_value = {"status_code": 101}  # type: ignore[attr-defined]

    with pytest.raises(BackendOperationTimeout) as exc:
        client.operation_wait({"id": "op-1"}, timeout=5.0)

    assert exc.value.timeout == 5.0
    # ?timeout= is sent as an integer number of seconds.
    _, kwargs = client.call.call_args  # type: ignore[attr-defined]
    assert kwargs["params"] == {"timeout": 5}


def test_subsecond_timeout_rounds_up_to_one(client: BackendClient) -> None:
    client.call.return_value = {"status_code": 200}  # type: ignore[attr-defined]

    client.operation_wait({"id": "op-1"}, timeout=0.05)

    _, kwargs = client.call.call_args  # type: ignore[attr-defined]
    assert kwargs["params"] == {"timeout": 1}


def test_success_returns_the_record(client: BackendClient) -> None:
    client.call.return_value = {"status_code": 200, "id": "op-1"}  # type: ignore[attr-defined]

    assert client.operation_wait({"id": "op-1"}) == {"status_code": 200, "id": "op-1"}


def test_failed_op_raises_operation_error(client: BackendClient) -> None:
    client.call.return_value = {"status_code": 400, "err": "no such image"}  # type: ignore[attr-defined]

    with pytest.raises(BackendOperationError, match="no such image"):
        client.operation_wait({"id": "op-1"})


def test_running_op_without_a_timeout_passes_through(client: BackendClient) -> None:
    """Without ``?timeout=`` a 101 cannot be a timeout, so it is returned."""
    client.call.return_value = {"status_code": 101}  # type: ignore[attr-defined]

    assert client.operation_wait({"id": "op-1"}) == {"status_code": 101}
