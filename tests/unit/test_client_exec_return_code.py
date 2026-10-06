"""Unit tests for ``BackendClient._exec_return_code``.

An exec operation usually records the guest's exit status as ``return`` on
its operation record. On a virtual-machine, LXD 6.x does not: its QEMU exec
driver fails the operation with a fixed message for two special statuses
(exit 127 -> "Command not found", exit 126 -> "Command not executable")
rather than recording them. These are legitimate command results -- a
runtime probe (`command -v docker`) relies on exit 127 meaning "absent" --
so they must map back to the exit code, not escape as a backend error.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from forgejo_lxd_runner.client import BackendClient, BackendOperationError


@pytest.fixture
def client() -> BackendClient:
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.call = MagicMock()  # type: ignore[method-assign]
    c.operation_wait = MagicMock()  # type: ignore[method-assign]
    return c


def test_return_recorded_on_the_operation(client: BackendClient) -> None:
    client.call.return_value = {"metadata": {"return": 3}}  # type: ignore[attr-defined]

    assert client._exec_return_code("op-1") == 3  # noqa: SLF001
    client.operation_wait.assert_not_called()  # type: ignore[attr-defined]


def test_falls_back_to_wait_when_return_absent(client: BackendClient) -> None:
    client.call.return_value = {"metadata": {}}  # type: ignore[attr-defined]
    client.operation_wait.return_value = {"metadata": {"return": 0}}  # type: ignore[attr-defined]

    assert client._exec_return_code("op-1") == 0  # noqa: SLF001
    client.operation_wait.assert_called_once()  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("Command not found", 127),
        ("Command not executable", 126),
    ],
)
def test_vm_special_exit_status_maps_to_exit_code(
    client: BackendClient, message: str, expected: int
) -> None:
    """LXD 6.x VM exec reports 127/126 as a failed operation; map them back."""
    client.call.return_value = {"metadata": {}}  # type: ignore[attr-defined]
    client.operation_wait.side_effect = BackendOperationError(message)  # type: ignore[attr-defined]

    assert client._exec_return_code("op-1") == expected  # noqa: SLF001


def test_an_unrelated_operation_error_still_propagates(client: BackendClient) -> None:
    """Only the two documented exec statuses are swallowed; real failures raise."""
    client.call.return_value = {"metadata": {}}  # type: ignore[attr-defined]
    client.operation_wait.side_effect = BackendOperationError("instance is not running")  # type: ignore[attr-defined]

    with pytest.raises(BackendOperationError, match="not running"):
        client._exec_return_code("op-1")  # noqa: SLF001
