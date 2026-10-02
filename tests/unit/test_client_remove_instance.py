"""Unit tests for ``BackendClient.remove_instance``.

Exercises the state-probe / stop / delete dance and the "tolerate 404 at
every step" contract. All HTTP is mocked at the ``get_instance_state`` /
``set_instance_state`` / ``run_operation`` boundary so we can drive each
branch (already-gone before probe, gone between probe and stop, gone
between stop and delete, stopped-so-skip-stop, running-so-stop).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from forgejo_lxd_runner.client import BackendClient

_STATUS_STOPPED = 102
_STATUS_RUNNING = 103


def _http_status_error(status_code: int) -> httpx.HTTPStatusError:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    return httpx.HTTPStatusError("boom", request=MagicMock(), response=resp)


@pytest.fixture
def client() -> BackendClient:
    """A ``BackendClient`` whose HTTP surfaces are all mocks."""
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.get_instance_state = MagicMock()  # type: ignore[method-assign]
    c.set_instance_state = MagicMock()  # type: ignore[method-assign]
    c.run_operation = MagicMock()  # type: ignore[method-assign]
    return c


def test_remove_instance_skips_stop_when_already_stopped(client: BackendClient) -> None:
    client.get_instance_state.return_value = {"status_code": _STATUS_STOPPED}  # type: ignore[attr-defined]

    client.remove_instance("job-1")

    client.get_instance_state.assert_called_once_with("job-1", project=None)  # type: ignore[attr-defined]
    client.set_instance_state.assert_not_called()  # type: ignore[attr-defined]
    client.run_operation.assert_called_once_with(  # type: ignore[attr-defined]
        "DELETE",
        "/1.0/instances/job-1",
        project=None,
        timeout=None,
        missing_ok=True,
    )


def test_remove_instance_stops_running_then_deletes(client: BackendClient) -> None:
    client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}  # type: ignore[attr-defined]

    client.remove_instance("job-1", project="proj-a", force=True, timeout=5.0)

    client.set_instance_state.assert_called_once_with(  # type: ignore[attr-defined]
        "job-1", "stop", project="proj-a", timeout=5.0, force=True
    )
    client.run_operation.assert_called_once_with(  # type: ignore[attr-defined]
        "DELETE",
        "/1.0/instances/job-1",
        project="proj-a",
        timeout=5.0,
        missing_ok=True,
    )


def test_remove_instance_tolerates_missing_before_state_probe(client: BackendClient) -> None:
    """404 on the state probe → post-condition already holds, no stop/delete."""
    client.get_instance_state.side_effect = _http_status_error(404)  # type: ignore[attr-defined]

    client.remove_instance("job-1")

    client.set_instance_state.assert_not_called()  # type: ignore[attr-defined]
    client.run_operation.assert_not_called()  # type: ignore[attr-defined]


def test_remove_instance_reraises_non_404_from_state_probe(client: BackendClient) -> None:
    client.get_instance_state.side_effect = _http_status_error(500)  # type: ignore[attr-defined]

    with pytest.raises(httpx.HTTPStatusError):
        client.remove_instance("job-1")

    client.set_instance_state.assert_not_called()  # type: ignore[attr-defined]
    client.run_operation.assert_not_called()  # type: ignore[attr-defined]


def test_remove_instance_tolerates_missing_between_state_and_stop(
    client: BackendClient,
) -> None:
    """Instance vanishes between the state GET and the stop PUT — the stop
    call raises ``HTTPStatusError(404)`` and ``remove_instance`` swallows
    it without issuing the DELETE."""
    client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}  # type: ignore[attr-defined]
    client.set_instance_state.side_effect = _http_status_error(404)  # type: ignore[attr-defined]

    client.remove_instance("job-1")

    client.run_operation.assert_not_called()  # type: ignore[attr-defined]


def test_remove_instance_reraises_non_404_from_stop(client: BackendClient) -> None:
    client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}  # type: ignore[attr-defined]
    client.set_instance_state.side_effect = _http_status_error(500)  # type: ignore[attr-defined]

    with pytest.raises(httpx.HTTPStatusError):
        client.remove_instance("job-1")

    client.run_operation.assert_not_called()  # type: ignore[attr-defined]


def test_remove_instance_delete_uses_missing_ok(client: BackendClient) -> None:
    """The final DELETE routes through ``run_operation(missing_ok=True)`` so
    a race with a concurrent delete between our stop and delete still
    satisfies the post-condition."""
    client.get_instance_state.return_value = {"status_code": _STATUS_STOPPED}  # type: ignore[attr-defined]
    # simulates 404 swallowed by missing_ok
    client.run_operation.return_value = None  # type: ignore[attr-defined]

    result: Any = client.remove_instance("job-1")

    assert result is None
    # Verify the missing_ok kwarg is actually passed (contract lock).
    _, kwargs = client.run_operation.call_args  # type: ignore[attr-defined]
    assert kwargs["missing_ok"] is True


def test_remove_instance_force_false_is_forwarded(client: BackendClient) -> None:
    client.get_instance_state.return_value = {"status_code": _STATUS_RUNNING}  # type: ignore[attr-defined]

    client.remove_instance("job-1", force=False)

    _, kwargs = client.set_instance_state.call_args  # type: ignore[attr-defined]
    assert kwargs["force"] is False
