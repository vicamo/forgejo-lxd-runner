"""Unit tests for ``BackendClient.create_network`` / ``remove_network``.

Both endpoints are synchronous on LXD and Incus, so the mocks sit at the
``call`` / ``request`` boundary rather than at ``run_operation``.
``create_network`` is checked for the "only send what the caller passed"
contract; ``remove_network`` for the 404-tolerance that mirrors
``remove_profile``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from forgejo_lxd_runner.client import BackendClient


def _response(status_code: int) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "boom", request=MagicMock(), response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


@pytest.fixture
def client() -> BackendClient:
    """A ``BackendClient`` whose HTTP surfaces are all mocks."""
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.call = MagicMock(return_value={})  # type: ignore[method-assign]
    c.request = MagicMock()  # type: ignore[method-assign]
    return c


# ---------------------------------------------------------------------------
# create_network


def test_create_network_sends_only_the_name_by_default(client: BackendClient) -> None:
    """Absent keys are left to the daemon rather than synthesised here."""
    client.create_network("flr-abc123")

    client.call.assert_called_once_with(  # type: ignore[attr-defined]
        "POST",
        "/1.0/networks",
        project=None,
        json={"name": "flr-abc123"},
    )


def test_create_network_forwards_every_supplied_field(client: BackendClient) -> None:
    client.create_network(
        "flr-abc123",
        description="per-job isolated bridge",
        config={"ipv4.nat": "true", "ipv6.address": "none"},
        project="proj-a",
    )

    client.call.assert_called_once_with(  # type: ignore[attr-defined]
        "POST",
        "/1.0/networks",
        project="proj-a",
        json={
            "name": "flr-abc123",
            "description": "per-job isolated bridge",
            "config": {"ipv4.nat": "true", "ipv6.address": "none"},
        },
    )


def test_create_network_sends_empty_config_when_explicitly_passed(
    client: BackendClient,
) -> None:
    """``{}`` is a caller decision ("no config"), not the same as absence."""
    client.create_network("flr-abc123", config={})

    payload = client.call.call_args.kwargs["json"]  # type: ignore[attr-defined]
    assert payload == {"name": "flr-abc123", "config": {}}


def test_create_network_propagates_http_errors(client: BackendClient) -> None:
    """A duplicate name (409) is the caller's to map, not ours to swallow."""
    client.call.side_effect = httpx.HTTPStatusError(  # type: ignore[attr-defined]
        "conflict", request=MagicMock(), response=_response(409)
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.create_network("flr-abc123")


# ---------------------------------------------------------------------------
# remove_network


def test_remove_network_deletes_via_request(client: BackendClient) -> None:
    client.request.return_value = _response(200)  # type: ignore[attr-defined]

    client.remove_network("flr-abc123")

    client.request.assert_called_once_with(  # type: ignore[attr-defined]
        "DELETE", "/1.0/networks/flr-abc123", project=None
    )


def test_remove_network_forwards_project(client: BackendClient) -> None:
    client.request.return_value = _response(200)  # type: ignore[attr-defined]

    client.remove_network("flr-abc123", project="proj-a")

    client.request.assert_called_once_with(  # type: ignore[attr-defined]
        "DELETE", "/1.0/networks/flr-abc123", project="proj-a"
    )


def test_remove_network_tolerates_missing_network(client: BackendClient) -> None:
    """404 already satisfies the post-condition, so it must not raise."""
    resp = _response(404)
    client.request.return_value = resp  # type: ignore[attr-defined]

    client.remove_network("flr-abc123")

    resp.raise_for_status.assert_not_called()


@pytest.mark.parametrize("status_code", [400, 403, 500])
def test_remove_network_raises_on_other_errors(client: BackendClient, status_code: int) -> None:
    """In-use (400), forbidden (403) and daemon faults still propagate."""
    client.request.return_value = _response(status_code)  # type: ignore[attr-defined]

    with pytest.raises(httpx.HTTPStatusError):
        client.remove_network("flr-abc123")
