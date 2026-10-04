"""Unit tests for ``BackendClient.get_instance``.

A synchronous ``GET /1.0/instances/<name>`` wrapper, so the mock sits at
the ``call`` boundary. The record it returns is the daemon's static view
of the instance -- ``architecture`` and friends -- which the server reads
to fill ``CreateResponse.arch``.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from forgejo_lxd_runner.client import BackendClient


@pytest.fixture
def client() -> BackendClient:
    """A ``BackendClient`` whose HTTP surfaces are all mocks."""
    with patch.object(BackendClient, "__init__", return_value=None):
        c = BackendClient()  # type: ignore[call-arg]
    c.call = MagicMock(return_value={})  # type: ignore[method-assign]
    return c


def test_get_instance_reads_the_instance_record(client: BackendClient) -> None:
    record = {"name": "job-1", "architecture": "x86_64"}
    client.call = MagicMock(return_value=record)  # type: ignore[method-assign]

    assert client.get_instance("job-1") == record
    client.call.assert_called_once_with(  # type: ignore[attr-defined]
        "GET", "/1.0/instances/job-1", project=None
    )


def test_get_instance_forwards_project(client: BackendClient) -> None:
    client.get_instance("job-1", project="proj-a")

    client.call.assert_called_once_with(  # type: ignore[attr-defined]
        "GET", "/1.0/instances/job-1", project="proj-a"
    )


def test_get_instance_propagates_http_errors(client: BackendClient) -> None:
    """A missing instance (404) is the caller's to map, not ours to swallow."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 404
    client.call.side_effect = httpx.HTTPStatusError(  # type: ignore[attr-defined]
        "not found", request=MagicMock(), response=resp
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.get_instance("ghost")
