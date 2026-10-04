"""Unit tests for the REST -> gRPC status code mapping."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import httpx
import pytest

from forgejo_lxd_runner.client import BackendOperationError
from forgejo_lxd_runner.server import _rest_error_to_grpc


def _http_exc(status_code: int | None) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "http://lxd/1.0/instances/x")
    if status_code is None:
        # Transport-layer failure: no response attached at all.
        return httpx.HTTPStatusError("boom", request=request, response=None)  # type: ignore[arg-type]
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f"HTTP {status_code}", request=request, response=response)


@pytest.mark.parametrize(
    ("status", "grpc_code"),
    [
        (400, grpc.StatusCode.INVALID_ARGUMENT),
        (409, grpc.StatusCode.INVALID_ARGUMENT),
        (422, grpc.StatusCode.INVALID_ARGUMENT),
        (404, grpc.StatusCode.NOT_FOUND),
        (403, grpc.StatusCode.PERMISSION_DENIED),
        (500, grpc.StatusCode.INTERNAL),
        (502, grpc.StatusCode.INTERNAL),
        (None, grpc.StatusCode.INTERNAL),
    ],
)
def test_http_error_to_grpc(status: int | None, grpc_code: grpc.StatusCode) -> None:
    assert _rest_error_to_grpc(_http_exc(status)) == grpc_code


def test_backend_operation_error_maps_to_invalid_argument() -> None:
    # Async op failure -- daemon reports status_code=400 inside the op record.
    assert (
        _rest_error_to_grpc(BackendOperationError("bad profile"))
        == grpc.StatusCode.INVALID_ARGUMENT
    )


def test_generic_request_error_maps_to_internal() -> None:
    err = httpx.ConnectError("no route", request=MagicMock(spec=httpx.Request))
    assert _rest_error_to_grpc(err) == grpc.StatusCode.INTERNAL
