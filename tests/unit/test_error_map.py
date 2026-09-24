"""Unit tests for the LXD → gRPC status code mapping."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from pylxd.exceptions import LXDAPIException

from forgejo_lxd_runner.server import _lxd_error_to_grpc


def _exc(status_code: int | None) -> LXDAPIException:
    response = MagicMock()
    if status_code is None:
        # Simulate a transport-layer failure with no response attached.
        del response.status_code
    else:
        response.status_code = status_code
    response.json.return_value = {"error": "x"}
    return LXDAPIException(response)


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
def test_lxd_error_to_grpc(status: int | None, grpc_code: grpc.StatusCode) -> None:
    assert _lxd_error_to_grpc(_exc(status)) == grpc_code
