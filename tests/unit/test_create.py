"""Unit tests for ``BackendPluginService.Create``."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import httpx
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu:24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def test_create_launches_instance_from_label_arg(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    resp = service.Create(_req(), context)

    mock_backend_client.launch_instance.assert_called_once_with(
        {
            "name": "job-1",
            "source": {"type": "image", "alias": "ubuntu:24.04"},
        },
    )
    assert resp.environment_id == "job-1"
    assert resp.os == "Linux"
    assert resp.arch == "X64"
    # Registered in the internal map.
    assert "job-1" in service._envs  # noqa: SLF001


def test_create_rejects_empty_label_arg(
    service: BackendPluginService, context: MagicMock, aborted: type[Exception]
) -> None:
    with pytest.raises(aborted) as exc:
        service.Create(_req(label_arg=""), context)
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


def test_create_rejects_empty_name(
    service: BackendPluginService, context: MagicMock, aborted: type[Exception]
) -> None:
    with pytest.raises(aborted) as exc:
        service.Create(_req(name=""), context)
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]


def test_create_maps_http_failure_to_internal(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    aborted: type[Exception],
) -> None:
    mock_backend_client.launch_instance.side_effect = httpx.HTTPStatusError(
        "boom", request=MagicMock(), response=MagicMock(status_code=500)
    )

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)
    assert exc.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
    assert "job-1" not in service._envs  # noqa: SLF001
