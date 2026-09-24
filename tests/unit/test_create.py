"""Unit tests for ``BackendPluginService.Create``."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from pylxd.exceptions import LXDAPIException

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu/24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def test_create_launches_instance_from_label_arg(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    mock_pylxd_client.instances.create.return_value = created

    resp = service.Create(_req(), context)

    mock_pylxd_client.instances.create.assert_called_once_with(
        {
            "name": "job-1",
            "source": {"type": "image", "alias": "ubuntu/24.04"},
        },
        wait=True,
    )
    assert resp.environment_id == "job-1"
    assert resp.os == "linux"
    assert resp.arch == "amd64"
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


def test_create_maps_lxd_failure_to_internal(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
) -> None:
    mock_pylxd_client.instances.create.side_effect = LXDAPIException(
        MagicMock(json=lambda: {"error": "boom"}, status_code=500)
    )
    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)
    assert exc.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
    assert "job-1" not in service._envs  # noqa: SLF001
