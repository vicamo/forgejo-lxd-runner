"""Unit tests for the ``ephemeral`` backend option."""

from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu/24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def _make_instance() -> MagicMock:
    inst = MagicMock(name="lxd_instance")
    inst.name = "job-1"
    inst.architecture = "x86_64"
    inst.expanded_config = {"image.os": "ubuntu"}
    return inst


@pytest.mark.parametrize("raw", ["true", "TRUE", "1", "yes", "on", " True "])
def test_create_forwards_ephemeral_true(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    raw: str,
) -> None:
    mock_pylxd_client.instances.create.return_value = _make_instance()
    service.Create(_req(backend_options={"ephemeral": raw}), context)
    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["ephemeral"] is True


@pytest.mark.parametrize("raw", ["false", "FALSE", "0", "no", "off", " False "])
def test_create_forwards_ephemeral_false(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    raw: str,
) -> None:
    mock_pylxd_client.instances.create.return_value = _make_instance()
    service.Create(_req(backend_options={"ephemeral": raw}), context)
    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["ephemeral"] is False


def test_create_omits_ephemeral_when_absent(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    mock_pylxd_client.instances.create.return_value = _make_instance()
    service.Create(_req(), context)
    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert "ephemeral" not in config


@pytest.mark.parametrize("raw", ["maybe", "yesplease", "2", "-"])
def test_create_rejects_invalid_ephemeral(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
    raw: str,
) -> None:
    with pytest.raises(aborted) as exc:
        service.Create(_req(backend_options={"ephemeral": raw}), context)
    assert exc.value.code == grpc.StatusCode.INVALID_ARGUMENT  # type: ignore[attr-defined]
    mock_pylxd_client.instances.create.assert_not_called()
