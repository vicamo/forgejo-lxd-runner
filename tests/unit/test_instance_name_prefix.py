"""Unit tests for ``--instance-name-prefix``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu/24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


def _make_instance(name: str) -> MagicMock:
    inst = MagicMock(name="lxd_instance")
    inst.name = name
    inst.architecture = "x86_64"
    inst.expanded_config = {"image.os": "ubuntu"}
    return inst


@pytest.mark.parametrize(
    ("prefix", "expected_lxd_name"),
    [
        ("", "job-1"),
        ("runner-a-", "runner-a-job-1"),
        ("prod-", "prod-job-1"),
    ],
)
def test_create_prefixes_lxd_instance_name(
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    prefix: str,
    expected_lxd_name: str,
) -> None:
    service = BackendPluginService(instance_name_prefix=prefix)
    mock_pylxd_client.instances.create.return_value = _make_instance(expected_lxd_name)

    resp = service.Create(_req(), context)

    # LXD receives the prefixed name.
    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["name"] == expected_lxd_name
    # But the runner-facing environment_id is unchanged.
    assert resp.environment_id == "job-1"
    # And the env's stored instance_name reflects what LXD echoed back.
    assert service._envs["job-1"].instance_name == expected_lxd_name  # noqa: SLF001


def test_create_default_prefix_is_empty(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    """The default-constructed service (used by every other test) has no prefix."""
    mock_pylxd_client.instances.create.return_value = _make_instance("job-1")
    service.Create(_req(), context)
    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["name"] == "job-1"
