"""Unit tests for ``--instance-name-prefix``."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService


def _req(**kw: object) -> plugin_pb2.CreateRequest:
    kw.setdefault("name", "job-1")
    kw.setdefault("label_arg", "ubuntu:24.04")
    return plugin_pb2.CreateRequest(**kw)  # type: ignore[arg-type]


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
    mock_backend_client: MagicMock,
    prefix: str,
    expected_lxd_name: str,
) -> None:
    service = BackendPluginService(instance_name_prefix=prefix)

    resp = service.Create(_req(), context)

    # LXD receives the prefixed name in the POST body.
    config = mock_backend_client.launch_instance.call_args.args[0]
    assert config["name"] == expected_lxd_name
    # But the runner-facing environment_id is unchanged.
    assert resp.environment_id == "job-1"
    # And the env's stored instance_name reflects the LXD-side name.
    assert service._envs["job-1"].instance_name == expected_lxd_name  # noqa: SLF001


def test_create_default_prefix_is_empty(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The default-constructed service (used by every other test) has no prefix."""
    service.Create(_req(), context)
    config = mock_backend_client.launch_instance.call_args.args[0]
    assert config["name"] == "job-1"
