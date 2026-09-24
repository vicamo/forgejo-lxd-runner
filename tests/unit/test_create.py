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
    created.architecture = "x86_64"
    created.expanded_config = {"image.os": "ubuntu"}
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


@pytest.mark.parametrize(
    ("status", "grpc_code"),
    [
        (400, grpc.StatusCode.INVALID_ARGUMENT),
        (404, grpc.StatusCode.NOT_FOUND),
        (403, grpc.StatusCode.PERMISSION_DENIED),
    ],
)
def test_create_maps_lxd_user_errors_to_client_status(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    aborted: type[Exception],
    status: int,
    grpc_code: grpc.StatusCode,
) -> None:
    """A user-facing LXD error surfaces as a client-side gRPC status.

    Prevents the runner from mistaking config typos (unknown profile,
    missing project) for INTERNAL failures and retrying them forever.
    """
    mock_pylxd_client.instances.create.side_effect = LXDAPIException(
        MagicMock(json=lambda: {"error": "x"}, status_code=status)
    )
    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)
    assert exc.value.code == grpc_code  # type: ignore[attr-defined]
    assert "job-1" not in service._envs  # noqa: SLF001


@pytest.mark.parametrize(
    ("lxd_arch", "reported_arch", "gha_arch"),
    [
        ("x86_64", "x86_64", "X64"),
        ("aarch64", "aarch64", "ARM64"),
        ("armv7l", "armv7l", "ARM"),
        ("s390x", "s390x", "S390x"),
        ("ppc64le", "ppc64le", "Ppc64le"),
        ("riscv64", "riscv64", "RiscV64"),
        # Unknown-to-us LXD string: forwarded verbatim, echoed back in RUNNER_ARCH.
        ("sparc64", "sparc64", "sparc64"),
    ],
)
def test_create_honours_lxd_arch_backend_option(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    lxd_arch: str,
    reported_arch: str,
    gha_arch: str,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = reported_arch
    mock_pylxd_client.instances.create.return_value = created

    resp = service.Create(_req(backend_options={"lxd_arch": lxd_arch}), context)

    mock_pylxd_client.instances.create.assert_called_once_with(
        {
            "name": "job-1",
            "source": {"type": "image", "alias": "ubuntu/24.04"},
            "architecture": lxd_arch,
        },
        wait=True,
    )
    assert resp.arch == gha_arch


def test_create_omits_architecture_when_option_absent(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(), context)

    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert "architecture" not in config


def test_create_honours_project_backend_option(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    created.expanded_config = {"image.os": "ubuntu"}
    mock_pylxd_client.instances.create.return_value = created

    resp = service.Create(_req(backend_options={"project": "ci"}), context)

    # A project-bound Client was constructed.
    mock_pylxd_client.factory.assert_any_call(project="ci")
    assert resp.environment_id == "job-1"
    # Env remembers the project so Start / Exec / Remove hit the same one.
    assert service._envs["job-1"].project == "ci"  # noqa: SLF001


def test_create_reuses_project_client(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(name="job-a", backend_options={"project": "ci"}), context)
    service.Create(_req(name="job-b", backend_options={"project": "ci"}), context)

    # One project=ci Client for both creates — the cache saves the second
    # socket open.
    project_calls = [
        c for c in mock_pylxd_client.factory.call_args_list if c.kwargs.get("project") == "ci"
    ]
    assert len(project_calls) == 1


def test_create_without_project_uses_default_client(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(), context)

    # No ``project=`` kwarg was passed — the default Client() covers it.
    for call in mock_pylxd_client.factory.call_args_list:
        assert "project" not in call.kwargs
    assert service._envs["job-1"].project is None  # noqa: SLF001


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ci", ["ci"]),
        ("ci,gpu", ["ci", "gpu"]),
        ("  ci ,  gpu  ", ["ci", "gpu"]),  # whitespace tolerated
        ("ci,,gpu", ["ci", "gpu"]),  # empty entries dropped
    ],
)
def test_create_passes_profiles(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    raw: str,
    expected: list[str],
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(backend_options={"profiles": raw}), context)

    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["profiles"] == expected


@pytest.mark.parametrize("raw", ["", "   ", ",,,", " , , "])
def test_create_omits_profiles_when_effectively_empty(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    raw: str,
) -> None:
    """Empty / whitespace-only value → let LXD apply the ``default`` profile."""
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(backend_options={"profiles": raw}), context)

    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert "profiles" not in config


@pytest.mark.parametrize(
    "instance_type", ["container", "virtual-machine", "unknown-type-passthrough"]
)
def test_create_passes_type(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
    instance_type: str,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(backend_options={"type": instance_type}), context)

    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert config["type"] == instance_type


def test_create_omits_type_when_option_absent(
    service: BackendPluginService,
    context: MagicMock,
    mock_pylxd_client: MagicMock,
) -> None:
    created = MagicMock(name="lxd_instance")
    created.name = "job-1"
    created.architecture = "x86_64"
    mock_pylxd_client.instances.create.return_value = created

    service.Create(_req(), context)

    (config,), _ = mock_pylxd_client.instances.create.call_args
    assert "type" not in config
