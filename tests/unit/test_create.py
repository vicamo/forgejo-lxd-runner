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
            "start": True,
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
    mock_backend_client: MagicMock,
    raw: str,
    expected: list[str],
) -> None:
    service.Create(_req(backend_options={"profiles": raw}), context)

    config = mock_backend_client.launch_instance.call_args.args[0]
    assert config["profiles"] == expected


@pytest.mark.parametrize("raw", ["", "   ", ",,,", " , , "])
def test_create_omits_profiles_when_effectively_empty(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    raw: str,
) -> None:
    """Empty / whitespace-only value → let LXD apply the ``default`` profile."""
    service.Create(_req(backend_options={"profiles": raw}), context)

    config = mock_backend_client.launch_instance.call_args.args[0]
    assert "profiles" not in config
    assert "profiles" not in config


def test_create_records_job_image(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """``image`` is recorded on the env for the later RPCs to act on.

    It must not reach the instance config: ``image`` names a container
    image to run *on* the instance, while the instance itself comes from
    ``label_arg``. Conflating the two would hand LXD a registry
    reference it cannot resolve.
    """
    mock_backend_client.exec_capture.side_effect = [
        (0, "/usr/bin/docker", ""),  # command -v docker
        (1, "", ""),  # command -v podman
        (0, "Server: Docker", ""),  # docker version
        (0, "", ""),  # docker pull
        (0, "deadbeef", ""),  # docker create
        (0, "[]", ""),  # container inspect
    ]

    service.Create(_req(image="node:20"), context)

    assert service._envs["job-1"].job_image == "node:20"  # noqa: SLF001

    config = mock_backend_client.launch_instance.call_args.args[0]
    assert config["source"] == {"type": "image", "alias": "ubuntu:24.04"}
    assert "image" not in config


def test_create_defaults_job_image_to_empty(
    service: BackendPluginService,
    context: MagicMock,
) -> None:
    """No ``container:`` block in the workflow → no job container.

    This is the common ``runs-on: lxd-ubuntu-2404`` job, so the empty
    string has to stay a first-class value rather than an error.
    """
    service.Create(_req(), context)

    assert service._envs["job-1"].job_image == ""  # noqa: SLF001


# ---------------------------------------------------------------------------
# Job containers — `jobs.<id>.container.image`


def test_create_prepares_the_job_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The image is pulled and the container created, but not started."""
    mock_backend_client.exec_capture.side_effect = [
        (0, "/usr/bin/docker", ""),  # command -v docker
        (1, "", ""),  # command -v podman
        (0, "Server: Docker", ""),  # docker version
        (0, "", ""),  # docker pull
        (0, "deadbeef", ""),  # docker create
        (0, '["NODE_VERSION=20.11.0"]', ""),  # container inspect
    ]

    service.Create(_req(image="node:20"), context)

    context.abort.assert_not_called()
    commands = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert ["docker", "pull", "node:20"] in commands
    create = next(c for c in commands if c[:2] == ["docker", "create"])
    # Mounted at identical paths, so CopyIn/CopyOut need no translation.
    assert "/root/actions-runner:/root/actions-runner" in create
    assert "/opt/hostedtoolcache:/opt/hostedtoolcache" in create
    # Starting it is Start's job, not Create's.
    assert not any(c[:2] == ["docker", "start"] for c in commands)
    assert service._envs["job-1"].executor is not None  # noqa: SLF001


def test_create_without_an_image_never_probes_for_a_runtime(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The common case must not pay for a feature it does not use."""
    service.Create(_req(), context)

    mock_backend_client.exec_capture.assert_not_called()
    context.abort.assert_not_called()


def test_create_without_a_runtime_aborts_failed_precondition(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """Operator misconfiguration, not a bad workflow."""
    context.abort.side_effect = RuntimeError("aborted")
    mock_backend_client.exec_capture.return_value = (1, "", "not found")

    with pytest.raises(RuntimeError, match="aborted"):
        service.Create(_req(image="node:20"), context)

    assert context.abort.call_args.args[0] == grpc.StatusCode.FAILED_PRECONDITION


def test_create_with_an_unresolvable_image_aborts_invalid_argument(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """A bad image reference is workflow input, not infrastructure."""
    context.abort.side_effect = RuntimeError("aborted")
    mock_backend_client.exec_capture.side_effect = [
        (0, "/usr/bin/docker", ""),
        (1, "", ""),
        (0, "Server: Docker", ""),
        (1, "", "manifest for node:nope not found"),
    ]

    with pytest.raises(RuntimeError, match="aborted"):
        service.Create(_req(image="node:nope"), context)

    assert context.abort.call_args.args[0] == grpc.StatusCode.INVALID_ARGUMENT


# ---------------------------------------------------------------------------
# Instance leaks on a failed Create


def test_create_removes_the_instance_when_the_job_container_fails(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """Create launched it, so Create has to clean it up.

    Nothing else can: Create aborts without returning an
    ``environment_id``, so Forgejo never learns the environment exists
    and will never call Remove for it. Leaving the instance running
    would strand it until an operator noticed.
    """
    context.abort.side_effect = RuntimeError("aborted")
    mock_backend_client.exec_capture.return_value = (1, "", "not found")

    with pytest.raises(RuntimeError, match="aborted"):
        service.Create(_req(image="node:20"), context)

    mock_backend_client.remove_instance.assert_called_once_with("job-1")
    assert "job-1" not in service._envs  # noqa: SLF001


def test_create_reports_the_original_error_when_cleanup_also_fails(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """A failed cleanup must not mask why Create failed."""
    context.abort.side_effect = RuntimeError("aborted")
    mock_backend_client.exec_capture.return_value = (1, "", "not found")
    mock_backend_client.remove_instance.side_effect = httpx.ConnectError("down")

    with pytest.raises(RuntimeError, match="aborted"):
        service.Create(_req(image="node:20"), context)

    # The runtime probe failure, not the ConnectError from the cleanup.
    assert context.abort.call_args.args[0] == grpc.StatusCode.FAILED_PRECONDITION


def test_create_keeps_the_instance_when_there_is_no_job_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The guard must not fire on the path that cannot fail."""
    service.Create(_req(), context)

    mock_backend_client.remove_instance.assert_not_called()
