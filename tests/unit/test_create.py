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

    payload = mock_backend_client.launch_instance.call_args.args[0]
    assert payload["name"] == "job-1"
    assert payload["source"] == {"type": "image", "alias": "ubuntu:24.04"}
    assert payload["start"] is True
    assert resp.environment_id == "job-1"
    assert resp.os == "Linux"
    assert resp.arch == "X64"
    # POSIX shell semantics, fixed regardless of the reported OS.
    assert resp.path_variable_name == "PATH"
    assert resp.path_separator == ":"
    assert resp.default_path_variable == (
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )
    assert resp.environment_case_insensitive is False
    # Registered in the internal map.
    assert "job-1" in service._envs  # noqa: SLF001
    mock_backend_client.get_instance.assert_called_once_with("job-1")


@pytest.mark.parametrize(
    ("reported_arch", "gha_arch"),
    [
        ("x86_64", "X64"),
        ("aarch64", "ARM64"),
        ("armv7l", "ARM"),
        ("s390x", "S390x"),
        ("ppc64le", "Ppc64le"),
        ("riscv64", "RiscV64"),
        # Unknown-to-us LXD string: echoed back verbatim in RUNNER_ARCH.
        ("sparc64", "sparc64"),
    ],
)
def test_create_reports_the_instance_architecture(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    reported_arch: str,
    gha_arch: str,
) -> None:
    """arch comes from the instance record, mapped to GHA vocabulary."""
    mock_backend_client.get_instance.return_value = {
        "architecture": reported_arch,
        "expanded_config": {"image.os": "ubuntu"},
    }

    resp = service.Create(_req(), context)

    assert resp.arch == gha_arch


@pytest.mark.parametrize(
    ("image_os", "gha_os"),
    [
        ("ubuntu", "Linux"),
        ("freebsd", "FreeBSD"),
        ("windows", "Windows"),
        # Missing metadata defaults to Linux.
        ("", "Linux"),
    ],
)
def test_create_reports_the_instance_os(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
    image_os: str,
    gha_os: str,
) -> None:
    """os comes from the image.os metadata, mapped to GHA vocabulary."""
    mock_backend_client.get_instance.return_value = {
        "architecture": "x86_64",
        "expanded_config": {"image.os": image_os} if image_os else {},
    }

    resp = service.Create(_req(), context)

    assert resp.os == gha_os


def test_create_discards_the_instance_when_the_arch_fetch_fails(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    mock_backend_client: MagicMock,
) -> None:
    """A record we can't read is an instance we can't describe: tear it down."""
    mock_backend_client.get_instance.side_effect = httpx.HTTPError("boom")

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)

    assert exc.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
    mock_backend_client.remove_instance.assert_called_once_with("job-1")
    assert "job-1" not in service._envs  # noqa: SLF001


def test_create_gives_the_instance_its_own_isolated_network(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """Every job gets a NAT'd bridge of its own, and the NIC rides it."""
    service.Create(_req(), context)

    net = mock_backend_client.create_network.call_args
    name = net.args[0]
    # A bridge name is the host interface name: <=15 chars.
    assert name.startswith("flr-")
    assert len(name) <= 15
    assert net.kwargs["config"] == {"ipv4.nat": "true", "ipv6.address": "none"}

    payload = mock_backend_client.launch_instance.call_args.args[0]
    assert payload["devices"] == {"eth0": {"type": "nic", "network": name}}
    # The name is remembered for Remove to reclaim.
    assert service._envs["job-1"].network == name  # noqa: SLF001


def test_create_before_the_network_is_not_cleaned_up(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    mock_backend_client: MagicMock,
) -> None:
    """A network-create failure aborts before anything exists to reclaim."""
    mock_backend_client.create_network.side_effect = httpx.HTTPError("boom")

    with pytest.raises(aborted) as exc:
        service.Create(_req(), context)

    assert exc.value.code == grpc.StatusCode.INTERNAL  # type: ignore[attr-defined]
    mock_backend_client.launch_instance.assert_not_called()
    mock_backend_client.remove_network.assert_not_called()


def test_create_reclaims_the_network_when_the_launch_fails(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    mock_backend_client: MagicMock,
) -> None:
    """The launch never referenced the network, so Create drops it."""
    mock_backend_client.launch_instance.side_effect = httpx.HTTPError("boom")

    with pytest.raises(aborted):
        service.Create(_req(), context)

    name = mock_backend_client.create_network.call_args.args[0]
    mock_backend_client.remove_network.assert_called_once_with(name)
    assert "job-1" not in service._envs  # noqa: SLF001


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


def test_create_without_an_image_still_detects_the_runtime(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """The instance's runtime is recorded whatever the job runs in."""
    mock_backend_client.exec_capture.return_value = (0, "", "")

    service.Create(_req(), context)

    probes = [c.args[1] for c in mock_backend_client.exec_capture.call_args_list]
    assert ["sh", "-c", "command -v docker"] in probes
    assert service._envs["job-1"].executor.runtime == "docker"  # noqa: SLF001
    context.abort.assert_not_called()


def test_create_without_an_image_survives_an_instance_with_no_runtime(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """A plain job on a plain image is the common case, not a failure."""
    mock_backend_client.exec_capture.return_value = (1, "", "not found")

    service.Create(_req(), context)

    assert service._envs["job-1"].executor.runtime == ""  # noqa: SLF001
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


# -----------------------
# Service containers
# -----------------------


def _svc(**kw: object) -> plugin_pb2.ServiceContainer:
    kw.setdefault("name", "redis")
    kw.setdefault("image", "redis:7")
    return plugin_pb2.ServiceContainer(**kw)  # type: ignore[arg-type]


def _runtime_calls(mock_backend_client: MagicMock) -> list[list[str]]:
    return [call.args[1] for call in mock_backend_client.exec_capture.call_args_list]


def test_create_without_services_creates_no_network(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """Detection still happens; a job with no services just uses nothing."""
    mock_backend_client.exec_capture.return_value = (0, "", "")

    service.Create(_req(), context)

    calls = _runtime_calls(mock_backend_client)
    assert not any("network" in c for c in calls)
    assert service._envs["job-1"].services is None  # noqa: SLF001


def test_create_starts_services_for_a_job_with_no_container(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """Services are containers even when the job itself runs on the instance.

    The job reaches them at localhost on their published ports, so they
    need no network of their own.
    """
    mock_backend_client.exec_capture.return_value = (0, "", "")

    service.Create(_req(services=[_svc(ports=["6379:6379"])]), context)

    calls = _runtime_calls(mock_backend_client)
    assert not any("network" in c for c in calls)
    run = next(c for c in calls if c[:2] == ["docker", "run"])
    assert "--network" not in run
    assert run[run.index("--publish") + 1] == "6379:6379"
    assert service._envs["job-1"].services is not None  # noqa: SLF001


def test_create_joins_the_job_container_to_the_service_network(
    service: BackendPluginService,
    context: MagicMock,
    mock_backend_client: MagicMock,
) -> None:
    """A step reaches a service by name only if both share a network."""
    mock_backend_client.exec_capture.return_value = (0, "", "")

    service.Create(_req(image="alpine", services=[_svc()]), context)

    create = next(c for c in _runtime_calls(mock_backend_client) if c[:2] == ["docker", "create"])
    assert create[create.index("--network") + 1] == "job-1"


def test_create_discards_the_instance_when_a_service_fails(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    mock_backend_client: MagicMock,
) -> None:
    """Create never returned an id, so nothing would ever Remove the instance."""
    mock_backend_client.exec_capture.side_effect = [
        (0, "/usr/bin/docker", ""),  # command -v docker
        (1, "", ""),  # command -v podman
        (0, "Server: Docker", ""),  # docker version
        (1, "", "no such image"),  # pull
    ]

    with pytest.raises(aborted):
        service.Create(_req(services=[_svc()]), context)

    mock_backend_client.remove_instance.assert_called_once_with("job-1")
    assert "job-1" not in service._envs  # noqa: SLF001


def test_create_rejects_services_on_an_instance_with_no_runtime(
    service: BackendPluginService,
    context: MagicMock,
    aborted: type[Exception],
    mock_backend_client: MagicMock,
) -> None:
    """Absence only becomes an error once something asks for a runtime."""
    mock_backend_client.exec_capture.return_value = (1, "", "not found")

    with pytest.raises(aborted) as exc:
        service.Create(_req(services=[_svc()]), context)

    assert exc.value.code == grpc.StatusCode.FAILED_PRECONDITION
    # The message has to name what wanted it.
    assert "services:" in str(exc.value)
    mock_backend_client.remove_instance.assert_called_once_with("job-1")
