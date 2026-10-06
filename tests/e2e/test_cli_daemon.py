"""Black-box end-to-end test: the real daemon process, driven over its socket.

Every other tier builds the gRPC server in-process with a ``grpc.server``
fixture. This one launches the actual ``forgejo-lxd-runner`` CLI as a
separate process -- exactly as a deployment would -- and drives it from
outside through the unix socket it binds. That is the only tier that
exercises the real entry point: argument parsing, the socket being created
with usable permissions, the metrics endpoint coming up, and a clean
SIGTERM shutdown. It talks to a live LXD/Incus daemon, so a real Create
boots a real instance and Exec runs a real command inside it.

Covers container and VM instances, each with and without a job container.
Requires a live daemon, cloud images, and package/registry egress; VMs need KVM.
"""

from __future__ import annotations

import contextlib
import io
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
import uuid
from collections.abc import Iterator
from pathlib import Path

import grpc
import httpx
import pytest

from forgejo_lxd_runner.client import BackendClient, BackendUnavailableError
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc


@pytest.fixture(scope="module")
def real_client() -> Iterator[BackendClient]:
    try:
        c = BackendClient()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))
    try:
        try:
            _ = c.server_info
        except httpx.ConnectError as exc:
            pytest.skip(f"backend socket unavailable: {exc}")
        yield c
    finally:
        c.close()


@pytest.fixture
def local_image(real_client: BackendClient, instance_type: str) -> Iterator[str]:
    """Pull a cloud-enabled image and give Create a temporary local alias."""
    if instance_type == "virtual-machine" and not Path("/dev/kvm").exists():
        pytest.skip("host has no /dev/kvm")
    sources = {
        "lxd": ("https://cloud-images.ubuntu.com/minimal/releases/", "24.04"),
        "incus": ("https://images.linuxcontainers.org", "ubuntu/24.04/cloud"),
    }
    if real_client.flavor not in sources:
        pytest.skip(f"no cloud image source for {real_client.flavor!r}")
    server, alias = sources[real_client.flavor]
    op = real_client.run_operation(
        "POST",
        "/1.0/images",
        json={
            "source": {
                "type": "image",
                "mode": "pull",
                "protocol": "simplestreams",
                "server": server,
                "alias": alias,
                "image_type": instance_type,
            }
        },
        timeout=900.0,
    )
    fingerprint = op["metadata"]["fingerprint"]
    alias = f"forgejo-e2e-alias-{uuid.uuid4().hex[:10]}"
    real_client.call("POST", "/1.0/images/aliases", json={"name": alias, "target": fingerprint})
    try:
        yield alias
    finally:
        with contextlib.suppress(Exception):
            real_client.request("DELETE", f"/1.0/images/aliases/{alias}")


@pytest.fixture
def job_profiles(
    real_client: BackendClient,
    job_container: bool,
    instance_type: str,
) -> Iterator[str]:
    """Apply runtime profiles and seed VM cloud-init before the guest agent starts."""
    import yaml

    names: list[str] = []
    try:
        stems = ["vm"] if instance_type == "virtual-machine" else []
        if job_container:
            stems += ["base", "docker"]
        for stem in stems:
            name = f"forgejo-e2e-{stem}-{uuid.uuid4().hex[:10]}"
            body = yaml.safe_load(
                (
                    Path(__file__).resolve().parents[2] / "examples" / "profiles" / f"{stem}.yaml"
                ).read_text()
            )
            real_client.create_profile(
                name,
                description=body.get("description"),
                config=body["config"],
                devices=body["devices"],
            )
            names.append(name)
        yield ",".join(["default", *names])
    finally:
        for name in reversed(names):
            with contextlib.suppress(Exception):
                real_client.remove_profile(name)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class _Daemon:
    """A spawned ``forgejo-lxd-runner`` process and the knobs to reach it."""

    def __init__(self, address: str, metrics_address: str, proc: subprocess.Popen[bytes]) -> None:
        self.address = address
        self.metrics_address = metrics_address
        self.proc = proc


@pytest.fixture
def daemon() -> Iterator[_Daemon]:
    """Launch the real CLI on a unix socket with metrics enabled."""
    with tempfile.TemporaryDirectory() as tmp:
        sock = Path(tmp) / "plugin.sock"
        address = f"unix://{sock}"
        metrics_port = _free_port()
        metrics_address = f"127.0.0.1:{metrics_port}"

        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "forgejo_lxd_runner",
                "--address",
                address,
                "--metrics-address",
                metrics_address,
                # Keep the health probe brisk so lxd_reachable is populated
                # well within the test's lifetime.
                "--health-check-interval",
                "1",
            ],
        )

        # Wait for the socket file to appear and accept a gRPC connection.
        deadline = time.monotonic() + 30
        channel = grpc.insecure_channel(address)
        try:
            while True:
                if proc.poll() is not None:
                    pytest.fail(f"daemon exited early with code {proc.returncode}")
                with contextlib.suppress(grpc.FutureTimeoutError):
                    grpc.channel_ready_future(channel).result(timeout=1)
                    break
                if time.monotonic() > deadline:
                    pytest.fail("daemon did not become ready within 30s")
        finally:
            channel.close()

        try:
            yield _Daemon(address, metrics_address, proc)
        finally:
            # The point of the test: SIGTERM drives a clean shutdown.
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    proc.wait(timeout=10)
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=10)


def _scrape(metrics_address: str) -> str:
    with urllib.request.urlopen(f"http://{metrics_address}/metrics", timeout=5) as resp:  # noqa: S310
        return resp.read().decode()


@pytest.mark.parametrize("instance_type", ["container", "virtual-machine"])
@pytest.mark.parametrize("job_container", [False, True], ids=["host-job", "container-job"])
def test_full_lifecycle_against_the_spawned_daemon(
    real_client: BackendClient,
    daemon: _Daemon,
    local_image: str,
    instance_type: str,
    job_container: bool,
    job_profiles: str,
) -> None:
    """Create -> Start -> CopyIn -> Exec -> CopyOut -> Remove, then verify
    metrics moved and SIGTERM shuts the process down cleanly.
    """
    env_id = f"forgejo-e2e-blackbox-{uuid.uuid4().hex[:10]}"
    channel = grpc.insecure_channel(daemon.address)
    stub = plugin_pb2_grpc.BackendPluginStub(channel)
    removed = False
    try:
        # Create boots the requested instance type from the aliased image.
        stub.Create(
            plugin_pb2.CreateRequest(
                name=env_id,
                label_arg=local_image,
                image="ghcr.io/containerd/busybox:latest" if job_container else "",
                backend_options={
                    "type": instance_type,
                    "profiles": job_profiles,
                    "system-ready": "builtin:cloud-init",
                    "system-ready-timeout": "600",
                },
            ),
            timeout=900.0,
        )

        assert real_client.get_instance(env_id)["type"] == instance_type
        # Create leaves a job container stopped; Start brings it up before Exec.
        frames = list(
            stub.Start(
                plugin_pb2.StartRequest(environment_id=env_id),
                timeout=120.0,
            )
        )
        assert frames
        assert frames[-1].WhichOneof("Output") == "start_complete"

        workdir = "/root/actions-runner/act"
        payload = b"copied into the execution context\n"
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            info = tarfile.TarInfo("input.txt")
            info.size = len(payload)
            tar.addfile(info, io.BytesIO(payload))
        data = archive.getvalue()
        stub.CopyIn(
            iter(
                [
                    plugin_pb2.CopyInChunk(
                        environment_id=env_id, dest_path=workdir, data=data[:512]
                    ),
                    plugin_pb2.CopyInChunk(data=data[512:]),
                ]
            ),
            timeout=120.0,
        )

        # Exec runs a real command inside it; collect stdout and exit code.
        marker = "forgejo-lxd-runner-blackbox-ok"
        stdout = b""
        stderr = b""
        exit_code = None
        for out in stub.Exec(
            plugin_pb2.ExecRequest(
                environment_id=env_id,
                command=[
                    "sh",
                    "-c",
                    f"cat {workdir}/input.txt; printf '%s\\n' {marker} > {workdir}/output.txt",
                ],
            ),
            timeout=120.0,
        ):
            kind = out.WhichOneof("Output")
            if kind == "data" and out.data.stream == plugin_pb2.DataChunk.STDOUT:
                stdout += out.data.data
            elif kind == "data" and out.data.stream == plugin_pb2.DataChunk.STDERR:
                stderr += out.data.data
            elif kind == "exec_complete":
                exit_code = out.exec_complete.exit_code
            elif kind == "exec_failed":
                pytest.fail(f"exec failed: {out.exec_failed.error_message}")

        assert exit_code == 0, f"Exec exited {exit_code}: {stderr.decode(errors='replace')}"
        assert stdout == payload
        copied = b"".join(
            chunk.data
            for chunk in stub.CopyOut(
                plugin_pb2.CopyOutRequest(environment_id=env_id, src_path=f"{workdir}/output.txt"),
                timeout=120.0,
            )
        )
        with tarfile.open(fileobj=io.BytesIO(copied)) as tar:
            members = tar.getmembers()
            assert len(members) == 1
            file = tar.extractfile(members[0])
            assert file is not None
            assert file.read() == f"{marker}\n".encode()

        # Metrics reflect the live traffic and a healthy daemon.
        deadline = time.monotonic() + 10
        while True:
            body = _scrape(daemon.metrics_address)
            if "forgejo_lxd_runner_lxd_reachable 1.0" in body:
                break
            if time.monotonic() > deadline:
                pytest.fail(f"lxd_reachable never went to 1; last scrape:\n{body}")
            time.sleep(0.5)
        assert 'forgejo_lxd_runner_rpc_requests_total{code="OK",method="Create"}' in body
        assert 'forgejo_lxd_runner_rpc_requests_total{code="OK",method="Exec"}' in body

        for method in ("Start", "CopyIn", "CopyOut"):
            assert f'forgejo_lxd_runner_rpc_requests_total{{code="OK",method="{method}"}}' in body

        stub.Remove(plugin_pb2.RemoveRequest(environment_id=env_id), timeout=120.0)
        removed = True
    finally:
        channel.close()
        if not removed:
            with contextlib.suppress(Exception):
                real_client.remove_instance(env_id, timeout=60.0)

    # SIGTERM must bring the process down cleanly (exit 0), proving the
    # signal-driven shutdown path works end to end.
    daemon.proc.send_signal(signal.SIGTERM)
    assert daemon.proc.wait(timeout=10) == 0
