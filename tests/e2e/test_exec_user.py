"""RPC-level end-to-end tests for ``ExecRequest.user`` resolution.

``Exec`` passes ``request.user`` -- a name or a numeric UID -- straight
to the executor's ``wrap``, which resolves it where the job's passwd
database lives. That split is the whole point of the feature, so each
path is covered against a real daemon:

* a **host job** (no ``container:``) runs on the instance itself, so a
  name is resolved against the *instance's* ``/etc/passwd`` by
  ``HostExecutor`` (``getent`` inside the instance) down to a numeric
  UID handed to LXD's instance exec;
* a **container job** runs inside a container, so the name is handed to
  ``<runtime> exec --user`` and resolved against the *container's*
  ``/etc/passwd`` by the runtime.

Both drive the real ``Exec`` RPC over a gRPC channel against a live
daemon. The host path needs only a cached image (no egress); the
container path needs a runtime installed inside the instance plus
registry egress, so it skips where those are unavailable -- exactly
like :mod:`tests.e2e.test_nested_runtime`.

Skipped when no daemon socket is autodetected.
"""

from __future__ import annotations

import contextlib
import tempfile
import time
import uuid
from collections.abc import Iterator
from concurrent import futures
from pathlib import Path
from typing import Any

import pytest

try:
    import grpc
except ModuleNotFoundError:  # pragma: no cover - import guard
    pytest.skip("grpc not installed", allow_module_level=True)

from forgejo_lxd_runner.client import BackendClient, BackendUnavailableError
from forgejo_lxd_runner.executor import (
    ContainerExecutor,
    ExecutorError,
    HostExecutor,
    resolve,
)
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc
from forgejo_lxd_runner.server import BackendPluginService, _Env

_EXAMPLE_PROFILES = Path(__file__).resolve().parents[2] / "examples" / "profiles"

# A cloud-init-capable image, needed only by the container path (the
# runtime is installed at first boot). Mirrors the table in
# tests/e2e/test_nested_runtime.py -- kept local because tests/ is not a
# package on CI, so a cross-module import breaks collection.
_CLOUD_IMAGE_SOURCES = {
    "lxd": {
        "type": "image",
        "protocol": "simplestreams",
        "server": "https://cloud-images.ubuntu.com/minimal/releases/",
        "alias": "24.04",
    },
    "incus": {
        "type": "image",
        "protocol": "simplestreams",
        "server": "https://images.linuxcontainers.org",
        "alias": "ubuntu/24.04/cloud",
    },
}

# A tiny image that ships a shell runs the container job's step. Resolved
# by the container runtime, not pulled by us, so no local alias is needed.
_JOB_CONTAINER_IMAGE = "docker.io/library/busybox:latest"

# Universal passwd entries present on every Linux image, so the
# assertions do not depend on a particular base image: ``nobody`` is the
# canonical unprivileged account, ``daemon`` is UID 1.
_NOBODY = "nobody"
_NOBODY_UID = 65534


@pytest.fixture(scope="module")
def real_client() -> Iterator[BackendClient]:
    try:
        c = BackendClient()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))
    try:
        yield c
    finally:
        c.close()


@pytest.fixture(scope="module")
def plugin_service(real_client: BackendClient) -> BackendPluginService:
    """A live service sharing the fixture's BackendClient."""
    service = BackendPluginService(name="lxd")
    service._client.close()
    service._client = real_client
    return service


@pytest.fixture(scope="module")
def plugin_stub(
    plugin_service: BackendPluginService,
) -> Iterator[plugin_pb2_grpc.BackendPluginStub]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    plugin_pb2_grpc.add_BackendPluginServicer_to_server(plugin_service, server)

    with tempfile.TemporaryDirectory() as tmp:
        sock = Path(tmp) / "plugin.sock"
        server.add_insecure_port(f"unix:{sock}")
        server.start()
        try:
            channel = grpc.insecure_channel(f"unix:{sock}")
            yield plugin_pb2_grpc.BackendPluginStub(channel)
            channel.close()
        finally:
            server.stop(grace=1.0).wait()


@pytest.fixture
def local_image(real_client: BackendClient) -> Iterator[str]:
    """Alias a cached container image for an offline host-path instance.

    Same trick as tests/e2e/test_create_rpc.py: reuse an image already on
    the host under a throwaway alias rather than pull over the network.
    Skips when nothing is cached to alias.
    """
    resp = real_client.request("GET", "/1.0/images?recursion=1")
    images = resp.json().get("metadata") or []
    fingerprint = next(
        (
            rec["fingerprint"]
            for rec in images
            if rec.get("type", "container") == "container" and rec.get("fingerprint")
        ),
        None,
    )
    if not fingerprint:
        pytest.skip("no cached container image on the host to alias")

    alias = f"forgejo-e2e-alias-{uuid.uuid4().hex[:10]}"
    real_client.call("POST", "/1.0/images/aliases", json={"name": alias, "target": fingerprint})
    try:
        yield alias
    finally:
        with contextlib.suppress(Exception):
            real_client.request("DELETE", f"/1.0/images/aliases/{alias}")


def _exec_user(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    env_id: str,
    command: list[str],
    user: str,
) -> tuple[int | None, str]:
    """Drive the Exec RPC as ``user`` and collect ``(exit_code, stdout)``."""
    out = b""
    code: int | None = None
    for frame in plugin_stub.Exec(
        plugin_pb2.ExecRequest(environment_id=env_id, command=command, user=user),
        timeout=60.0,
    ):
        which = frame.WhichOneof("Output")
        if which == "data" and frame.data.stream == plugin_pb2.DataChunk.STDOUT:
            out += frame.data.data
        elif which == "exec_complete":
            code = frame.exec_complete.exit_code
    return code, out.decode().strip()


def test_exec_resolves_a_user_name_for_a_host_job(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    plugin_service: BackendPluginService,
    real_client: BackendClient,
    local_image: str,
) -> None:
    """A host job resolves ``user`` against the instance's passwd.

    No ``container:`` image, so the step runs on the instance and
    ``HostExecutor`` must turn the name into the UID the instance's
    ``/etc/passwd`` gives it. Covers all three input shapes the executor
    distinguishes: a name (resolved via ``getent``), a numeric UID
    (passed straight through), and an unknown name (INVALID_ARGUMENT).
    """
    env_id = f"forgejo-e2e-user-host-{uuid.uuid4().hex[:10]}"
    real_client.launch_instance(
        {"name": env_id, "source": {"type": "image", "alias": local_image}, "start": True},
        timeout=300.0,
    )
    try:
        # Give init a beat to spawn a usable shell, same as the exec
        # tests in test_client.py.
        time.sleep(6)
        with plugin_service._lock:
            plugin_service._envs[env_id] = _Env(
                instance_name=env_id,
                executor=HostExecutor(client=real_client, instance=env_id),
            )

        # Name resolved against the instance's passwd.
        code, out = _exec_user(plugin_stub, env_id, ["id", "-u"], _NOBODY)
        assert code == 0
        assert out == str(_NOBODY_UID)

        # Numeric UID passes straight through -- UID 1 (``daemon``) is on
        # every Linux image.
        code, out = _exec_user(plugin_stub, env_id, ["id", "-u"], "1")
        assert code == 0
        assert out == "1"

        # An unknown name is a bad request, surfaced as INVALID_ARGUMENT.
        with pytest.raises(grpc.RpcError) as excinfo:
            _exec_user(plugin_stub, env_id, ["id", "-u"], f"ghost-{uuid.uuid4().hex[:8]}")
        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    finally:
        with contextlib.suppress(Exception):
            real_client.remove_instance(env_id, timeout=60.0)


def _load_profile(client: BackendClient, stem: str, name: str) -> None:
    """Register ``examples/profiles/<stem>.yaml`` under ``name``."""
    import yaml  # noqa: PLC0415 - test-only dependency

    body: dict[str, Any] = yaml.safe_load((_EXAMPLE_PROFILES / f"{stem}.yaml").read_text())
    client.create_profile(
        name,
        description=body.get("description"),
        config=body.get("config"),
        devices=body.get("devices"),
    )


def _wait_for_cloud_init(client: BackendClient, name: str, timeout: float = 600.0) -> str:
    """Block until cloud-init reaches a terminal state, return it."""
    deadline = time.monotonic() + timeout
    status = "unknown"
    while time.monotonic() < deadline:
        with contextlib.suppress(Exception):
            _, out, _ = client.exec_capture(name, ["cloud-init", "status"])
            status = out.strip().removeprefix("status:").strip()
            if status in {"done", "error", "degraded", "disabled"}:
                return status
        time.sleep(5.0)
    return f"timeout (last: {status})"


def test_exec_resolves_a_user_name_for_a_container_job(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    plugin_service: BackendPluginService,
    real_client: BackendClient,
) -> None:
    """A container job resolves ``user`` against the container's passwd.

    With a ``container:`` image the step runs inside the container, so
    the name goes to ``<runtime> exec --user`` and is resolved against
    the *container's* ``/etc/passwd`` -- a different database from the
    instance's. Needs a runtime inside the instance plus registry
    egress to pull both the runtime packages and the job image, so it
    skips where either is unavailable rather than failing.
    """
    if real_client.flavor not in _CLOUD_IMAGE_SOURCES:
        pytest.skip(f"no cloud image source for flavor {real_client.flavor!r}")

    suffix = uuid.uuid4().hex[:10]
    env_id = f"forgejo-e2e-user-ctr-{suffix}"
    base_name = f"forgejo-e2e-base-{suffix}"
    docker_name = f"forgejo-e2e-docker-{suffix}"
    source = _CLOUD_IMAGE_SOURCES[real_client.flavor]

    _load_profile(real_client, "base", base_name)
    try:
        _load_profile(real_client, "docker", docker_name)
        try:
            try:
                real_client.launch_instance(
                    {
                        "name": env_id,
                        "source": source,
                        "profiles": ["default", base_name, docker_name],
                        "start": True,
                    },
                    timeout=900.0,
                )
            except Exception as exc:  # noqa: BLE001
                pytest.skip(f"cannot boot a cloud instance: {type(exc).__name__}: {exc}")

            status = _wait_for_cloud_init(real_client, env_id)
            if status != "done":
                pytest.skip(f"cloud-init did not install docker cleanly: {status}")

            # Build the real container executor the way Create does, then
            # create + start the job container. Only a *pull* failure is
            # environmental -- no registry egress to fetch the job image --
            # and skips. Anything else (notably a nested container that
            # will not start) is the shipped `base` profile failing to
            # deliver a runnable sandbox: a real defect this test exists to
            # catch, so let it fail.
            executor = resolve(real_client, env_id, _JOB_CONTAINER_IMAGE)
            assert isinstance(executor, ContainerExecutor)
            try:
                executor.create(f"{env_id}-job", workdir="/", mounts=[])
            except ExecutorError as exc:
                text = str(exc)
                if "pull" in text and any(
                    marker in text
                    for marker in ("dial tcp", "TLS handshake", "no such host", "i/o timeout")
                ):
                    pytest.skip(f"job image unreachable from inside the instance: {text[:300]}")
                raise
            executor.start()

            with plugin_service._lock:
                plugin_service._envs[env_id] = _Env(
                    instance_name=env_id,
                    executor=executor,
                    job_image=_JOB_CONTAINER_IMAGE,
                )

            # The name is resolved by the runtime against the container
            # image's own passwd -- busybox ships ``nobody`` at 65534.
            code, out = _exec_user(plugin_stub, env_id, ["id", "-u"], _NOBODY)
            assert code == 0, f"exec as {_NOBODY!r} failed: rc={code} out={out!r}"
            assert out == str(_NOBODY_UID)
        finally:
            with contextlib.suppress(Exception):
                real_client.remove_instance(env_id, timeout=120.0)
            with contextlib.suppress(Exception):
                real_client.remove_profile(docker_name)
    finally:
        with contextlib.suppress(Exception):
            real_client.remove_profile(base_name)
