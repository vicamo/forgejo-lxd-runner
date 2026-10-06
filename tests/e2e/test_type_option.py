"""RPC-level end-to-end tests for the ``type`` backend option.

``Create`` reads ``backend_options["type"]`` and forwards it verbatim to
LXD as ``config["type"]`` -- ``container`` (LXD's default) or
``virtual-machine``. The effect is only visible on the daemon side: the
instance the daemon actually builds is one or the other. These tests
drive a *real* ``Create`` RPC (gRPC over a unix socket into a live
``BackendPluginService`` talking to a real daemon) and then read the
instance record back to assert its ``type``.

Like :mod:`tests.e2e.test_create_rpc`, ``Create`` resolves ``label_arg``
as a *local* image alias, so each case aliases an image already cached
on the host rather than pulling over the network:

* the **default** case needs a cached *container* image and asserts the
  built instance is a container even though no ``type`` was requested;
* the **virtual-machine** case needs a VM image plus host KVM. A VM
  image is rarely cached, so rather than skip, the fixture pulls a
  minimal one from simplestreams (idempotent -- a rerun reuses the
  cache) and aliases that. It still skips without ``/dev/kvm``, since
  that is a host capability no pull can supply.

Skipped when no daemon socket is autodetected.
"""

from __future__ import annotations

import contextlib
import tempfile
import uuid
from collections.abc import Iterator
from concurrent import futures
from pathlib import Path

import grpc
import pytest

from forgejo_lxd_runner.client import BackendClient, BackendUnavailableError
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc
from forgejo_lxd_runner.server import BackendPluginService

# Simplestreams sources for a minimal *virtual-machine* image, one per
# daemon flavour. Mirrors the container sources the other e2e modules
# use, but asks for the VM variant via ``image_type`` so the daemon
# fetches a disk image rather than a rootfs tarball. Canonical's
# ubuntu-minimal remote for LXD; the community images server for Incus.
_VM_IMAGE_SOURCES = {
    "lxd": {
        "type": "image",
        "mode": "pull",
        "protocol": "simplestreams",
        "server": "https://cloud-images.ubuntu.com/minimal/releases/",
        "alias": "24.04",
        "image_type": "virtual-machine",
    },
    "incus": {
        "type": "image",
        "mode": "pull",
        "protocol": "simplestreams",
        "server": "https://images.linuxcontainers.org",
        "alias": "ubuntu/24.04/cloud",
        "image_type": "virtual-machine",
    },
}


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


def _alias_cached_image(real_client: BackendClient, instance_type: str) -> Iterator[str]:
    """Alias a cached image of ``instance_type``; skip when none is cached.

    ``Create`` resolves ``label_arg`` as a local alias, so a real Create
    needs one. Rather than pull, reuse an image already on the host whose
    ``type`` (``container`` / ``virtual-machine``) matches what the test
    means to build. The alias is thrown away on teardown.
    """
    resp = real_client.request("GET", "/1.0/images?recursion=1")
    images = resp.json().get("metadata") or []
    fingerprint = next(
        (
            rec["fingerprint"]
            for rec in images
            if rec.get("type", "container") == instance_type and rec.get("fingerprint")
        ),
        None,
    )
    if not fingerprint:
        pytest.skip(f"no cached {instance_type} image on the host to alias")

    alias = f"forgejo-e2e-alias-{uuid.uuid4().hex[:10]}"
    real_client.call("POST", "/1.0/images/aliases", json={"name": alias, "target": fingerprint})
    try:
        yield alias
    finally:
        with contextlib.suppress(Exception):
            real_client.request("DELETE", f"/1.0/images/aliases/{alias}")


@pytest.fixture
def container_image(real_client: BackendClient) -> Iterator[str]:
    yield from _alias_cached_image(real_client, "container")


@pytest.fixture
def vm_image(real_client: BackendClient) -> Iterator[str]:
    """Alias a minimal virtual-machine image, pulling it if need be.

    ``Create`` resolves ``label_arg`` as a local alias, so a VM image
    must be present on the host. Unlike container images, a VM image is
    rarely pre-cached, so rather than skip we pull a minimal one from
    simplestreams. The daemon no-ops the pull when the image is already
    cached (keyed by fingerprint), so reruns are cheap. The cached image
    is left in place for reuse; only the throwaway alias is removed.

    ``/dev/kvm`` is still required -- it is a host capability no pull can
    provide -- so the case skips without it.
    """
    if not Path("/dev/kvm").exists():
        pytest.skip("host has no /dev/kvm, cannot build a virtual-machine instance")
    try:
        source = _VM_IMAGE_SOURCES[real_client.flavor]
    except KeyError:
        pytest.skip(f"no virtual-machine image source registered for flavor {real_client.flavor!r}")

    op = real_client.run_operation("POST", "/1.0/images", json={"source": source}, timeout=900.0)
    fingerprint = (op.get("metadata") or {}).get("fingerprint")
    if not fingerprint:
        pytest.skip("virtual-machine image pull returned no fingerprint")

    alias = f"forgejo-e2e-alias-{uuid.uuid4().hex[:10]}"
    real_client.call("POST", "/1.0/images/aliases", json={"name": alias, "target": fingerprint})
    try:
        yield alias
    finally:
        with contextlib.suppress(Exception):
            real_client.request("DELETE", f"/1.0/images/aliases/{alias}")


@contextlib.contextmanager
def _created(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    alias: str,
    backend_options: dict[str, str],
) -> Iterator[str]:
    """Drive a real Create RPC with ``backend_options``; yield env id.

    The instance really boots -- Create launches it with ``start: true``
    -- so teardown goes through the Remove RPC, with a direct client
    delete as a backstop if an assertion fails before Remove runs.
    """
    env_id = f"forgejo-e2e-type-{uuid.uuid4().hex[:10]}"
    plugin_stub.Create(
        plugin_pb2.CreateRequest(name=env_id, label_arg=alias, backend_options=backend_options),
        timeout=600.0,
    )
    try:
        yield env_id
        plugin_stub.Remove(
            plugin_pb2.RemoveRequest(environment_id=env_id),
            timeout=120.0,
        )
    finally:
        with contextlib.suppress(Exception):
            real_client.remove_instance(env_id, timeout=120.0)


def test_create_defaults_to_a_container_without_type(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    container_image: str,
) -> None:
    """No ``type`` option -> the daemon builds a container.

    The server omits ``config["type"]`` when the option is absent, so
    LXD applies its own default. Asserting the built instance's recorded
    type is a container proves the omission reaches the daemon as an
    actual container, not just a missing key.
    """
    with _created(plugin_stub, real_client, container_image, {}) as env_id:
        record = real_client.get_instance(env_id)
        assert record.get("type") == "container"


def test_create_honours_type_virtual_machine(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    vm_image: str,
) -> None:
    """``type: virtual-machine`` -> the daemon builds a VM.

    The option is forwarded verbatim to ``config["type"]``; the only way
    to prove it took effect is that the daemon actually built a VM, which
    the instance record reports as ``type == "virtual-machine"``.
    """
    with _created(plugin_stub, real_client, vm_image, {"type": "virtual-machine"}) as env_id:
        record = real_client.get_instance(env_id)
        assert record.get("type") == "virtual-machine"


def _exec(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    env_id: str,
    command: list[str],
) -> tuple[int | None, str]:
    """Drive the Exec RPC and collect ``(exit_code, stdout)``."""
    out = b""
    code: int | None = None
    for frame in plugin_stub.Exec(
        plugin_pb2.ExecRequest(environment_id=env_id, command=command),
        timeout=60.0,
    ):
        which = frame.WhichOneof("Output")
        if which == "data" and frame.data.stream == plugin_pb2.DataChunk.STDOUT:
            out += frame.data.data
        elif which == "exec_complete":
            code = frame.exec_complete.exit_code
    return code, out.decode().strip()


def test_create_leaves_a_virtual_machine_immediately_exec_able(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    vm_image: str,
) -> None:
    """``Create`` on a VM must return only once the guest agent is ready.

    A virtual-machine reports Running the instant the hypervisor starts,
    but its exec endpoint is served by the agent inside the guest, which
    is not connected until the guest OS boots -- the daemon answers exec
    with 404 until then. ``Create`` waits out that gap, so an ``Exec``
    fired the moment ``Create`` returns must already reach the guest.
    This is the server behavior that makes a VM runner usable at all; a
    container has no such gap, so the VM is the case that proves it.
    """
    with _created(plugin_stub, real_client, vm_image, {"type": "virtual-machine"}) as env_id:
        code, out = _exec(plugin_stub, env_id, ["sh", "-c", "echo agent-ready"])
        assert code == 0
        assert out == "agent-ready"


def test_vm_exec_reports_missing_command_as_exit_127_not_an_error(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    vm_image: str,
) -> None:
    """A not-found command on a VM comes back as exit 127, never an error.

    This is the regression guard for the LXD-6.x virtual-machine quirk:
    its QEMU exec driver does not record a guest exit of 127/126 as the
    operation's ``return``; it fails the exec *operation* with a fixed
    message (``Command not found`` / ``Command not executable``). A
    client that waits on that operation would see a
    ``BackendOperationError`` where every other driver yields a plain
    exit code.

    ``exec_capture`` resolves both back to the exit code, so a non-zero
    exit stays a command *result* on every driver and daemon version.
    This matters because ``detect_runtime`` probes with
    ``sh -c "command -v docker"`` and reads exit 127 as "runtime
    absent"; if that raised, ``Create`` on a docker-less VM would abort
    with ``job container: Command not found`` instead of falling back to
    the host executor.

    A VM is required: the container driver has always reported these as a
    plain ``return``, so only the VM path exercises the conversion. The
    assertion is a real witness -- revert the mapping in
    ``BackendClient._exec_return_code`` and this fails with a raised
    ``BackendOperationError``.
    """
    with _created(plugin_stub, real_client, vm_image, {"type": "virtual-machine"}) as env_id:
        # A command the guest's PATH cannot resolve: exit 127.
        rc, _out, _err = real_client.exec_capture(
            env_id, ["this-command-does-not-exist-forgejo-e2e"]
        )
        assert rc == 127

        # The real runtime probe takes the same path on a VM with no
        # docker installed, and must likewise come back as 127.
        rc, _out, _err = real_client.exec_capture(env_id, ["sh", "-c", "command -v docker"])
        assert rc == 127


def test_vm_exec_reports_non_executable_as_exit_126_not_an_error(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    vm_image: str,
) -> None:
    """A non-executable target on a VM comes back as exit 126, never an error.

    The sibling of the 127 case: LXD 6.x's QEMU exec driver maps a guest
    exit of 126 to ``Command not executable`` and fails the operation,
    where the container driver records it as a plain ``return``. Plant a
    file without the execute bit and exec it directly; ``exec_capture``
    must yield 126 rather than raising ``BackendOperationError``.
    """
    with _created(plugin_stub, real_client, vm_image, {"type": "virtual-machine"}) as env_id:
        target = "/tmp/forgejo-e2e-nonexec"
        rc, _out, _err = real_client.exec_capture(
            env_id,
            ["sh", "-c", f"printf '#!/bin/sh\\necho hi\\n' > {target} && chmod 0644 {target}"],
        )
        assert rc == 0

        rc, _out, _err = real_client.exec_capture(env_id, [target])
        assert rc == 126


def test_container_exec_reports_missing_command_as_exit_127(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    container_image: str,
) -> None:
    """The container driver reports a missing command as exit 127 too.

    The container path never hit the VM-only operation-error conversion,
    but pinning it here makes the cross-driver invariant explicit: a
    not-found command is exit 127 on *every* driver, so a future change
    to the exit-code resolution cannot silently diverge the two.
    """
    with _created(plugin_stub, real_client, container_image, {}) as env_id:
        rc, _out, _err = real_client.exec_capture(
            env_id, ["this-command-does-not-exist-forgejo-e2e"]
        )
        assert rc == 127
