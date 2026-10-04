"""RPC-level end-to-end tests for ``CreateResponse`` metadata fields.

Complements :mod:`tests.e2e.test_rpc` (which drives Start / Remove but
pre-launches the instance and injects it into ``_envs``) by exercising a
*real* ``Create`` RPC end to end: a gRPC channel over a unix socket
driving a real ``BackendPluginService`` that talks to a real daemon,
all the way to the ``CreateResponse`` the runner reads back.

``Create`` resolves its image from ``label_arg`` as
``source={"type": "image", "alias": label_arg}`` -- a *local* alias on
the daemon. The other e2e modules pull remote simplestreams images;
here we instead alias an image already cached on the host, so the test
needs no network egress and no image pull. The module skips when the
host has no cached container image to alias.

This is the only test that observes ``CreateResponse.arch`` over the
full stack -- every other tier reads the raw instance record or mocks
the client.

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
    """A live service sharing the fixture's BackendClient.

    Sharing matters: the test aliases an image and verifies cleanup
    through the same daemon connection the service drives.
    """
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
    """Alias a cached container image and yield the alias name.

    ``Create`` resolves ``label_arg`` as a local image alias, so a real
    Create needs one. Rather than pull over the network, we reuse an
    image already on the host: pick any cached container image and give
    it a throwaway alias, removed on teardown. Skips when the host has
    nothing cached to alias -- there is no offline way to get an image.
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


@contextlib.contextmanager
def _created(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    alias: str,
) -> Iterator[plugin_pb2.CreateResponse]:
    """Drive a real Create RPC, yield its response, always Remove after.

    The instance really boots -- Create launches it with ``start: true``
    and probes its record -- so teardown goes through the Remove RPC,
    with a direct client delete as a backstop if an assertion fails
    before Remove runs.
    """
    env_id = f"forgejo-e2e-create-{uuid.uuid4().hex[:10]}"
    resp = plugin_stub.Create(
        plugin_pb2.CreateRequest(name=env_id, label_arg=alias),
        timeout=300.0,
    )
    try:
        yield resp
        plugin_stub.Remove(
            plugin_pb2.RemoveRequest(environment_id=env_id),
            timeout=120.0,
        )
    finally:
        with contextlib.suppress(Exception):
            real_client.remove_instance(env_id, timeout=60.0)


# GHA's RUNNER_ARCH vocabulary (the .NET Architecture enum tokens the
# server maps LXD architectures onto). A real host reports one of these.
_GHA_ARCH = {"X86", "X64", "ARM", "ARM64"}


def test_create_reports_architecture(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    real_client: BackendClient,
    local_image: str,
) -> None:
    """Create populates CreateResponse.arch from the instance record.

    The daemon stamps the booted instance with the host's architecture;
    the server maps it to GHA's RUNNER_ARCH token. We assert membership
    in that vocabulary rather than a specific value so the test is
    portable across build hosts (x86_64, aarch64, ...).
    """
    with _created(plugin_stub, real_client, local_image) as resp:
        assert resp.arch in _GHA_ARCH, f"unexpected RUNNER_ARCH token {resp.arch!r}"
