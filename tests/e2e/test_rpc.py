"""RPC-level end-to-end tests: real gRPC + real LXD/Incus daemon.

Complements :mod:`tests.e2e.test_client` (real daemon, direct client
API) and :mod:`tests.grpc.test_lifecycle` (real gRPC, mocked
BackendClient) by exercising the full stack — a gRPC channel over a
unix socket driving a real ``BackendPluginService`` that talks to a
real daemon. This is the layer that catches integration bugs neither
of the other tiers can see.

The server's ``Create`` currently hardcodes ``source={"type":"image",
"alias":label_arg}`` and has no way to accept the ``{protocol, server,
alias}`` dict our per-flavour tiny-image picker produces — so this
file pre-launches the instance via the client fixture and injects it
into ``_envs`` before driving Start / Remove through gRPC. Once
``Create``'s label parser grows a full image-reference syntax the
pre-launch step can be replaced with a real Create RPC.

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

import grpc
import pytest

from forgejo_lxd_runner.client import BackendClient, BackendUnavailableError
from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc
from forgejo_lxd_runner.server import BackendPluginService, _Env

# Kept in sync with the same table in ``tests/e2e/test_client.py``. Local
# copy rather than a cross-file import because ``tests/`` isn't a package
# on CI (no ``__init__.py``, no ``sys.path`` entry) — importing across
# test modules breaks collection.
_TINY_IMAGE_SOURCES = {
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
        "alias": "alpine/edge",
    },
}


def _tiny_image_source(client: BackendClient) -> dict[str, str]:
    try:
        return _TINY_IMAGE_SOURCES[client.flavor]
    except KeyError:
        pytest.skip(f"no tiny image source registered for flavor {client.flavor!r}")


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

    Sharing matters: tests reach into ``_envs`` to inject state, and
    the service must talk to the same daemon connection the tests use
    for setup / verification.
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


def test_start_and_remove_against_real_daemon(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    plugin_service: BackendPluginService,
    real_client: BackendClient,
) -> None:
    """Drive Start → Remove over gRPC against a live daemon.

    Verifies Start's stream terminates with ``start_complete``, Remove
    tears the instance down, and a follow-up state read 404s.
    """
    source = _tiny_image_source(real_client)
    env_id = f"forgejo-e2e-rpc-{uuid.uuid4().hex[:10]}"

    # Bring the instance into being through the client — see module
    # docstring for why we don't go through Create over gRPC yet.
    # Launched already running, exactly as Create would: Start confirms
    # a booted instance rather than booting one.
    real_client.launch_instance({"name": env_id, "source": source, "start": True}, timeout=180.0)
    with plugin_service._lock:
        plugin_service._envs[env_id] = _Env(instance_name=env_id)

    try:
        frames = list(
            plugin_stub.Start(
                plugin_pb2.StartRequest(environment_id=env_id),
                timeout=120.0,
            )
        )
        assert frames, "Start returned no frames"
        assert frames[-1].WhichOneof("Output") == "start_complete"

        # Give init a moment before Remove — same reason as the exec
        # tests, some images finish "Running" before a working shell.
        time.sleep(2)

        plugin_stub.Remove(
            plugin_pb2.RemoveRequest(environment_id=env_id),
            timeout=120.0,
        )

        # After Remove the instance is gone; the plugin's own map is empty.
        assert env_id not in plugin_service._envs
        resp = real_client.request("GET", f"/1.0/instances/{env_id}")
        assert resp.status_code == 404
    finally:
        # Backstop cleanup: if any assertion failed before Remove, do
        # it directly through the client so we don't leak an instance.
        # remove_instance already tolerates "already gone", which is the
        # normal case once the Remove above succeeded.
        with contextlib.suppress(Exception):
            real_client.remove_instance(env_id, timeout=60.0)


def test_remove_is_idempotent_over_grpc(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
) -> None:
    """Remove on an unknown environment_id returns cleanly.

    Exercises the "env is None" branch through the full gRPC stack.
    """
    plugin_stub.Remove(
        plugin_pb2.RemoveRequest(environment_id=f"never-created-{uuid.uuid4().hex[:8]}"),
        timeout=10.0,
    )


def test_remove_after_manual_delete_over_grpc(
    plugin_stub: plugin_pb2_grpc.BackendPluginStub,
    plugin_service: BackendPluginService,
    real_client: BackendClient,
) -> None:
    """Remove tolerates the instance vanishing under it.

    Injects an env record whose instance was never created — the
    daemon 404s on the state probe, and Remove must still return
    cleanly. Exercises remove_instance's state-probe 404 branch
    through the full gRPC stack.
    """
    env_id = f"forgejo-e2e-rpc-ghost-{uuid.uuid4().hex[:8]}"
    with plugin_service._lock:
        plugin_service._envs[env_id] = _Env(instance_name=env_id)

    plugin_stub.Remove(
        plugin_pb2.RemoveRequest(environment_id=env_id),
        timeout=10.0,
    )
    assert env_id not in plugin_service._envs
