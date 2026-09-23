"""End-to-end smoke test against a real local LXD.

Skipped unless ``FORGEJO_LXD_E2E=1`` is set — this touches the host and
requires the caller to be able to talk to the LXD socket.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pylxd
import pytest

from forgejo_lxd_runner.proto.plugin.v1alpha import plugin_pb2
from forgejo_lxd_runner.server import BackendPluginService, _lxd_arch_to_gha

pytestmark = pytest.mark.skipif(
    os.environ.get("FORGEJO_LXD_E2E") != "1",
    reason="set FORGEJO_LXD_E2E=1 to run against a real LXD",
)

# Local alias the fixture installs, used verbatim as the plugin's
# ``label_arg``. The MVP Create looks up images only in the local LXD image
# store; remote-image support is a later commit.
E2E_IMAGE_ALIAS = "forgejo-lxd-runner-e2e"


class _Ctx:
    def abort(self, code: object, details: object) -> None:
        raise AssertionError(f"aborted: {code} {details}")


@pytest.fixture(scope="module")
def e2e_image() -> Iterator[str]:
    """Ensure a small Ubuntu image is available under E2E_IMAGE_ALIAS.

    Pulled from the ``ubuntu-minimal`` simplestreams remote the first time;
    subsequent runs reuse the cached copy. Not removed on teardown so a
    developer iterating locally doesn't re-download between runs.
    """

    client = pylxd.Client()
    if not client.images.exists(E2E_IMAGE_ALIAS, alias=True):
        image = client.images.create_from_simplestreams(
            "https://cloud-images.ubuntu.com/minimal/releases/",
            "24.04",
        )
        image.add_alias(E2E_IMAGE_ALIAS, "forgejo-lxd-runner e2e smoke test")
    yield E2E_IMAGE_ALIAS


def test_full_lifecycle_on_real_lxd(e2e_image: str) -> None:
    service = BackendPluginService()
    name = f"forgejo-lxd-runner-e2e-{uuid.uuid4().hex[:8]}"
    ctx = _Ctx()
    try:
        service.Create(
            plugin_pb2.CreateRequest(name=name, label_arg=e2e_image),
            ctx,
        )
        list(service.Start(plugin_pb2.StartRequest(environment_id=name), ctx))

        outs = list(
            service.Exec(
                plugin_pb2.ExecRequest(environment_id=name, command=["/bin/true"]),
                ctx,
            )
        )
        assert outs[-1].exec_complete.exit_code == 0
    finally:
        service.Remove(plugin_pb2.RemoveRequest(environment_id=name), ctx)


@pytest.mark.parametrize(
    "lxd_arch",
    ["x86_64", "aarch64", "armv7l", "s390x", "ppc64le", "riscv64"],
)
def test_lxd_arch_backend_option(lxd_arch: str) -> None:
    """Explicit ``lxd_arch`` reaches LXD and round-trips through ``arch``.

    Non-host architectures are skipped: LXD refuses to launch a container of
    a different arch than the host kernel unless qemu-user is wired in, and
    that's out of scope for a smoke test.
    """
    host_arch = os.uname().machine
    if lxd_arch != host_arch:
        pytest.skip(f"host is {host_arch}, cannot launch {lxd_arch} container")

    service = BackendPluginService()
    name = f"forgejo-lxd-runner-e2e-{uuid.uuid4().hex[:8]}"
    ctx = _Ctx()
    try:
        resp = service.Create(
            plugin_pb2.CreateRequest(
                name=name,
                label_arg="ubuntu-minimal/24.04",
                backend_options={"lxd_arch": lxd_arch},
            ),
            ctx,
        )
        assert resp.arch == _lxd_arch_to_gha(lxd_arch)
    finally:
        service.Remove(plugin_pb2.RemoveRequest(environment_id=name), ctx)
