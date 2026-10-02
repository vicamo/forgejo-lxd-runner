"""End-to-end coverage for the nested container-runtime profiles.

``examples/profiles/{docker,podman}.yaml`` install a container runtime
inside the LXD instance at first boot via cloud-init. That needs three
things the repository cannot guarantee on an arbitrary machine:

* a daemon to talk to (skipped by the shared fixtures when absent),
* working egress from *inside the instance* to the distribution's
  package mirrors,
* patience — a cold boot plus package install runs into minutes.

The second one is the interesting failure. A host firewall that drops
traffic forwarded off the LXD bridge leaves the instance with DNS but no
route to the mirrors; Docker on the host installs exactly such a rule
(``FORWARD`` policy ``DROP``), so a developer machine with Docker on it
fails these tests for reasons that have nothing to do with this code.
Rather than reporting that as a bug in the profiles, `_require_egress`
probes reachability from inside a throwaway instance and skips.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from forgejo_lxd_runner.client import BackendClient, BackendUnavailableError

_EXAMPLE_PROFILES = Path(__file__).resolve().parents[2] / "examples" / "profiles"

# cloud-init is the whole mechanism under test, so these have to be
# cloud-enabled images — the alpine image the other e2e modules use for
# speed has no cloud-init at all, and on the linuxcontainers server the
# plain `ubuntu/24.04` variant doesn't either. Hence `/cloud`.
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


def _cloud_image_source(client: BackendClient) -> dict[str, str]:
    """Pick a cloud-init-capable image for the daemon under test."""
    try:
        return _CLOUD_IMAGE_SOURCES[client.flavor]
    except KeyError:
        pytest.skip(f"no cloud image source registered for flavor {client.flavor!r}")


# Cold boot + apt update + package install. Generous because a cold
# mirror and a slow disk both land here.
_CLOUD_INIT_TIMEOUT = 600.0


class NestedRuntime:
    """Command access to the inside of an instance.

    ``exec_stream`` yields frames, which is the right shape for the Exec
    RPC but a poor one for assertions. This collapses a run into
    ``(rc, stdout, stderr)`` and adds the two questions these tests
    actually ask: has cloud-init settled, and is the runtime usable.
    """

    def __init__(self, client: BackendClient, name: str) -> None:
        self.client = client
        self.name = name

    def run(self, *command: str) -> tuple[int, str, str]:
        """Run ``command`` and return ``(exit_code, stdout, stderr)``."""
        out: list[bytes] = []
        err: list[bytes] = []
        rc = -1
        for kind, data in self.client.exec_stream(self.name, list(command)):
            if kind == "stdout":
                out.append(data)
            elif kind == "stderr":
                err.append(data)
            elif kind == "exit":
                rc = int(data)
        return (
            rc,
            b"".join(out).decode(errors="replace"),
            b"".join(err).decode(errors="replace"),
        )

    def sh(self, script: str) -> tuple[int, str, str]:
        """Run ``script`` through ``sh -c``."""
        return self.run("sh", "-c", script)

    def wait_for_cloud_init(self, timeout: float = _CLOUD_INIT_TIMEOUT) -> str:
        """Block until cloud-init reaches a terminal state, return it.

        Polls rather than using ``cloud-init status --wait`` because the
        exec channel is not available for the first few seconds of boot,
        and ``--wait`` inside a not-yet-ready instance simply fails.
        """
        deadline = time.monotonic() + timeout
        status = "unknown"
        while time.monotonic() < deadline:
            with contextlib.suppress(Exception):
                _, out, _ = self.run("cloud-init", "status")
                status = out.strip().removeprefix("status:").strip()
                if status in {"done", "error", "degraded", "disabled"}:
                    return status
            time.sleep(5.0)
        return f"timeout (last: {status})"

    def diagnostics(self) -> str:
        """Cloud-init's own account of what went wrong, for assertions."""
        _, out, _ = self.sh(
            "cloud-init status --long 2>&1 | head -40; "
            "echo '--- cloud-init-output.log ---'; "
            "tail -40 /var/log/cloud-init-output.log 2>&1"
        )
        return out


def _load_profile(client: BackendClient, stem: str, name: str) -> None:
    """Register ``examples/profiles/<stem>.yaml`` under ``name``."""
    import yaml  # noqa: PLC0415 — test-only dependency

    body: dict[str, Any] = yaml.safe_load((_EXAMPLE_PROFILES / f"{stem}.yaml").read_text())
    client.create_profile(
        name,
        description=body.get("description"),
        config=body.get("config"),
        devices=body.get("devices"),
    )


@pytest.fixture(scope="module")
def client() -> Iterator[BackendClient]:
    try:
        c = BackendClient()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))
    with c:
        yield c


@pytest.fixture(scope="module")
def _require_egress(client: BackendClient) -> None:
    """Skip the module unless an instance can reach the package mirrors.

    Launches a bare instance — no profile, nothing to install — and asks
    it to fetch a release index. Cheap relative to the tests it guards,
    and it distinguishes "the profiles are broken" from "this host does
    not let instances talk to the internet".
    """
    name = f"forgejo-e2e-egress-{uuid.uuid4().hex[:10]}"
    try:
        try:
            client.launch_instance(
                {"name": name, "source": _cloud_image_source(client), "profiles": ["default"]},
                timeout=900.0,
            )
            client.set_instance_state(name, "start", timeout=300.0)
        except Exception as exc:
            # Most often the image server is unreachable. Skipping keeps
            # that distinct from a profile defect, and reports the real
            # error rather than the 404 a later call would raise against
            # the instance that was never created.
            pytest.skip(f"cannot boot a probe instance: {type(exc).__name__}: {exc}")
        runtime = NestedRuntime(client, name)

        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            with contextlib.suppress(Exception):
                rc, _, _ = runtime.run("true")
                if rc == 0:
                    break
            time.sleep(3.0)

        rc, out, _ = runtime.sh(
            "timeout 30 curl -4 -sS -o /dev/null -w '%{http_code}' "
            "http://archive.ubuntu.com/ubuntu/dists/noble/InRelease 2>&1 "
            "|| echo UNREACHABLE"
        )
        if "200" not in out:
            pytest.skip(
                "instances cannot reach the package mirrors "
                f"(probe said {out.strip()!r}); a host firewall dropping "
                "traffic forwarded off the LXD bridge is the usual cause"
            )
    finally:
        with contextlib.suppress(Exception):
            client.remove_instance(name, timeout=120.0)


@pytest.fixture
def nested(client: BackendClient, request: pytest.FixtureRequest) -> Iterator[NestedRuntime]:
    """Boot an instance with ``base`` plus the runtime profile under test.

    Parametrised with the profile stem (``docker`` / ``podman``). Both
    profiles are registered under throwaway names so a developer's own
    ``base`` / ``docker`` profiles are never touched.
    """
    stem: str = request.param
    suffix = uuid.uuid4().hex[:10]
    base_name = f"forgejo-e2e-base-{suffix}"
    runtime_name = f"forgejo-e2e-{stem}-{suffix}"
    instance = f"forgejo-e2e-nested-{suffix}"

    _load_profile(client, "base", base_name)
    try:
        _load_profile(client, stem, runtime_name)
        try:
            client.launch_instance(
                {
                    "name": instance,
                    "source": _cloud_image_source(client),
                    "profiles": ["default", base_name, runtime_name],
                },
                timeout=900.0,
            )
            client.set_instance_state(instance, "start", timeout=300.0)
            try:
                yield NestedRuntime(client, instance)
            finally:
                with contextlib.suppress(Exception):
                    client.remove_instance(instance, timeout=120.0)
        finally:
            with contextlib.suppress(Exception):
                client.remove_profile(runtime_name)
    finally:
        with contextlib.suppress(Exception):
            client.remove_profile(base_name)


@pytest.mark.parametrize("nested", ["docker"], indirect=True)
def test_docker_profile_brings_up_a_working_daemon(
    _require_egress: None,
    nested: NestedRuntime,
) -> None:
    """``base,docker`` yields an instance running a usable docker daemon.

    Asserts against the daemon rather than the binary: ``docker
    version --format {{.Server.Version}}`` only answers once dockerd is
    up and the socket is accepting connections, which is the property
    the profile actually promises.
    """
    status = nested.wait_for_cloud_init()
    assert status == "done", f"cloud-init did not finish cleanly: {nested.diagnostics()}"

    rc, out, err = nested.run("docker", "version", "--format", "{{.Server.Version}}")
    assert rc == 0, f"docker daemon not reachable: rc={rc} out={out!r} err={err!r}"
    assert out.strip(), "docker reported an empty server version"

    rc, out, _ = nested.sh("systemctl is-active docker")
    assert out.strip() == "active", f"docker.service is {out.strip()!r}"


@pytest.mark.parametrize("nested", ["podman"], indirect=True)
def test_podman_profile_installs_a_usable_runtime(
    _require_egress: None,
    nested: NestedRuntime,
) -> None:
    """``base,podman`` yields an instance with a usable podman.

    There is no daemon to check — podman is fork/exec — so ``podman
    info`` exiting zero is the readiness signal: it initialises storage
    and the runtime config, which is where a missing nesting knob would
    surface.
    """
    status = nested.wait_for_cloud_init()
    assert status == "done", f"cloud-init did not finish cleanly: {nested.diagnostics()}"

    rc, out, err = nested.run("podman", "info", "--format", "{{.Host.OCIRuntime.Name}}")
    assert rc == 0, f"podman not usable: rc={rc} out={out!r} err={err!r}"
    assert out.strip(), "podman reported an empty OCI runtime name"


@pytest.mark.parametrize("nested", ["docker"], indirect=True)
def test_docker_profile_can_run_a_container(
    _require_egress: None,
    nested: NestedRuntime,
) -> None:
    """The nested daemon can actually pull and run an image.

    Distinct from the daemon check above: a daemon can be up while
    nesting is still too restricted to start a container. Pulling from
    Docker Hub additionally needs registry egress, so an image-pull
    failure skips rather than fails — the mirror probe in
    ``_require_egress`` says nothing about Docker Hub.
    """
    status = nested.wait_for_cloud_init()
    assert status == "done", f"cloud-init did not finish cleanly: {nested.diagnostics()}"

    rc, out, err = nested.sh("timeout 300 docker run --rm hello-world 2>&1")
    combined = f"{out}{err}"
    if rc != 0 and (
        "dial tcp" in combined or "TLS handshake" in combined or "no such host" in combined
    ):
        pytest.skip(f"registry unreachable from inside the instance: {combined.strip()[:300]}")

    assert rc == 0, f"docker run failed: rc={rc} output={combined!r}"
    assert "Hello from Docker!" in combined
