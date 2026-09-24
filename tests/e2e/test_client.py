"""End-to-end tests for :mod:`forgejo_lxd_runner.client`.

These require a live LXD or Incus daemon on the host and are skipped
otherwise. The socket is intentionally *not* passed in — autodetect is
part of what we're exercising. The CI matrix sets
``FORGEJO_LXD_RUNNER_E2E_BACKEND`` to ``lxd`` or ``incus`` so we can
assert autodetect landed on the intended daemon.
"""

from __future__ import annotations

import contextlib
import os
import time
import uuid
from collections.abc import Iterator

import httpx
import pytest

from forgejo_lxd_runner.client import (
    BackendClient,
    BackendOperationError,
    BackendUnavailableError,
)


@pytest.fixture(scope="module")
def client() -> Iterator[BackendClient]:
    """A BackendClient bound to whatever daemon autodetect finds.

    Skips the whole module when no socket is reachable, so this file is
    a no-op on developer machines without LXD/Incus installed.
    """
    try:
        c = BackendClient()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))
    try:
        yield c
    finally:
        c.close()


def test_autodetect_selects_expected_backend(client: BackendClient) -> None:
    """The socket autodetect list must land on the daemon CI installed.

    ``FORGEJO_LXD_RUNNER_E2E_BACKEND`` is set by the workflow to the
    matrix flavour (``lxd`` / ``incus``); when unset (local dev) we just
    check that *some* socket got picked.
    """
    assert client.socket_path  # something was picked

    expected = os.environ.get("FORGEJO_LXD_RUNNER_E2E_BACKEND")
    if not expected:
        pytest.skip("FORGEJO_LXD_RUNNER_E2E_BACKEND not set")

    # The autodetect table only contains one socket per flavour, so a
    # substring match is enough to distinguish them.
    assert expected in client.socket_path, (
        f"autodetect picked {client.socket_path!r}, expected a {expected} socket"
    )


def test_root_endpoint_reports_server_environment(client: BackendClient) -> None:
    """server_info / flavor expose the daemon's /1.0 self-description.

    Both attributes are lazy: ``server_info`` fetches ``/1.0`` on first
    access and caches the dict, and ``flavor`` derives from that cache.
    A second read of ``server_info`` must return the exact same object
    to prove the fetch didn't repeat.
    """
    meta = client.server_info

    # api_status / api_version are documented as always present on both
    # LXD and Incus.
    assert meta.get("api_status") in {"stable", "development"}
    assert meta.get("api_version")

    # Cache identity: a second access returns the same object.
    assert client.server_info is meta

    assert client.flavor in {"lxd", "incus"}
    expected = os.environ.get("FORGEJO_LXD_RUNNER_E2E_BACKEND")
    if expected:
        assert client.flavor == expected


def test_supports_matches_advertised_api_extensions(client: BackendClient) -> None:
    """supports() returns True for advertised extensions, False otherwise.

    We don't hard-code an extension name here — the set drifts across
    daemon versions, and the point of ``supports()`` is precisely to
    ask the daemon rather than assume. Instead we pick one from the
    daemon's own list and assert both directions: a real name is True,
    a bogus one is False.
    """
    exts = client.server_info.get("api_extensions") or []
    assert exts, "daemon advertised no api_extensions — cannot verify supports()"

    # Pick any real extension; supports() must agree.
    assert client.supports(exts[0])

    # A name the daemon can't possibly advertise: False.
    assert not client.supports("forgejo-lxd-runner-nonexistent-extension")


def test_operations_listing_is_reachable(client: BackendClient) -> None:
    """GET /1.0/operations exercises call() against a real listing endpoint.

    The daemon returns the URLs of any in-flight operations; on an idle
    CI daemon this is usually empty, but the *response envelope* is what
    we're verifying: call() must unwrap ``metadata`` cleanly and return
    the dict shape without blowing up on an empty payload.
    """
    meta = client.call("GET", "/1.0/operations")
    # Shape: {"running": [...], "success": [...], ...} or {}. Both fine.
    assert isinstance(meta, dict)


def test_run_operation_creates_and_deletes_empty_instance(
    client: BackendClient,
) -> None:
    """End-to-end async round-trip through run_operation().

    Creates an instance with ``source.type=none`` (LXD/Incus lets you
    build an empty stopped instance without pulling an image — perfect
    for exercising the async operation wiring without any network egress),
    then removes it. Both endpoints return 202 + an operation reference,
    so this covers ``call → operation_wait`` in both directions.
    """
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    payload = {
        "name": name,
        "source": {"type": "none"},
    }

    try:
        result = client.run_operation(
            "POST",
            "/1.0/instances",
            json=payload,
            timeout=30.0,
        )
        assert result.get("status_code") == 200
    finally:
        _delete_instance(client, name)


def _delete_instance(client: BackendClient, name: str) -> None:
    """Best-effort teardown for an instance created during a test."""
    resp = client.request("DELETE", f"/1.0/instances/{name}")
    if resp.status_code == 202:
        op = resp.json().get("metadata") or {}
        if op.get("id"):
            client.operation_wait(op, timeout=30.0)


# ---------------------------------------------------------------------------
# close()


def test_close_disconnects_client_from_daemon() -> None:
    """After close(), further requests must fail — no silent reuse.

    We spin up a fresh client (not the module-scoped fixture, since we
    intend to tear it down) and verify:

    * a request before close() reaches the daemon;
    * close() completes without error;
    * a request after close() raises RuntimeError — the marker httpx
      uses to say "this client is done".

    Skipped when no daemon is reachable, symmetric with the module
    fixture's skip.
    """
    try:
        fresh = BackendClient()
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))

    # Prove the socket is live before we close it.
    meta = fresh.call("GET", "/1.0")
    assert meta.get("api_version")

    fresh.close()

    with pytest.raises(RuntimeError):
        fresh.request("GET", "/1.0")


def test_context_manager_closes_on_exit() -> None:
    """`with BackendClient() as c:` runs close() on exit.

    Same shape as test_close_disconnects_client_from_daemon, but drives
    the context-manager sugar instead of an explicit close(). Verifies:

    * the block yields a live client (a call inside the block reaches
      the daemon);
    * on exit, close() has run — a request after the block raises
      RuntimeError, the same signal an explicit close() produces.
    """
    try:
        with BackendClient() as fresh:
            assert fresh.call("GET", "/1.0").get("api_version")
    except BackendUnavailableError as exc:
        pytest.skip(str(exc))

    with pytest.raises(RuntimeError):
        fresh.request("GET", "/1.0")


# ---------------------------------------------------------------------------
# launch_instance


def test_launch_instance_creates_stopped_instance(client: BackendClient) -> None:
    """launch_instance() with source.type=none yields a Stopped instance.

    Exercises the same async round-trip as the raw run_operation() case,
    but through the public semantic helper the server uses. Also verifies
    the instance actually landed on the daemon by reading it back via
    call() before cleanup.
    """
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    config = {"name": name, "source": {"type": "none"}}

    try:
        result = client.launch_instance(config, timeout=30.0)
        assert result.get("status_code") == 200

        state = client.call("GET", f"/1.0/instances/{name}")
        assert state.get("name") == name
        # source.type=none produces an instance that exists but is not
        # started — daemon reports "Stopped" in the sync record.
        assert state.get("status") == "Stopped"
    finally:
        _delete_instance(client, name)


def test_launch_instance_rejects_duplicate_name(client: BackendClient) -> None:
    """A second launch with the same name is refused by the daemon.

    The *shape* of the refusal varies between daemons and Incus versions:

    * LXD rejects duplicates synchronously with ``409 Conflict``, so the
      initial ``call()`` raises ``httpx.HTTPStatusError``.
    * Older Incus versions raise the same conflict inside the async
      operation, surfacing as ``BackendOperationError``.
    * Recent Incus treats a duplicate ``source.type=none`` create as a
      no-op and returns success; in that case we can't assert anything
      about error mapping, so we skip.

    What the test locks in is the invariant we actually care about: the
    client propagates whichever error the daemon produces without
    silently swallowing it.
    """
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    config = {"name": name, "source": {"type": "none"}}

    try:
        client.launch_instance(config, timeout=30.0)
        try:
            client.launch_instance(config, timeout=30.0)
        except (BackendOperationError, httpx.HTTPStatusError):
            return  # expected: some form of "already exists"
        pytest.skip("daemon accepts idempotent re-create; no error to assert on")
    finally:
        _delete_instance(client, name)


def test_launch_instance_reports_http_error_on_bad_payload(
    client: BackendClient,
) -> None:
    """A malformed request body is rejected synchronously as HTTP 4xx.

    Missing the required ``source`` key is caught by the daemon before it
    kicks off any async work, so we get an httpx.HTTPStatusError out of
    the initial call() rather than a BackendOperationError from the wait.
    Distinguishes the two error surfaces so callers can map them to
    different gRPC status codes.
    """
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    with pytest.raises(httpx.HTTPStatusError):
        client.launch_instance({"name": name}, timeout=30.0)


# ---------------------------------------------------------------------------
# get_instance_state / set_instance_state


def test_get_instance_state_raises_for_unknown_name(client: BackendClient) -> None:
    """Unknown instance names yield HTTP 404 → httpx.HTTPStatusError."""
    name = f"forgejo-e2e-missing-{uuid.uuid4().hex[:10]}"
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        client.get_instance_state(name)
    assert excinfo.value.response.status_code == 404


# Two daemon-appropriate simplestreams sources. Canonical LXD 5.21 no
# longer pulls from images.linuxcontainers.org (it silently completes
# the create operation without provisioning the instance), so LXD uses
# Canonical's own ubuntu-minimal remote; Incus keeps the community
# images.linuxcontainers.org remote. Both are ~30-40 MB and boot fast.
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
    """Pick a daemon-appropriate image source from ``client.flavor``.

    ``client.flavor`` is derived from the daemon's own ``/1.0``
    self-description, so it stays accurate even when the socket path
    is customised. Falls back to a skip when the daemon reports an
    unknown flavour.
    """
    try:
        return _TINY_IMAGE_SOURCES[client.flavor]
    except KeyError:
        pytest.skip(f"cannot pick a tiny image source for flavor {client.flavor!r}")


def test_set_instance_state_completes_start_and_stop(
    client: BackendClient,
) -> None:
    """set_instance_state drives a real instance through start and stop.

    Uses a tiny bootable image so we can assert on the observable state
    after each transition — with ``source.type=none`` the daemon accepts
    ``start`` but the instance immediately falls back to Stopped, which
    makes state assertions meaningless. Also pins the status_code enum
    values the Start RPC's idempotence check keys off (102=Stopped,
    103=Running).

    ``stop`` uses ``force=True`` — a graceful shutdown takes seconds
    even on a tiny image while force is instantaneous, and we're not
    testing guest ACPI behaviour here.
    """
    source = _tiny_image_source(client)
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    try:
        # First-time image pull can take a while; give the daemon room.
        client.launch_instance({"name": name, "source": source}, timeout=180.0)

        # Canonical LXD 5.21 has been observed silently completing the
        # create operation as "success" without provisioning anything
        # when the image remote is unreachable. Verify the instance
        # actually landed before touching /state, so that failure mode
        # points the finger at launch_instance instead of surfacing as
        # a mysterious 404 later.
        record = client.call("GET", f"/1.0/instances/{name}")
        assert record.get("name") == name, (
            f"launch_instance reported success but instance {name!r} is absent from the daemon"
        )

        # Fresh instance sits Stopped.
        pre = client.get_instance_state(name)
        assert pre.get("status") == "Stopped"
        assert pre.get("status_code") == 102

        client.set_instance_state(name, "start", timeout=60.0)
        running = client.get_instance_state(name)
        assert running.get("status") == "Running"
        assert running.get("status_code") == 103

        client.set_instance_state(name, "stop", force=True, timeout=30.0)
        stopped = client.get_instance_state(name)
        assert stopped.get("status") == "Stopped"
        assert stopped.get("status_code") == 102
    finally:
        _delete_instance(client, name)


# ---------------------------------------------------------------------------
# exec_stream — needs a running instance, so we reuse the tiny-image helper.


def test_exec_stream_captures_output_and_exit_codes(client: BackendClient) -> None:
    """exec_stream yields stdout/stderr frames and a final ("exit", int).

    Runs three commands against a single freshly-booted instance (one
    launch amortises the image pull across all three checks):

    1. ``echo`` a marker on stdout → frames concatenated must contain
       the marker, exit code must be 0.
    2. ``echo`` a marker on stderr → same, but on the stderr channel.
    3. ``exit 42`` → propagates the non-zero exit code.

    The daemon is free to chunk small payloads however it likes, so we
    reassemble frames by kind rather than pinning the frame count.
    """
    source = _tiny_image_source(client)
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    try:
        client.launch_instance({"name": name, "source": source}, timeout=180.0)
        client.set_instance_state(name, "start", timeout=60.0)

        # Give init a moment to spawn a working shell. A fixed wait is
        # crude but honest — the earlier "poll exec until it works"
        # trick masked real generator errors from the retry loop.
        time.sleep(5)

        # 1. stdout + exit 0
        frames = list(client.exec_stream(name, ["/bin/sh", "-c", "echo forgejo-e2e-out"]))
        stdout = b"".join(p for k, p in frames if k == "stdout")
        exits = [p for k, p in frames if k == "exit"]
        assert b"forgejo-e2e-out" in stdout
        assert exits == [0]

        # 2. stderr + exit 0
        frames = list(client.exec_stream(name, ["/bin/sh", "-c", "echo forgejo-e2e-err 1>&2"]))
        stderr = b"".join(p for k, p in frames if k == "stderr")
        exits = [p for k, p in frames if k == "exit"]
        assert b"forgejo-e2e-err" in stderr
        assert exits == [0]

        # 3. non-zero exit propagates.
        frames = list(client.exec_stream(name, ["/bin/sh", "-c", "exit 42"]))
        exits = [p for k, p in frames if k == "exit"]
        assert exits == [42]
    finally:
        with contextlib.suppress(BackendOperationError, httpx.HTTPError):
            client.set_instance_state(name, "stop", force=True, timeout=30.0)
        _delete_instance(client, name)


# ---------------------------------------------------------------------------
# push_directory / push_file / push_symlink — reuse the tiny-image helper
# so we can shell into the guest to verify what actually landed.


def test_push_helpers_replay_directory_file_and_symlink(
    client: BackendClient,
) -> None:
    """push_directory / push_file / push_symlink land the expected FS entries.

    Uses the same tiny bootable image as the state and exec tests so we
    can shell in and inspect what actually appeared on disk. Verifies:

    * ``push_directory`` creates the target with directory semantics.
    * ``push_file`` writes exact bytes and honours the ``mode`` header.
    * ``push_symlink`` creates a link whose target matches what we sent.
    """
    source = _tiny_image_source(client)
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    payload = b"forgejo-e2e-file-payload\n"
    try:
        client.launch_instance({"name": name, "source": source}, timeout=180.0)
        client.set_instance_state(name, "start", timeout=60.0)
        # Same rationale as the exec test: init needs a beat to spawn a
        # working shell before we can read files back.
        time.sleep(5)

        client.push_directory(name, "/root/forgejo-e2e")
        client.push_file(name, "/root/forgejo-e2e/hello", payload, mode=0o600)
        client.push_symlink(name, "/root/forgejo-e2e/hello.lnk", "/root/forgejo-e2e/hello")

        def _run(cmd: list[str]) -> tuple[bytes, int]:
            frames = list(client.exec_stream(name, cmd))
            stdout = b"".join(p for k, p in frames if k == "stdout")
            exits = [p for k, p in frames if k == "exit"]
            assert exits, f"no exit frame from {cmd!r}"
            return stdout, exits[0]

        # Directory landed.
        _, rc = _run(["/bin/sh", "-c", "test -d /root/forgejo-e2e"])
        assert rc == 0

        # File contents match exactly.
        out, rc = _run(["/bin/cat", "/root/forgejo-e2e/hello"])
        assert rc == 0
        assert out == payload

        # File mode honours the header (0600 → "600" in stat's %a format).
        out, rc = _run(["/bin/sh", "-c", "stat -c %a /root/forgejo-e2e/hello"])
        assert rc == 0
        assert out.strip() == b"600"

        # Symlink resolves to the target we asked for.
        out, rc = _run(["/bin/sh", "-c", "readlink /root/forgejo-e2e/hello.lnk"])
        assert rc == 0
        assert out.strip() == b"/root/forgejo-e2e/hello"
    finally:
        with contextlib.suppress(BackendOperationError, httpx.HTTPError):
            client.set_instance_state(name, "stop", force=True, timeout=30.0)
        _delete_instance(client, name)


# ---------------------------------------------------------------------------
# pull_file — round-trip against push_* on one shared instance so we don't
# pay the image-pull cost twice.


def test_pull_file_round_trips_and_reports_404(client: BackendClient) -> None:
    """pull_file returns the right (kind, data, mode) for each entry type.

    Uses ``push_*`` to plant a known tree, then ``pull_file`` to fetch
    each entry back. Verifies file bytes+mode, directory listing, and
    symlink target — plus 404 propagation for a missing path.
    """
    import json

    source = _tiny_image_source(client)
    name = f"forgejo-e2e-{uuid.uuid4().hex[:10]}"
    payload = b"forgejo-e2e-pull-payload\n"
    try:
        client.launch_instance({"name": name, "source": source}, timeout=180.0)
        client.set_instance_state(name, "start", timeout=60.0)
        time.sleep(5)

        client.push_directory(name, "/root/forgejo-e2e-pull")
        client.push_file(name, "/root/forgejo-e2e-pull/hello", payload, mode=0o600)
        client.push_symlink(
            name, "/root/forgejo-e2e-pull/hello.lnk", "/root/forgejo-e2e-pull/hello"
        )

        # File: exact bytes + mode.
        kind, data, mode = client.pull_file(name, "/root/forgejo-e2e-pull/hello")
        assert kind == "file"
        assert data == payload
        assert mode == 0o600

        # Directory: listing enumerates the two children we planted.
        # Both daemons wrap sync responses in a metadata envelope; the
        # raw file-body variant may either return that envelope or the
        # bare list at top level. Handle either shape.
        kind, data, _ = client.pull_file(name, "/root/forgejo-e2e-pull")
        assert kind == "directory"
        entries = json.loads(data.decode())
        if isinstance(entries, dict):
            entries = entries.get("metadata") or []
        assert set(entries) >= {"hello", "hello.lnk"}

        # Symlink: body is the target path.
        kind, data, _ = client.pull_file(name, "/root/forgejo-e2e-pull/hello.lnk")
        assert kind == "symlink"
        assert data == b"/root/forgejo-e2e-pull/hello"

        # Missing path: synchronous 404 → httpx.HTTPStatusError.
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            client.pull_file(name, "/root/does-not-exist")
        assert excinfo.value.response.status_code == 404
    finally:
        with contextlib.suppress(BackendOperationError, httpx.HTTPError):
            client.set_instance_state(name, "stop", force=True, timeout=30.0)
        _delete_instance(client, name)
