"""End-to-end tests for :mod:`forgejo_lxd_runner.client`.

These require a live LXD or Incus daemon on the host and are skipped
otherwise. The socket is intentionally *not* passed in — autodetect is
part of what we're exercising. The CI matrix sets
``FORGEJO_LXD_RUNNER_E2E_BACKEND`` to ``lxd`` or ``incus`` so we can
assert autodetect landed on the intended daemon.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest

from forgejo_lxd_runner.client import (
    BackendClient,
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
