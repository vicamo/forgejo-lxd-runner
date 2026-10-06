"""Connection-mode selection in ``BackendClient.__init__``.

Two modes: a local Unix socket (autodetected or pinned) and a remote
HTTPS endpoint reached over mutual TLS. These tests cover which mode is
chosen and the validation guarding the remote mode. ``load_cert_chain``
is stubbed so no real PEM files are needed and no connection is opened.
"""

from __future__ import annotations

import ssl

import httpx
import pytest

from forgejo_lxd_runner.client import BackendClient


@pytest.fixture(autouse=True)
def _stub_cert_loading(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip reading PEM files from disk; we only test wiring, not TLS."""
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *a, **k: None)


def test_pinned_socket_path_uses_unix_mode() -> None:
    client = BackendClient(socket_path="/run/lxd.sock")
    assert client.socket_path == "/run/lxd.sock"
    assert client.endpoint is None
    assert client._ssl_ctx is None


def test_autodetect_when_nothing_pinned(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "forgejo_lxd_runner.client._autodetect_socket",
        lambda: "/run/detected.sock",
    )
    client = BackendClient()
    assert client.socket_path == "/run/detected.sock"
    assert client.endpoint is None


def test_endpoint_selects_remote_mode() -> None:
    client = BackendClient(
        endpoint="https://lxd-prod:8443",
        client_cert="/etc/certs/c.crt",
        client_key="/etc/certs/c.key",
    )
    assert client.socket_path is None
    assert client.endpoint == "https://lxd-prod:8443"
    assert isinstance(client._ssl_ctx, ssl.SSLContext)


def test_endpoint_strips_trailing_slash() -> None:
    client = BackendClient(
        endpoint="https://lxd-prod:8443/",
        client_cert="/etc/certs/c.crt",
        client_key="/etc/certs/c.key",
    )
    assert client.endpoint == "https://lxd-prod:8443"


def test_remote_mode_base_url_is_the_endpoint() -> None:
    client = BackendClient(
        endpoint="https://lxd-prod:8443",
        client_cert="/etc/certs/c.crt",
        client_key="/etc/certs/c.key",
    )
    assert client._http.base_url == httpx.URL("https://lxd-prod:8443")


def test_endpoint_rejects_non_https() -> None:
    with pytest.raises(ValueError, match="https://"):
        BackendClient(
            endpoint="http://lxd-prod:8443",
            client_cert="/etc/certs/c.crt",
            client_key="/etc/certs/c.key",
        )


def test_endpoint_requires_cert_and_key() -> None:
    with pytest.raises(ValueError, match="requires client_cert and client_key"):
        BackendClient(endpoint="https://lxd-prod:8443")


def test_endpoint_rejects_cert_without_key() -> None:
    with pytest.raises(ValueError, match="together"):
        BackendClient(endpoint="https://lxd-prod:8443", client_cert="/etc/certs/c.crt")


def test_endpoint_rejects_key_without_cert() -> None:
    with pytest.raises(ValueError, match="together"):
        BackendClient(endpoint="https://lxd-prod:8443", client_key="/etc/certs/c.key")


def test_server_cert_builds_a_pinned_context(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    real = ssl.create_default_context

    def _spy(*args: object, **kwargs: object) -> ssl.SSLContext:
        captured.update(kwargs)
        return real()

    monkeypatch.setattr(ssl, "create_default_context", _spy)
    client = BackendClient(
        endpoint="https://lxd-prod:8443",
        client_cert="/etc/certs/c.crt",
        client_key="/etc/certs/c.key",
        server_cert="/etc/certs/server.crt",
    )
    assert isinstance(client._ssl_ctx, ssl.SSLContext)
    assert captured.get("cafile") == "/etc/certs/server.crt"


def test_no_server_cert_uses_system_trust(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    real = ssl.create_default_context

    def _spy(*args: object, **kwargs: object) -> ssl.SSLContext:
        captured.update(kwargs)
        return real()

    monkeypatch.setattr(ssl, "create_default_context", _spy)
    BackendClient(
        endpoint="https://lxd-prod:8443",
        client_cert="/etc/certs/c.crt",
        client_key="/etc/certs/c.key",
    )
    assert "cafile" not in captured
