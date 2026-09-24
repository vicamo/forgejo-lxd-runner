"""Unit test: the package version is logged at startup."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from forgejo_lxd_runner import __version__
from forgejo_lxd_runner.__main__ import serve


def test_version_is_logged_at_startup(caplog) -> None:
    """serve() emits the package version before opening the port.

    We stub the whole gRPC + health-checker surface so ``serve`` returns
    immediately without touching a real server; the only thing we assert
    on is that the version line hit the log.
    """
    fake_server = MagicMock()
    fake_server.wait_for_termination.return_value = None

    with (
        patch("forgejo_lxd_runner.__main__.grpc.server", return_value=fake_server),
        patch("forgejo_lxd_runner.__main__.HealthService") as fake_checker_cls,
        patch("forgejo_lxd_runner.__main__.BackendPluginService"),
        patch("forgejo_lxd_runner.__main__.signal.signal"),
    ):
        fake_checker_cls.return_value = MagicMock()
        with caplog.at_level(logging.INFO, logger="forgejo_lxd_runner"):
            serve(address="unix:///tmp/does-not-matter.sock", workers=1)

    messages = [rec.getMessage() for rec in caplog.records]
    assert any(f"forgejo-lxd-runner {__version__} starting" in m for m in messages), messages
