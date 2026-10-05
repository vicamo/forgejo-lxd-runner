"""Unit tests for serve()'s signal-driven shutdown path."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

from forgejo_lxd_runner.__main__ import serve


def test_serve_does_not_stop_the_server_until_a_signal_arrives() -> None:
    """The old code called server.stop() at startup; the new path must not."""
    fake_server = MagicMock()
    fake_event = MagicMock()
    fake_event.wait.return_value = None  # return immediately, as if signalled

    with (
        patch("forgejo_lxd_runner.__main__.grpc.server", return_value=fake_server),
        patch("forgejo_lxd_runner.__main__.HealthService") as fake_checker_cls,
        patch("forgejo_lxd_runner.__main__.BackendPluginService"),
        patch("forgejo_lxd_runner.__main__.signal.signal"),
        patch("forgejo_lxd_runner.__main__.threading.Event", return_value=fake_event),
    ):
        fake_checker = MagicMock()
        fake_checker_cls.return_value = fake_checker
        serve(address="unix:///tmp/does-not-matter.sock", workers=1)

    # Exactly one stop, after the wait returned -- not speculatively at start.
    fake_server.stop.assert_called_once_with(grace=5)
    # The server drain is awaited.
    fake_server.stop.return_value.wait.assert_called_once_with()
    # The health poller is stopped as part of shutdown.
    fake_checker.stop.assert_called_once_with()


def test_serve_signal_handler_only_sets_the_stop_event() -> None:
    """SIGINT/SIGTERM must just set the event -- all teardown is on main."""
    fake_server = MagicMock()
    fake_event = MagicMock()
    fake_event.wait.return_value = None
    handlers: dict[int, object] = {}

    def _capture(signum: int, handler: object) -> None:
        handlers[signum] = handler

    with (
        patch("forgejo_lxd_runner.__main__.grpc.server", return_value=fake_server),
        patch("forgejo_lxd_runner.__main__.HealthService"),
        patch("forgejo_lxd_runner.__main__.BackendPluginService"),
        patch("forgejo_lxd_runner.__main__.signal.signal", side_effect=_capture),
        patch("forgejo_lxd_runner.__main__.threading.Event", return_value=fake_event),
    ):
        serve(address="unix:///tmp/does-not-matter.sock", workers=1)

    import signal as _signal

    handler = handlers[_signal.SIGTERM]
    assert callable(handler)
    handler(_signal.SIGTERM, None)
    # The handler's sole job is to set the event; it never touches the server.
    assert call() in fake_event.set.call_args_list
