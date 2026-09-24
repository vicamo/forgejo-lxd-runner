"""Minimal REST client for the LXD / Incus ``/1.0`` API.

Incus forked from LXD at API ``/1.0`` and kept the shape almost identical:
the URL layout, JSON payload keys, async-operation model, and websocket
endpoints for exec / file transfer all match. The differences we care
about are the Unix socket path and, occasionally, which API extensions a
given daemon advertises (for example ``oci_images`` — Incus-only, gates
``docker:`` / ``oci:`` remote image references).

``BackendClient`` speaks the shared subset over a Unix socket via
``httpx``: one HTTP session, no per-project client cache (LXD's
``pylxd.Client`` binds ``project`` at construction time — a papercut we
sidestep by passing ``?project=`` per request). Every RPC-specific helper
(``instances_create``, ``operation_wait``, exec / file transfer) grows in
its own commit alongside the ``BackendPluginService`` method that needs
it.
"""

from __future__ import annotations

import contextlib
import logging
import os
import queue
import threading
from collections.abc import Iterator
from typing import Any

import httpx
import websockets
from websockets.sync.client import connect as ws_connect

log = logging.getLogger(__name__)


# In autodetect order: Incus first (the newer, actively developed fork),
# then the Snap-packaged LXD, then the distro LXD. First readable socket
# wins. Operators pin a specific one by passing ``socket_path=``.
_DEFAULT_SOCKETS = (
    "/var/lib/incus/unix.socket",
    "/var/snap/lxd/common/lxd/unix.socket",
    "/var/lib/lxd/unix.socket",
)


class BackendUnavailableError(RuntimeError):
    """No usable LXD / Incus Unix socket could be found."""


class BackendOperationError(RuntimeError):
    """An async LXD / Incus operation completed with ``status_code=400``.

    Carries the daemon's ``err`` string in ``args[0]``. Distinct from
    HTTP errors (``httpx.HTTPStatusError``) so callers can map
    daemon-side failures to a different gRPC status than transport
    failures.
    """


class BackendClient:
    """Thin ``httpx`` wrapper over the LXD / Incus ``/1.0`` REST API.

    Parameters
    ----------
    socket_path:
        Absolute path to the daemon's Unix socket. When ``None``, the
        first path from ``_DEFAULT_SOCKETS`` that exists on disk is
        picked; a missing socket raises ``BackendUnavailableError``.
    """

    def __init__(self, socket_path: str | None = None) -> None:
        if socket_path is None:
            socket_path = _autodetect_socket()
        self.socket_path = socket_path
        # ``base_url`` host is arbitrary — httpx needs *something* to
        # assemble URLs against, but the actual transport is the Unix
        # socket, so no DNS or TCP ever happens.
        self._http = httpx.Client(
            transport=httpx.HTTPTransport(uds=socket_path),
            base_url="http://localhost",
            timeout=httpx.Timeout(30.0, connect=5.0),
        )
        # Populated lazily on first ``server_info`` call. Cached because
        # the answer is fixed for the daemon's lifetime and every
        # capability check would otherwise round-trip.
        self._server_info: dict[str, Any] | None = None
        # Cached derivative of ``server_info``; computed on first access
        # so ``flavor`` never re-parses the environment dict.
        self._flavor: str | None = None

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> BackendClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Server capability probe

    @property
    def server_info(self) -> dict[str, Any]:
        """The daemon's ``/1.0`` metadata dict, fetched once and cached.

        The daemon returns everything we need to distinguish flavours
        and negotiate features: ``environment.server_name`` (``"lxd"``
        vs. ``"incus"``), ``api_extensions`` (feature flags such as
        ``oci_images``), and ``api_version``. The first access
        populates the cache; later accesses return it unchanged.
        """

        if self._server_info is None:
            self._server_info = self.call("GET", "/1.0")
        return self._server_info

    @property
    def flavor(self) -> str:
        """``"incus"`` or ``"lxd"`` — whichever the daemon self-reports.

        Falls back to ``"unknown"`` on daemons that don't set
        ``environment.server``; callers should treat that as
        "assume LXD-compatible subset only".
        """

        if self._flavor is not None:
            return self._flavor
        env = self.server_info.get("environment") or {}
        name = str(env.get("server") or "").lower()
        if name in {"incus", "lxd"}:
            self._flavor = name
        else:
            self._flavor = "unknown"
        return self._flavor

    def supports(self, extension: str) -> bool:
        """Whether the daemon advertises ``extension`` in ``api_extensions``.

        Use this to gate features that only exist on one flavour — for
        example ``supports("oci_images")`` before honouring an
        ``oci:``/``docker:`` image reference.
        """

        exts = self.server_info.get("api_extensions") or []
        return extension in exts

    # ------------------------------------------------------------------
    # Low-level HTTP

    def request(
        self,
        method: str,
        path: str,
        *,
        project: str | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a request to the daemon and return the raw response.

        ``project`` is added as a ``?project=`` query parameter when
        set — both LXD and Incus accept it in that form, which lets us
        avoid the per-project client cache pylxd forces. Callers stay
        responsible for interpreting the response body (sync vs. async
        operation, error mapping, etc.); for the common cases prefer
        :meth:`call` (sync endpoints) or :meth:`run_operation` (async).
        """

        params = dict(kwargs.pop("params", None) or {})
        if project:
            params["project"] = project
        if params:
            kwargs["params"] = params
        return self._http.request(method, path, **kwargs)

    def call(
        self,
        method: str,
        path: str,
        *,
        project: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Send a synchronous request and return the ``metadata`` dict.

        Wraps :meth:`request` for the common "call the API, get the
        payload back" pattern: raises ``httpx.HTTPStatusError`` on non-2xx
        via ``raise_for_status``, then unwraps ``response.json()["metadata"]``
        (an empty dict when absent). Use this for endpoints that reply
        synchronously — ``GET /1.0/instances/<name>``, ``GET /1.0/…/state``,
        etc. For ``202 Accepted`` endpoints that hand back an operation,
        use :meth:`run_operation` instead.
        """

        resp = self.request(method, path, project=project, **kwargs)
        resp.raise_for_status()
        return resp.json().get("metadata") or {}

    # ------------------------------------------------------------------
    # Async operations

    def operation_wait(
        self,
        operation: dict[str, Any],
        *,
        project: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Block until an async operation finishes, then return its metadata.

        ``operation`` is the ``metadata`` dict from a ``202 Accepted``
        response — every write endpoint on LXD/Incus returns one when it
        kicks off background work (create / start / stop / delete / exec
        / …). We synchronously wait on ``/1.0/operations/<uuid>/wait``;
        the daemon accepts ``?timeout=<seconds>`` and returns 200 with
        the final operation record either way (a timed-out wait returns
        the still-``running`` record, distinguished from success by the
        ``status_code`` field).

        Raises ``httpx.HTTPStatusError`` on non-2xx responses so callers
        can map to gRPC status codes. On operation-level failure (LXD
        reports ``status_code=400`` inside the operation) we surface the
        ``err`` string as a ``BackendOperationError``.
        """

        op_id = operation["id"]
        params: dict[str, Any] = {}
        if timeout is not None and timeout > 0:
            # The daemon parses ``?timeout=`` as an integer number of
            # seconds; a float (e.g. ``30.0``) makes it reply 500. Round
            # up so a sub-second timeout still waits at least one tick.
            params["timeout"] = max(1, int(timeout))
        body = self.call(
            "GET",
            f"/1.0/operations/{op_id}/wait",
            project=project,
            params=params or None,
        )
        # LXD/Incus wraps the operation record; ``status_code`` 200
        # means "Success", 400 "Failure" — anything else (101 Running)
        # is only possible under an explicit timeout.
        if body.get("status_code") == 400:
            raise BackendOperationError(str(body.get("err") or "operation failed"))
        return body

    def run_operation(
        self,
        method: str,
        path: str,
        *,
        project: str | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Kick off an async operation and wait for it to finish.

        Every write endpoint on LXD/Incus (instance create/start/stop/
        delete, exec, file push, …) replies with ``202 Accepted`` and an
        operation record. This helper posts the request via :meth:`call`
        and then blocks on :meth:`operation_wait` until the operation
        completes (or times out). Returns the final operation metadata.
        """

        operation = self.call(method, path, project=project, **kwargs)
        return self.operation_wait(operation, project=project, timeout=timeout)

    # ------------------------------------------------------------------
    # Instance lifecycle

    def launch_instance(
        self,
        config: dict[str, Any],
        *,
        project: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Create an instance from ``config`` and wait for it to be ready.

        Wraps the two-step LXD/Incus create dance via :meth:`run_operation`:
        ``POST /1.0/instances`` returns a ``202 Accepted`` with an operation
        record, which we then wait on synchronously. Returns the completed
        operation metadata; ``httpx.HTTPStatusError`` and
        :class:`BackendOperationError` propagate so the caller can map
        them to gRPC status codes.
        """

        return self.run_operation(
            "POST", "/1.0/instances", project=project, timeout=timeout, json=config
        )

    def get_instance_state(
        self,
        name: str,
        *,
        project: str | None = None,
    ) -> dict[str, Any]:
        """Return the ``/1.0/instances/<name>/state`` metadata dict."""

        return self.call("GET", f"/1.0/instances/{name}/state", project=project)

    def set_instance_state(
        self,
        name: str,
        action: str,
        *,
        project: str | None = None,
        timeout: float | None = None,
        force: bool = False,
        stateful: bool = False,
    ) -> dict[str, Any]:
        """Drive an instance through a state transition and wait for it.

        Wraps ``PUT /1.0/instances/<name>/state`` — the endpoint LXD /
        Incus expose for ``start`` / ``stop`` / ``restart`` / ``freeze``
        / ``unfreeze``. The daemon replies with a 202 + operation which
        this helper blocks on via :meth:`run_operation`. ``timeout`` on
        the payload stays at ``-1`` (no daemon-side deadline); the
        keyword ``timeout`` argument is the client-side wait budget.
        """

        payload: dict[str, Any] = {"action": action, "timeout": -1}
        if force:
            payload["force"] = True
        if stateful:
            payload["stateful"] = True
        return self.run_operation(
            "PUT",
            f"/1.0/instances/{name}/state",
            project=project,
            timeout=timeout,
            json=payload,
        )

    # ------------------------------------------------------------------
    # Exec streaming

    def exec_stream(
        self,
        name: str,
        command: list[str],
        *,
        environment: dict[str, str] | None = None,
        user: int | None = None,
        cwd: str | None = None,
        project: str | None = None,
    ) -> Iterator[tuple[str, Any]]:
        """Run ``command`` inside instance ``name`` and stream its output.

        Yields ``("stdout", bytes)`` / ``("stderr", bytes)`` tuples as
        the daemon flushes frames from the exec websockets, then a final
        ``("exit", int)`` with the process exit status. On daemon-side
        failure raises ``BackendOperationError``; HTTP transport failures
        surface as ``httpx.HTTPError`` from the initial POST.

        Under the hood this drives four websockets — the three fd sockets
        (stdin/stdout/stderr) plus a control channel — that LXD/Incus
        creates for every ``wait-for-websocket`` exec. We don't feed
        stdin (the plugin protocol has no stdin channel), so fd 0 is
        opened and immediately closed to signal EOF; fds 1 and 2 are
        drained concurrently in threads into a shared queue so the
        generator can yield frames in arrival order.
        """

        payload: dict[str, Any] = {
            "command": command,
            "wait-for-websocket": True,
            "interactive": False,
        }
        if environment:
            payload["environment"] = environment
        if user is not None:
            payload["user"] = user
        if cwd:
            payload["cwd"] = cwd

        op = self.call("POST", f"/1.0/instances/{name}/exec", project=project, json=payload)
        op_id = op["id"]
        fds = (op.get("metadata") or {}).get("fds") or {}
        # LXD exposes fd secrets keyed by fd number as strings: "0","1","2","control".
        try:
            secret_stdin = fds["0"]
            secret_stdout = fds["1"]
            secret_stderr = fds["2"]
            secret_control = fds["control"]
        except KeyError as exc:
            raise BackendOperationError(f"exec operation missing fd secret {exc}") from exc

        def _ws(fd_secret: str) -> Any:
            # Pre-connect a Unix socket ourselves and hand it to the
            # ``websockets`` handshake — the ``sock=`` kwarg lets us keep
            # ws:// URIs while talking over AF_UNIX. Always used as a
            # context manager by the callers below; ``websockets`` 15+
            # deprecates the "just call ``close()``" pattern.
            import socket as _socket

            sock = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
            sock.connect(self.socket_path)
            uri = f"ws://localhost/1.0/operations/{op_id}/websocket?secret={fd_secret}"
            return ws_connect(uri, sock=sock, open_timeout=None, close_timeout=None)

        out_q: queue.Queue[tuple[str, bytes] | None] = queue.Queue()

        def _drain(fd_secret: str, kind: str) -> None:
            try:
                ws_cm = _ws(fd_secret)
            except Exception as exc:  # pragma: no cover — defensive
                out_q.put(("_error", str(exc).encode()))
                out_q.put(None)
                return
            try:
                with ws_cm as ws:
                    try:
                        for frame in ws:
                            if not frame:
                                # LXD signals EOF on an fd with an empty binary frame.
                                break
                            data = frame.encode() if isinstance(frame, str) else frame
                            out_q.put((kind, data))
                    except websockets.ConnectionClosed:
                        pass
            finally:
                out_q.put(None)

        # Hold control + stdin open across the whole drain. The daemon
        # waits for all four fd sockets to connect before starting the
        # process, so we can't connect+close stdin early — that races
        # the drain threads' connects and the daemon returns HTTP 500
        # on the losers. ExitStack keeps them alive across the ``yield``
        # loop below without a nested ``with`` that would swallow it.
        with contextlib.ExitStack() as stack:
            stack.enter_context(_ws(secret_control))
            stack.enter_context(_ws(secret_stdin))

            t_out = threading.Thread(target=_drain, args=(secret_stdout, "stdout"), daemon=True)
            t_err = threading.Thread(target=_drain, args=(secret_stderr, "stderr"), daemon=True)
            t_out.start()
            t_err.start()

            remaining = 2
            try:
                while remaining:
                    item = out_q.get()
                    if item is None:
                        remaining -= 1
                        continue
                    kind, data = item
                    if kind == "_error":
                        raise BackendOperationError(data.decode(errors="replace"))
                    yield kind, data
            finally:
                t_out.join(timeout=5)
                t_err.join(timeout=5)

        # Now the operation record carries the final return code.
        meta = self.call("GET", f"/1.0/operations/{op_id}", project=project)
        return_code = (meta.get("metadata") or {}).get("return")
        if return_code is None:
            # Fall back to waiting on the operation if we somehow raced
            # the recorded return; wait is idempotent post-completion.
            waited = self.operation_wait({"id": op_id}, project=project)
            return_code = waited.get("metadata", {}).get("return")
        yield "exit", int(return_code or 0)


def _autodetect_socket() -> str:
    """Return the first existing socket from ``_DEFAULT_SOCKETS``.

    Kept as a module-level helper so tests can monkey-patch it without
    reaching into ``BackendClient.__init__``.
    """

    for candidate in _DEFAULT_SOCKETS:
        if os.path.exists(candidate):
            log.debug("using backend socket %s", candidate)
            return candidate
    raise BackendUnavailableError(
        "no LXD / Incus Unix socket found; tried " + ", ".join(_DEFAULT_SOCKETS)
    )
