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
import time
from collections.abc import Iterator
from typing import Any, Literal, overload

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


# ``/1.0/operations/<uuid>/wait`` long-polls: the daemon holds the request
# open for at most ``?timeout=`` seconds, then returns the operation record
# -- 200 Success / 400 Failure if it finished, otherwise a non-terminal code
# (101 Started, 103 Running, ...) if it is still going (image download, VM
# boot, ...). We poll in fixed windows rather than one unbounded blocking
# read: a non-terminal code is positive proof the operation is still alive,
# so the HTTP read budget only ever needs to cover a single window -- never
# the whole (network-speed-dependent) operation.
_OP_POLL_WINDOW = 30
# Margin over the daemon's wait window for it to serialise and ship the reply.
_OP_POLL_READ_MARGIN = 15
# Floor on the time between two polls. A daemon that ignores ``?timeout=``
# (or returns a non-terminal code immediately) would otherwise spin the loop
# into a tight request storm; padding each poll out to this interval bounds
# the request rate without adding latency when the long-poll window is
# actually honoured.
_OP_POLL_MIN_INTERVAL = 1.0

# LXD/Incus instance state ``status_code`` values. Mirrored from the daemon's
# ``shared.StatusCodeStopped`` constant; documented at
# https://documentation.ubuntu.com/lxd/latest/rest-api/#instances .
_INSTANCE_STATUS_STOPPED = 102


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
        response -- every write endpoint on LXD/Incus returns one when it
        kicks off background work (create / start / stop / delete / exec
        / ...). We poll ``/1.0/operations/<uuid>/wait`` in fixed windows:
        each poll long-polls for at most ``_OP_POLL_WINDOW`` seconds and
        returns the operation record. ``status_code`` 200 means Success,
        400 Failure; a non-terminal code (101 Started, 103 Running, ...)
        is positive proof the operation is still in flight (downloading /
        booting) -- we loop again. Polling in bounded windows keeps the
        HTTP read budget fixed and independent of how long the operation
        actually takes.

        ``timeout`` is an overall wall-clock deadline. When it elapses
        before the operation finishes we return the still-running record
        (distinguished from success by its ``status_code``). ``timeout=
        None`` waits indefinitely, looping while the daemon keeps
        reporting the operation as still running.

        Raises ``httpx.HTTPStatusError`` on non-2xx HTTP responses so
        callers can map to gRPC status codes. A failed operation surfaces
        as a ``BackendOperationError``; the two daemons signal it
        differently and we handle both -- LXD returns a ``type="error"``
        envelope (message in ``error``, ``metadata=null``) while Incus
        returns a ``type="sync"`` envelope whose operation record carries
        ``status_code`` 400/401 and the message in ``err`` -- both under
        HTTP 200.
        """

        op_id = operation["id"]
        path = f"/1.0/operations/{op_id}/wait"
        # A fixed per-poll read budget covers one window plus the daemon's
        # reply latency -- it never has to grow with the operation length.
        read_timeout = httpx.Timeout(_OP_POLL_WINDOW + _OP_POLL_READ_MARGIN, connect=5.0)
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            # Shrink the final poll's window so we never overshoot the
            # caller's deadline waiting on the daemon.
            window = _OP_POLL_WINDOW
            if deadline is not None:
                remaining = deadline - time.monotonic()
                window = max(1, min(_OP_POLL_WINDOW, int(remaining)))
            poll_started = time.monotonic()
            # ``/wait`` is itself a *synchronous* endpoint: it replies with
            # the standard response envelope, not a bare operation record.
            # Read the envelope so we can tell a finished/failed operation
            # from a still-running one -- a failed op surfaces only in the
            # envelope (``type="error"``), never in ``metadata``.
            resp = self.request(
                "GET",
                path,
                project=project,
                params={"timeout": window},
                timeout=read_timeout,
            )
            resp.raise_for_status()
            envelope = resp.json()
            if envelope.get("type") == "error":
                # The operation failed; the message lives on the envelope.
                raise BackendOperationError(str(envelope.get("error") or "operation failed"))
            record = envelope.get("metadata") or {}
            status = record.get("status_code")
            if status == 200:
                # Success -- the operation finished cleanly.
                return record
            if status in (400, 401):
                # Some daemons (Incus) report a failed op *inside* the
                # record (``status_code`` 400 Failure / 401 Cancelled) under
                # a ``type="sync"`` envelope, rather than via ``type="error"``
                # -- surface its ``err`` string the same way.
                raise BackendOperationError(str(record.get("err") or "operation failed"))
            # A non-terminal code (100 Created, 101 Started, 103 Running,
            # 105 Pending, ...) is positive proof the operation is still in
            # flight: the daemon's wait window elapsed before it finished.
            # Give up once the overall deadline passes, reporting the
            # still-running record; otherwise loop for another window.
            if deadline is not None and time.monotonic() >= deadline:
                return record
            # A daemon that returns the non-terminal code faster than the
            # requested window (ignoring ``?timeout=``) would spin the loop;
            # pad each poll out to a minimum interval to bound the rate.
            idle = _OP_POLL_MIN_INTERVAL - (time.monotonic() - poll_started)
            if idle > 0:
                time.sleep(idle)

    @overload
    def run_operation(
        self,
        method: str,
        path: str,
        *,
        project: str | None = ...,
        timeout: float | None = ...,
        missing_ok: Literal[False] = ...,
        **kwargs: Any,
    ) -> dict[str, Any]: ...

    @overload
    def run_operation(
        self,
        method: str,
        path: str,
        *,
        project: str | None = ...,
        timeout: float | None = ...,
        missing_ok: Literal[True],
        **kwargs: Any,
    ) -> dict[str, Any] | None: ...

    def run_operation(
        self,
        method: str,
        path: str,
        *,
        project: str | None = None,
        timeout: float | None = None,
        missing_ok: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        """Kick off an async operation and wait for it to finish.

        Every write endpoint on LXD/Incus (instance create/start/stop/
        delete, exec, file push, …) replies with ``202 Accepted`` and an
        operation record. This helper posts the request via :meth:`call`
        and then blocks on :meth:`operation_wait` until the operation
        completes (or times out). Returns the final operation metadata.

        When ``missing_ok=True``, a ``404 Not Found`` on the initial
        request is treated as success and ``None`` is returned — the
        resource is already gone, so the operation is a no-op.
        Symmetric with :func:`os.makedirs` / :func:`shutil.rmtree`'s
        ``ignore_errors`` idiom; useful for teardown paths that race
        with concurrent deletes.
        """

        if missing_ok:
            resp = self.request(method, path, project=project, **kwargs)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            operation = resp.json().get("metadata") or {}
        else:
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

    def remove_instance(
        self,
        name: str,
        *,
        project: str | None = None,
        force: bool = True,
        timeout: float | None = None,
    ) -> None:
        """Remove an instance, tolerating "already gone" at every step.

        Deletion is a three-step dance: read state, stop if the instance
        is not already stopped, then delete. LXD/Incus refuse ``DELETE``
        on a running instance, so the stop step cannot be skipped
        blindly. Any of the three requests can race with a concurrent
        delete (operator ``lxc delete``, another Remove RPC, an
        ``ephemeral`` self-destruct on stop) — every 404 along the way
        is treated as success because the caller's post-condition
        (``instance <name> does not exist in <project>``) already holds.

        ``force=True`` (the default) matches this plugin's teardown
        semantics: kill the instance rather than wait for a clean
        shutdown. Pass ``force=False`` for a graceful stop.

        Unlike :meth:`set_instance_state`, whose contract is "make the
        state change happen" and which therefore surfaces 404 as an
        error, this method's contract is "the instance is gone when I
        return"; 404 satisfies that contract.
        """

        try:
            state = self.get_instance_state(name, project=project)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            return
        if state.get("status_code") != _INSTANCE_STATUS_STOPPED:
            try:
                self.set_instance_state(name, "stop", project=project, timeout=timeout, force=force)
            except httpx.HTTPStatusError as exc:
                # Instance vanished between the state probe and the stop
                # request — post-condition already satisfied.
                if exc.response.status_code != 404:
                    raise
                return
        self.run_operation(
            "DELETE",
            f"/1.0/instances/{name}",
            project=project,
            timeout=timeout,
            missing_ok=True,
        )

    # ------------------------------------------------------------------
    # Profiles

    def create_profile(
        self,
        name: str,
        *,
        description: str | None = None,
        config: dict[str, str] | None = None,
        devices: dict[str, dict[str, str]] | None = None,
        project: str | None = None,
    ) -> None:
        """Create profile ``name`` on the daemon.

        ``POST /1.0/profiles`` is a synchronous endpoint on both LXD and
        Incus — no operation to wait on — so this goes through
        :meth:`call` rather than :meth:`run_operation`. Only the keys the
        caller supplies are sent; omitted ones are left to the daemon's
        own defaults rather than synthesised here. A duplicate name
        surfaces as ``httpx.HTTPStatusError`` (409) for the caller to map.
        """

        payload: dict[str, Any] = {"name": name}
        if description is not None:
            payload["description"] = description
        if config is not None:
            payload["config"] = config
        if devices is not None:
            payload["devices"] = devices
        self.call("POST", "/1.0/profiles", project=project, json=payload)

    def remove_profile(
        self,
        name: str,
        *,
        project: str | None = None,
    ) -> None:
        """Remove profile ``name``, tolerating "already gone".

        Mirrors :meth:`remove_instance`'s contract: the post-condition is
        ``profile <name> does not exist in <project>``, which a 404
        already satisfies. Every other status still raises. Note the
        daemon refuses to delete a profile that instances still
        reference — that surfaces as a 400 and is the caller's problem.
        """

        resp = self.request("DELETE", f"/1.0/profiles/{name}", project=project)
        if resp.status_code == 404:
            return
        resp.raise_for_status()

    # ------------------------------------------------------------------
    # Networks

    def create_network(
        self,
        name: str,
        *,
        description: str | None = None,
        config: dict[str, str] | None = None,
        project: str | None = None,
    ) -> None:
        """Create managed network ``name`` on the daemon.

        ``POST /1.0/networks`` is synchronous on LXD 5.x and Incus, but
        LXD 6.0 made it asynchronous — it replies ``202 Accepted`` with an
        operation that must be waited on before the bridge actually
        exists. Dispatch on the response envelope's ``type``: wait out an
        ``async`` operation, return immediately on a ``sync`` reply.
        Skipping the wait races the instance create that references the
        bridge, which then fails with ``Network not found``. Only the keys
        the caller supplies are sent; the daemon fills in the rest (type
        defaults to ``bridge``, ``ipv4.address`` to a free subnet, and so
        on). A duplicate name surfaces as ``httpx.HTTPStatusError`` (409)
        for the caller to map.

        A bridge network's name becomes the host's Linux bridge
        interface name, so it is capped at 15 characters — the caller
        picks a short name, not this method's concern.
        """

        payload: dict[str, Any] = {"name": name}
        if description is not None:
            payload["description"] = description
        if config is not None:
            payload["config"] = config
        resp = self.request("POST", "/1.0/networks", project=project, json=payload)
        resp.raise_for_status()
        envelope = resp.json()
        if envelope.get("type") == "async":
            self.operation_wait(envelope.get("metadata") or {}, project=project)

    def remove_network(
        self,
        name: str,
        *,
        project: str | None = None,
    ) -> None:
        """Remove managed network ``name``, tolerating "already gone".

        Mirrors :meth:`remove_profile`'s contract: the post-condition is
        ``network <name> does not exist in <project>``, which a 404
        already satisfies. Every other status still raises. The daemon
        refuses to delete a network instances still use — that surfaces
        as a 400 and is the caller's problem.

        ``DELETE /1.0/networks/{name}`` is synchronous on Incus but
        asynchronous on LXD 6.0 — it replies ``202 Accepted`` with an
        operation that must be waited on, otherwise the post-condition
        isn't guaranteed when the method returns. Dispatch on the
        envelope ``type`` like :meth:`create_network`.
        """

        resp = self.request("DELETE", f"/1.0/networks/{name}", project=project)
        if resp.status_code == 404:
            return
        resp.raise_for_status()
        envelope = resp.json()
        if envelope.get("type") == "async":
            self.operation_wait(envelope.get("metadata") or {}, project=project)

    # ------------------------------------------------------------------
    # Exec streaming

    def exec_capture(
        self,
        name: str,
        command: list[str],
        *,
        environment: dict[str, str] | None = None,
        user: int | None = None,
        cwd: str | None = None,
        project: str | None = None,
    ) -> tuple[int, str, str]:
        """Run ``command`` to completion, returning ``(rc, stdout, stderr)``.

        :meth:`exec_stream` yields frames, which is the shape the Exec
        RPC needs but an awkward one for callers that just want a result
        — probing for a binary, reading an image's metadata. Output is
        decoded with ``errors="replace"``: these are diagnostics, and a
        stray non-UTF-8 byte should not raise.
        """

        out: list[bytes] = []
        err: list[bytes] = []
        rc = -1
        for kind, payload in self.exec_stream(
            name,
            command,
            environment=environment,
            user=user,
            cwd=cwd,
            project=project,
        ):
            if kind == "stdout":
                out.append(payload)
            elif kind == "stderr":
                err.append(payload)
            elif kind == "exit":
                rc = int(payload)
        return (
            rc,
            b"".join(out).decode(errors="replace"),
            b"".join(err).decode(errors="replace"),
        )

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

    # ------------------------------------------------------------------
    # File transfer

    def _file_headers(self, file_type: str, mode: int | None = None) -> dict[str, str]:
        """Build the ``X-*-{type,mode}`` headers under the daemon's prefix.

        LXD uses ``X-LXD-*`` and Incus uses ``X-Incus-*``; the daemon
        matches its own prefix only. The flavour comes from
        ``self.flavor`` (cached via ``server_info()``).
        """

        prefix = "X-Incus" if self.flavor == "incus" else "X-LXD"
        headers = {f"{prefix}-type": file_type}
        if mode is not None and file_type == "file":
            headers[f"{prefix}-mode"] = f"{mode:04o}"
        return headers

    def push_directory(self, instance: str, path: str, *, project: str | None = None) -> None:
        """Create ``path`` inside ``instance`` as a directory (mkdir -p semantics)."""
        self.call(
            "POST",
            f"/1.0/instances/{instance}/files",
            project=project,
            params={"path": path},
            headers=self._file_headers("directory"),
        )

    def push_file(
        self,
        instance: str,
        path: str,
        data: bytes,
        *,
        mode: int = 0o644,
        project: str | None = None,
    ) -> None:
        """Upload ``data`` to ``path`` inside ``instance``."""
        self.call(
            "POST",
            f"/1.0/instances/{instance}/files",
            project=project,
            params={"path": path},
            headers=self._file_headers("file", mode=mode),
            content=data,
        )

    def push_symlink(
        self, instance: str, path: str, target: str, *, project: str | None = None
    ) -> None:
        """Create ``path`` inside ``instance`` as a symlink to ``target``."""
        self.call(
            "POST",
            f"/1.0/instances/{instance}/files",
            project=project,
            params={"path": path},
            headers=self._file_headers("symlink"),
            content=target.encode(),
        )

    def pull_file(
        self, instance: str, path: str, *, project: str | None = None
    ) -> tuple[str, bytes, int]:
        """Fetch ``path`` from ``instance``.

        Returns ``(kind, data, mode)`` where ``kind`` is ``"file"``,
        ``"directory"``, or ``"symlink"``. For a directory, ``data`` is
        the JSON body listing the entries (utf-8, ready to
        ``json.loads``); for a file, the raw bytes; for a symlink, the
        target path bytes. ``mode`` is a numeric mode (0 when the daemon
        did not report one, e.g. directories on some versions).

        Bypasses :meth:`call` on purpose: file bytes and symlink targets
        aren't JSON, so the ``metadata`` unwrap ``call()`` performs would
        either fail or throw away the payload. We consume the raw
        response body and read ``kind`` / ``mode`` from headers instead.
        """

        resp = self.request(
            "GET",
            f"/1.0/instances/{instance}/files",
            project=project,
            params={"path": path},
        )
        resp.raise_for_status()
        # Look up either prefix — the daemon replies with its own.
        headers = resp.headers
        kind = headers.get("X-Incus-type") or headers.get("X-LXD-type")
        if kind is None:
            raise BackendOperationError(f"daemon did not report X-*-type on GET files for {path!r}")
        mode_str = headers.get("X-Incus-mode") or headers.get("X-LXD-mode") or "0"
        try:
            mode = int(mode_str, 8)
        except ValueError:
            mode = 0
        return kind, resp.content, mode


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
