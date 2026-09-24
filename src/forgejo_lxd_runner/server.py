"""BackendPlugin gRPC service — LXD / Incus-backed implementation.

Commit 1 (MVP): the plugin respects only ``CreateRequest.label_arg`` (the
image reference) and lets the daemon supply every other default —
project, profile, user, network, storage. Later commits add options one
at a time.

Known simplifications, all called out in code:

* ``CreateResponse`` reports a hardcoded filesystem layout and
  ``os=Linux`` / ``arch=X64`` (the GHA ``RUNNER_OS`` / ``RUNNER_ARCH``
  vocabulary). Discovery from the LXD image metadata is a later commit.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator

import grpc
import httpx

from .client import BackendClient, BackendOperationError
from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc

log = logging.getLogger(__name__)

# LXD / Incus instance status codes we care about.
_STATUS_RUNNING = 103


class _Env:
    """Per-environment state tracked by the plugin."""

    __slots__ = ("instance_name",)

    def __init__(self, instance_name: str) -> None:
        self.instance_name = instance_name


class BackendPluginService(plugin_pb2_grpc.BackendPluginServicer):
    """LXD / Incus-backed implementation of ``plugin.v1alpha.BackendPlugin``."""

    DEFAULT_NAME = "lxd"
    """Default wire-protocol backend name returned by ``Capabilities``.

    Must match the plugin's scheme in the runner's ``plugins:`` config —
    labels like ``mylabel:lxd://<image>`` are routed to this backend.
    Override via the ``--name`` CLI flag when running multiple plugin
    processes so each is addressable under a distinct scheme.
    """

    def __init__(self, name: str = DEFAULT_NAME) -> None:
        # The name is what Forgejo runner labels reference via the
        # ``<label>:<name>://<arg>`` scheme. Making it configurable lets
        # an operator run several plugin processes side by side — each
        # with its own connection settings — and address them
        # independently from a single runner config.
        self.name = name
        # BackendClient autodetects the daemon's Unix socket (Incus,
        # Snap-packaged LXD, distro LXD in that order).
        self._client = BackendClient()
        self._envs: dict[str, _Env] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _lookup(self, context: grpc.ServicerContext, env_id: str) -> _Env:
        with self._lock:
            env = self._envs.get(env_id)
        if env is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown environment {env_id!r}")
            raise AssertionError("unreachable")  # for type checkers
        return env

    # ------------------------------------------------------------------
    # BackendPlugin RPCs
    # ------------------------------------------------------------------

    def Capabilities(  # noqa: N802 — gRPC method name
        self,
        request: plugin_pb2.CapabilitiesRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CapabilitiesResponse:
        return plugin_pb2.CapabilitiesResponse(name=self.name)

    def Create(  # noqa: N802
        self,
        request: plugin_pb2.CreateRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CreateResponse:
        if not request.label_arg:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "label_arg is required (set image via `mylabel:lxd://<image>`)",
            )

        image = request.label_arg
        # ``CreateRequest.name`` is the runner-assigned per-job handle; reuse
        # it verbatim so the daemon-side instance name == environment_id.
        name = request.name
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "name is required")

        config: dict[str, object] = {
            "name": name,
            "source": {"type": "image", "alias": image},
        }
        try:
            self._client.launch_instance(config)
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd create {image!r}: {exc}")
            raise AssertionError("unreachable") from exc

        with self._lock:
            self._envs[name] = _Env(instance_name=name)

        log.info("created environment %s from image %s", name, image)

        # TODO: discover os/arch and expose a knob for the paths.
        return plugin_pb2.CreateResponse(
            environment_id=name,
            root_path="/root/actions-runner",
            act_path="/root/actions-runner/act",
            tool_cache_path="/opt/hostedtoolcache",
            temp_path="/tmp",
            os="Linux",
            arch="X64",
        )

    def Start(  # noqa: N802
        self,
        request: plugin_pb2.StartRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.StartOutput]:
        env = self._lookup(context, request.environment_id)
        name = env.instance_name
        try:
            state = self._client.get_instance_state(name)
            if state.get("status_code") != _STATUS_RUNNING:
                self._client.set_instance_state(name, "start")
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd start: {exc}")

        log.info("started environment %s", request.environment_id)
        # No image_env discovery yet.
        yield plugin_pb2.StartOutput(start_complete=plugin_pb2.StartComplete())

    def Exec(  # noqa: N802
        self,
        request: plugin_pb2.ExecRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.ExecOutput]:
        env = self._lookup(context, request.environment_id)

        environ = dict(request.env) if request.env else None
        cwd = request.workdir or None
        # ``request.user`` is proto3 ``optional string``. Only numeric
        # UIDs for now; name lookup is a later commit.
        uid: int | None = None
        if request.HasField("user") and request.user:
            try:
                uid = int(request.user)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"user must be a numeric UID for now, got {request.user!r}",
                )

        try:
            for kind, payload in self._client.exec_stream(
                env.instance_name,
                list(request.command),
                environment=environ,
                user=uid,
                cwd=cwd,
            ):
                if kind == "exit":
                    yield plugin_pb2.ExecOutput(
                        exec_complete=plugin_pb2.ExecComplete(exit_code=int(payload)),
                    )
                    return
                stream = (
                    plugin_pb2.DataChunk.STDOUT if kind == "stdout" else plugin_pb2.DataChunk.STDERR
                )
                yield plugin_pb2.ExecOutput(
                    data=plugin_pb2.DataChunk(stream=stream, data=payload),
                )
        except (httpx.HTTPError, BackendOperationError) as exc:
            yield plugin_pb2.ExecOutput(
                exec_failed=plugin_pb2.ExecFailed(error_message=str(exc)),
            )

    def CopyIn(  # noqa: N802
        self, request_iterator: Iterator[plugin_pb2.CopyInChunk], context: grpc.ServicerContext
    ) -> plugin_pb2.CopyInResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "CopyIn not implemented")

    def CopyOut(  # noqa: N802
        self, request: plugin_pb2.CopyOutRequest, context: grpc.ServicerContext
    ) -> Iterator[plugin_pb2.CopyOutChunk]:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "CopyOut not implemented")

    def Remove(  # noqa: N802
        self, request: plugin_pb2.RemoveRequest, context: grpc.ServicerContext
    ) -> plugin_pb2.RemoveResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Remove not implemented")
