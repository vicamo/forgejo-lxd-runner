"""BackendPlugin gRPC service — LXD-backed implementation.

Commit 1 (MVP): the plugin respects only ``CreateRequest.label_arg`` (the
LXD image reference) and lets pylxd / LXD supply every other default —
project, profile, user, network, storage. Later commits add options one
at a time.

Known simplifications, all called out in code:

* ``Exec`` buffers the full command output before yielding. A later
  commit will switch to the streaming websocket exec so long-running
  commands report progress in real time.
* ``CopyIn`` / ``CopyOut`` buffer the whole tar archive in memory. Fine
  for typical workflow payloads; a streaming rewrite is a later commit.
* ``CreateResponse`` reports a hardcoded filesystem layout and
  ``os=linux`` / ``arch=amd64``. Discovery from the running instance is
  a later commit.
"""

from __future__ import annotations

import io
import logging
import tarfile
import tempfile
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import grpc
import pylxd
from pylxd.exceptions import LXDAPIException, NotFound

from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc

log = logging.getLogger(__name__)

# LXD instance status codes we care about.
_LXD_STATUS_STOPPED = 102
_LXD_STATUS_RUNNING = 103

_COPY_CHUNK_SIZE = 256 * 1024


class _Env:
    """Per-environment state tracked by the plugin."""

    __slots__ = ("instance_name",)

    def __init__(self, instance_name: str) -> None:
        self.instance_name = instance_name


class BackendPluginService(plugin_pb2_grpc.BackendPluginServicer):
    """LXD-backed implementation of ``plugin.v1alpha.BackendPlugin``."""

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
        # No endpoint / cert args yet: pylxd auto-detects the local socket
        # and lands in the ``default`` project.
        self._client = pylxd.Client()
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

    def _instance(self, context: grpc.ServicerContext, env_id: str) -> Any:
        env = self._lookup(context, env_id)
        try:
            return self._client.instances.get(env.instance_name)
        except NotFound as exc:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"lxd instance {env.instance_name!r} is gone",
            )
            raise AssertionError("unreachable") from exc

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
        # it verbatim so LXD instance name == environment_id.
        name = request.name
        if not name:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "name is required")

        config = {
            "name": name,
            "source": {"type": "image", "alias": image},
        }
        try:
            instance = self._client.instances.create(config, wait=True)
        except LXDAPIException as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd create {image!r}: {exc}")
            raise AssertionError("unreachable") from exc

        with self._lock:
            self._envs[name] = _Env(instance_name=instance.name)

        log.info("created environment %s from image %s", name, image)

        # TODO: discover os/arch and expose a knob for the paths.
        return plugin_pb2.CreateResponse(
            environment_id=name,
            root_path="/root/actions-runner",
            act_path="/root/actions-runner/act",
            tool_cache_path="/opt/hostedtoolcache",
            temp_path="/tmp",
            os="linux",
            arch="amd64",
        )

    def Start(  # noqa: N802
        self,
        request: plugin_pb2.StartRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.StartOutput]:
        instance = self._instance(context, request.environment_id)
        try:
            if instance.status_code != _LXD_STATUS_RUNNING:
                instance.start(wait=True)
        except LXDAPIException as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd start: {exc}")

        log.info("started environment %s", request.environment_id)
        # No image_env discovery yet.
        yield plugin_pb2.StartOutput(start_complete=plugin_pb2.StartComplete())

    def Exec(  # noqa: N802
        self,
        request: plugin_pb2.ExecRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.ExecOutput]:
        instance = self._instance(context, request.environment_id)

        env = dict(request.env) if request.env else None
        cwd = request.workdir or None
        # ``request.user`` is proto3 ``optional string``. We accept only
        # numeric UIDs for now (pylxd's execute() takes an int); name lookup
        # is a later commit.
        uid = 0
        if request.HasField("user") and request.user:
            try:
                uid = int(request.user)
            except ValueError:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    f"user must be a numeric UID for now, got {request.user!r}",
                )

        try:
            # NOTE: buffered — see module docstring.
            result = instance.execute(
                list(request.command),
                environment=env,
                cwd=cwd,
                user=uid,
            )
        except LXDAPIException as exc:
            yield plugin_pb2.ExecOutput(
                exec_failed=plugin_pb2.ExecFailed(error_message=str(exc)),
            )
            return

        for stream, payload in (
            (plugin_pb2.DataChunk.STDOUT, result.stdout),
            (plugin_pb2.DataChunk.STDERR, result.stderr),
        ):
            if not payload:
                continue
            data = payload.encode() if isinstance(payload, str) else payload
            yield plugin_pb2.ExecOutput(
                data=plugin_pb2.DataChunk(stream=stream, data=data),
            )

        yield plugin_pb2.ExecOutput(
            exec_complete=plugin_pb2.ExecComplete(exit_code=result.exit_code),
        )

    def CopyIn(  # noqa: N802
        self,
        request_iterator: Iterator[plugin_pb2.CopyInChunk],
        context: grpc.ServicerContext,
    ) -> plugin_pb2.CopyInResponse:
        first = next(request_iterator, None)
        if first is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "CopyIn: empty stream")
            raise AssertionError("unreachable")
        if not first.HasField("environment_id") or not first.HasField("dest_path"):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "CopyIn: first chunk must set environment_id and dest_path",
            )

        instance = self._instance(context, first.environment_id)
        dest_path = first.dest_path

        buf = io.BytesIO()
        if first.data:
            buf.write(first.data)
        for chunk in request_iterator:
            if chunk.HasField("environment_id") or chunk.HasField("dest_path"):
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "CopyIn: environment_id/dest_path only on first chunk",
                )
            buf.write(chunk.data)
        buf.seek(0)

        with tempfile.TemporaryDirectory() as tmp:
            try:
                with tarfile.open(fileobj=buf, mode="r|*") as tar:
                    tar.extractall(tmp)  # noqa: S202 — trusted runner input
            except tarfile.TarError as exc:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CopyIn: bad tar: {exc}")

            try:
                # Ensure the destination directory exists before pushing.
                instance.execute(["mkdir", "-p", dest_path])
                instance.files.recursive_put(tmp, dest_path)
            except LXDAPIException as exc:
                context.abort(grpc.StatusCode.INTERNAL, f"CopyIn: {exc}")

        return plugin_pb2.CopyInResponse()

    def CopyOut(  # noqa: N802
        self,
        request: plugin_pb2.CopyOutRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.CopyOutChunk]:
        instance = self._instance(context, request.environment_id)
        src = request.src_path

        with tempfile.TemporaryDirectory() as tmp:
            try:
                instance.files.recursive_get(src, tmp)
            except LXDAPIException as exc:
                context.abort(grpc.StatusCode.INTERNAL, f"CopyOut: {exc}")

            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w") as tar:
                for child in Path(tmp).iterdir():
                    tar.add(child, arcname=child.name)
            buf.seek(0)

        while True:
            data = buf.read(_COPY_CHUNK_SIZE)
            if not data:
                break
            yield plugin_pb2.CopyOutChunk(data=data)

    def Remove(  # noqa: N802
        self,
        request: plugin_pb2.RemoveRequest,
        context: grpc.ServicerContext,
    ) -> plugin_pb2.RemoveResponse:
        env_id = request.environment_id
        with self._lock:
            env = self._envs.pop(env_id, None)
        # Idempotent: Remove after a failed Create, or a duplicate teardown,
        # should not raise.
        if env is None:
            return plugin_pb2.RemoveResponse()

        try:
            instance = self._client.instances.get(env.instance_name)
        except NotFound:
            return plugin_pb2.RemoveResponse()

        try:
            if instance.status_code != _LXD_STATUS_STOPPED:
                instance.stop(force=True, wait=True)
            instance.delete(wait=True)
        except LXDAPIException as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd remove: {exc}")

        log.info("removed environment %s", env_id)
        return plugin_pb2.RemoveResponse()
