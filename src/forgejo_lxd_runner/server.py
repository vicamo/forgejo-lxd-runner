"""BackendPlugin gRPC service — LXD / Incus-backed implementation.

Commit 1 (MVP): the plugin respects only ``CreateRequest.label_arg`` (the
image reference) and lets the daemon supply every other default —
project, profile, user, network, storage. Later commits add options one
at a time.

Known simplifications, all called out in code:

* ``CopyIn`` / ``CopyOut`` buffer the whole tar archive in memory. Fine
  for typical workflow payloads; a streaming rewrite is a later commit.
* ``CreateResponse`` reports a hardcoded filesystem layout and
  ``os=Linux`` / ``arch=X64`` (the GHA ``RUNNER_OS`` / ``RUNNER_ARCH``
  vocabulary). Discovery from the LXD image metadata is a later commit.
"""

from __future__ import annotations

import io
import json
import logging
import os
import tarfile
import threading
from collections.abc import Iterator

import grpc
import httpx

from .client import BackendClient, BackendOperationError
from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc

log = logging.getLogger(__name__)

# LXD / Incus instance status codes we care about.
_STATUS_RUNNING = 103

# CopyOut yields the tar body in fixed-size gRPC chunks.
_COPY_CHUNK_SIZE = 256 * 1024


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

        env = self._lookup(context, first.environment_id)
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

        try:
            with tarfile.open(fileobj=buf, mode="r|*") as tar:
                self._push_tar(env.instance_name, dest_path, tar)
        except tarfile.TarError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"CopyIn: bad tar: {exc}")
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"CopyIn: {exc}")

        return plugin_pb2.CopyInResponse()

    def _push_tar(self, instance: str, dest_path: str, tar: tarfile.TarFile) -> None:
        """Walk ``tar`` and replay each entry onto ``instance:dest_path``.

        Directories become ``X-*-type: directory`` POSTs, files carry
        their bytes with the recorded mode, symlinks store their target
        as the body. Hardlinks and other exotic types are ignored — CI
        payloads (build inputs, source trees) never carry them.
        """

        # Make sure the destination directory exists first.
        self._client.push_directory(instance, dest_path)

        for member in tar:
            # ``member.name`` is a relative path inside the tarball.
            target = os.path.join(dest_path, member.name).replace(os.sep, "/")
            if member.isdir():
                self._client.push_directory(instance, target)
            elif member.isfile():
                extracted = tar.extractfile(member)
                data = extracted.read() if extracted is not None else b""
                self._client.push_file(instance, target, data, mode=member.mode or 0o644)
            elif member.issym():
                self._client.push_symlink(instance, target, member.linkname)
            # Anything else (block/char/fifo/hardlink) is silently skipped.

    def CopyOut(  # noqa: N802
        self,
        request: plugin_pb2.CopyOutRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.CopyOutChunk]:
        env = self._lookup(context, request.environment_id)
        src = request.src_path

        buf = io.BytesIO()
        try:
            with tarfile.open(fileobj=buf, mode="w") as tar:
                self._pull_into_tar(env.instance_name, src, tar, arcbase="")
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"CopyOut: {exc}")

        buf.seek(0)
        while True:
            data = buf.read(_COPY_CHUNK_SIZE)
            if not data:
                break
            yield plugin_pb2.CopyOutChunk(data=data)

    def _pull_into_tar(self, instance: str, src: str, tar: tarfile.TarFile, arcbase: str) -> None:
        """Recursively fetch ``src`` from ``instance`` and append to ``tar``.

        Directory listings come back as JSON arrays; files and symlinks
        return their content directly (with ``X-*-type`` in the response
        headers telling us which). The recursion mirrors what a plain
        ``recursive_get`` used to do — one REST round-trip per entry.
        """

        kind, data, mode = self._client.pull_file(instance, src)
        base = os.path.basename(src.rstrip("/")) or src
        arcname = os.path.join(arcbase, base).replace(os.sep, "/") if arcbase else base

        if kind == "directory":
            info = tarfile.TarInfo(name=arcname)
            info.type = tarfile.DIRTYPE
            info.mode = mode or 0o755
            tar.addfile(info)
            for entry in json.loads(data.decode() or "[]"):
                child = f"{src.rstrip('/')}/{entry}"
                self._pull_into_tar(instance, child, tar, arcbase=arcname)
        elif kind == "symlink":
            info = tarfile.TarInfo(name=arcname)
            info.type = tarfile.SYMTYPE
            info.linkname = data.decode()
            tar.addfile(info)
        else:  # "file"
            info = tarfile.TarInfo(name=arcname)
            info.size = len(data)
            info.mode = mode or 0o644
            tar.addfile(info, io.BytesIO(data))

    def Remove(  # noqa: N802
        self, request: plugin_pb2.RemoveRequest, context: grpc.ServicerContext
    ) -> plugin_pb2.RemoveResponse:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "Remove not implemented")
