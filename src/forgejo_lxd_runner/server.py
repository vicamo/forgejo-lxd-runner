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

_IMAGE_ENV_PREFIX = "environment."


def _image_env_from_instance(instance: Any) -> dict[str, str]:
    """Extract image-baked env vars from an LXD instance.

    LXD surfaces environment variables baked into the image (via the image's
    own metadata, plus any layered profiles) as ``environment.<NAME>`` keys
    on the instance's ``expanded_config``. We strip the prefix and hand
    the resulting map to the runner as ``StartComplete.image_env`` so job
    env vars can be layered on top of them.
    """
    config = getattr(instance, "expanded_config", None) or {}
    return {
        key[len(_IMAGE_ENV_PREFIX) :]: str(value)
        for key, value in config.items()
        if key.startswith(_IMAGE_ENV_PREFIX) and key != _IMAGE_ENV_PREFIX
    }


_COPY_CHUNK_SIZE = 256 * 1024


# Map LXD architecture names (kernel / ``uname -m`` style) to the values GitHub
# Actions exposes as ``RUNNER_ARCH`` and ``runner.arch``. GHA inherits its
# vocabulary from the .NET ``System.Runtime.InteropServices.Architecture``
# enum via the Azure Pipelines agent, so we map every value the enum defines
# and pass everything else through untouched (best-effort — LXD may run on
# platforms .NET has no name for).
#
# .NET enum reference (all values across .NET versions):
#   https://learn.microsoft.com/dotnet/api/system.runtime.interopservices.architecture
# LXD architecture names come from ``shared/osarch/architectures.go``:
#   https://github.com/canonical/lxd/blob/main/shared/osarch/architectures.go
# GHA ``RUNNER_ARCH`` contract:
#   https://docs.github.com/actions/learn-github-actions/variables#default-environment-variables
_LXD_ARCH_TO_GHA: dict[str, str] = {
    # .NET: X86 (Core 1.0) — 32-bit x86
    "i686": "X86",
    "i386": "X86",
    # .NET: X64 (Core 1.0) — 64-bit x86 / amd64 / x86_64
    "x86_64": "X64",
    # .NET: Arm (Core 1.0) — 32-bit ARMv7
    "armv7l": "ARM",
    # .NET: Armv6 (.NET 7) — 32-bit ARMv6 (e.g. Raspberry Pi Zero). GHA has
    # no separate token; RUNNER_ARCH lumps this under ARM.
    "armv6l": "ARM",
    # .NET: Arm64 (Core 3.0) — 64-bit ARM / AArch64
    "aarch64": "ARM64",
    # .NET: S390x (.NET 6) — IBM Z, big-endian
    "s390x": "S390x",
    # .NET: Ppc64le (.NET 7) — 64-bit little-endian POWER
    "ppc64le": "Ppc64le",
    # .NET: LoongArch64 (.NET 7)
    "loongarch64": "LoongArch64",
    # .NET: RiscV64 (.NET 8)
    "riscv64": "RiscV64",
    # .NET: Wasm (.NET 5) — WebAssembly. LXD never reports this, but included
    # for completeness so the mapping mirrors the enum 1:1.
    "wasm32": "Wasm",
    "wasm64": "Wasm",
}


def _lxd_arch_to_gha(lxd_arch: str) -> str:
    """Translate an LXD architecture string into GHA's ``RUNNER_ARCH`` value.

    Unknown architectures pass through unchanged — they still populate
    ``RUNNER_ARCH`` and ``runner.arch``, which is more useful than an empty
    string for workflows that grew their own detection.
    """
    return _LXD_ARCH_TO_GHA.get(lxd_arch, lxd_arch)


# Map LXD's ``image.os`` metadata property to GHA's ``RUNNER_OS`` /
# ``runner.os`` value. GHA inherits its vocabulary from the .NET
# ``System.Runtime.InteropServices.OSPlatform`` type (``Linux``, ``Windows``,
# ``OSX``, ``FreeBSD``), matching what GitHub-hosted runners set.
#
# .NET reference:
#   https://learn.microsoft.com/dotnet/api/system.runtime.interopservices.osplatform
# LXD image metadata (``os`` property comes from simplestreams and image.yaml):
#   https://documentation.ubuntu.com/lxd/latest/reference/image_format/
# GHA ``RUNNER_OS`` contract:
#   https://docs.github.com/actions/learn-github-actions/variables#default-environment-variables
#
# The set of non-Linux OSes LXD actually supports is tiny: FreeBSD (container
# or VM) and Windows (VM only). Everything else — ubuntu, debian, alpine,
# arch, fedora, centos, rocky, almalinux, opensuse, void, nixos, gentoo,
# oracle, openwrt, plamo, slackware — is Linux, so we default to that.
_LXD_OS_TO_GHA: dict[str, str] = {
    "freebsd": "FreeBSD",
    "windows": "Windows",
}


def _lxd_os_to_gha(image_os: str) -> str:
    """Translate LXD ``image.os`` to GHA's ``RUNNER_OS`` value.

    Defaults to ``Linux`` — the overwhelming majority of LXD images, and
    the safe fallback when metadata is missing on custom images.
    """
    return _LXD_OS_TO_GHA.get(image_os.lower(), "Linux")


class _Env:
    """Per-environment state tracked by the plugin."""

    __slots__ = ("instance_name", "project")

    def __init__(self, instance_name: str, project: str | None = None) -> None:
        self.instance_name = instance_name
        self.project = project


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
        self._envs: dict[str, _Env] = {}
        self._lock = threading.Lock()
        # One pylxd Client per project (pylxd binds ``project`` at Client
        # construction time, not per-call). ``None`` keys the default
        # client — created lazily on the first RPC, which is always
        # ``Capabilities`` right after the plugin starts.
        self._clients: dict[str | None, pylxd.Client] = {}

    def _client_for(self, project: str | None) -> pylxd.Client:
        key = project or None
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = pylxd.Client(project=project) if project else pylxd.Client()
                self._clients[key] = client
        return client

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
            return self._client_for(env.project).instances.get(env.instance_name)
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

        config: dict[str, object] = {
            "name": name,
            "source": {"type": "image", "alias": image},
        }
        # ``lxd_arch`` backend option: forces the LXD instance architecture
        # (LXD vocabulary, e.g. ``x86_64`` / ``aarch64``). When absent, LXD
        # picks it from the image. Unknown values are passed through so LXD
        # can validate against its own architecture list and produce a
        # descriptive error.
        if lxd_arch := request.backend_options.get("lxd_arch"):
            config["architecture"] = lxd_arch
        # ``type`` backend option: LXD instance type — ``container`` (default)
        # or ``virtual-machine``. LXD vocabulary; unknown values are
        # forwarded so LXD produces the descriptive error.
        if instance_type := request.backend_options.get("type"):
            config["type"] = instance_type
        # ``profiles`` backend option: comma-separated list of LXD profile
        # names to apply. Absent (or empty after parsing) → LXD applies the
        # ``default`` profile, which is what most single-project setups want.
        # Explicit ``profiles: ""`` is treated as absence rather than "no
        # profiles" (an empty list disables the root disk and network).
        profiles_raw = request.backend_options.get("profiles", "")
        profiles = [p.strip() for p in profiles_raw.split(",") if p.strip()]
        if profiles:
            config["profiles"] = profiles
        # ``project`` backend option: create the instance inside the named
        # LXD project (features.* on the project decide isolation scope).
        # When absent, pylxd's default client stays in whatever project it
        # discovered — usually ``default``.
        project = request.backend_options.get("project") or None
        client = self._client_for(project)
        try:
            instance = client.instances.create(config, wait=True)
        except LXDAPIException as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd create {image!r}: {exc}")
            raise AssertionError("unreachable") from exc

        with self._lock:
            self._envs[name] = _Env(instance_name=instance.name, project=project)

        log.info("created environment %s from image %s", name, image)

        # TODO: expose a knob for the paths.
        return plugin_pb2.CreateResponse(
            environment_id=name,
            root_path="/root/actions-runner",
            act_path="/root/actions-runner/act",
            tool_cache_path="/opt/hostedtoolcache",
            temp_path="/tmp",
            os=_lxd_os_to_gha(instance.expanded_config.get("image.os", "")),
            arch=_lxd_arch_to_gha(instance.architecture),
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
        image_env = _image_env_from_instance(instance)
        yield plugin_pb2.StartOutput(start_complete=plugin_pb2.StartComplete(image_env=image_env))

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
            instance = self._client_for(env.project).instances.get(env.instance_name)
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
