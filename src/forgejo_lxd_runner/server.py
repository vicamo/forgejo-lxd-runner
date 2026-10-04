"""BackendPlugin gRPC service — LXD / Incus-backed implementation.

Commit 1 (MVP): the plugin respects only ``CreateRequest.label_arg`` (the
image reference) and lets the daemon supply every other default —
project, profile, user, network, storage. Later commits add options one
at a time.

Known simplifications, all called out in code:

* ``CopyIn`` / ``CopyOut`` buffer the whole tar archive in memory. Fine
  for typical workflow payloads; a streaming rewrite is a later commit.
* ``CreateResponse`` reports a hardcoded filesystem layout and a
  hardcoded ``os=Linux``. The architecture is discovered from the LXD
  instance record; deriving the OS from the image metadata, and exposing
  the paths as backend options, are later commits.
"""

from __future__ import annotations

import io
import json
import logging
import os
import tarfile
import threading
import uuid
from collections.abc import Iterator

import grpc
import httpx

from .client import BackendClient, BackendOperationError
from .executor import (
    ContainerExecutor,
    Executor,
    ExecutorError,
    Service,
    ServiceSet,
    mount_specs,
    resolve,
)
from .proto.plugin.v1alpha import plugin_pb2, plugin_pb2_grpc

log = logging.getLogger(__name__)

# LXD / Incus instance status codes we care about.
_STATUS_RUNNING = 103

# CopyOut yields the tar body in fixed-size gRPC chunks.
_COPY_CHUNK_SIZE = 256 * 1024

# The filesystem layout promised to Forgejo in ``CreateResponse``.
# TODO: discover os/arch and expose a knob for these.
_ROOT_PATH = "/root/actions-runner"
_ACT_PATH = "/root/actions-runner/act"
_TOOL_CACHE_PATH = "/opt/hostedtoolcache"
_TEMP_PATH = "/tmp"

#: Bind-mounted into a job container at identical paths, so the layout
#: above stays true inside it and CopyIn/CopyOut need no translation.
_JOB_CONTAINER_MOUNTS = (_ROOT_PATH, _TOOL_CACHE_PATH, _TEMP_PATH)

#: Prefix for a job's per-instance network. A bridge network's name
#: becomes the host's Linux bridge interface, capped at 15 characters,
#: so the name is this prefix plus 10 hex digits of randomness (14
#: total) rather than the much longer instance name.
_NETWORK_PREFIX = "flr-"


def _network_name() -> str:
    """Return a fresh <=15-char name for a job's isolated bridge."""
    return f"{_NETWORK_PREFIX}{uuid.uuid4().hex[:10]}"


# Map LXD architecture names (kernel / ``uname -m`` style) to the values GitHub
# Actions exposes as ``RUNNER_ARCH`` and ``runner.arch``. GHA inherits its
# vocabulary from the .NET ``System.Runtime.InteropServices.Architecture``
# enum via the Azure Pipelines agent, so we map every value the enum defines
# and pass everything else through untouched (best-effort -- LXD may run on
# platforms .NET has no name for).
#
# .NET enum reference (all values across .NET versions):
#   https://learn.microsoft.com/dotnet/api/system.runtime.interopservices.architecture
# LXD architecture names come from ``shared/osarch/architectures.go``:
#   https://github.com/canonical/lxd/blob/main/shared/osarch/architectures.go
# GHA ``RUNNER_ARCH`` contract:
#   https://docs.github.com/actions/learn-github-actions/variables#default-environment-variables
_LXD_ARCH_TO_GHA: dict[str, str] = {
    # .NET: X86 (Core 1.0) -- 32-bit x86
    "i686": "X86",
    "i386": "X86",
    # .NET: X64 (Core 1.0) -- 64-bit x86 / amd64 / x86_64
    "x86_64": "X64",
    # .NET: Arm (Core 1.0) -- 32-bit ARMv7
    "armv7l": "ARM",
    # .NET: Armv6 (.NET 7) -- 32-bit ARMv6 (e.g. Raspberry Pi Zero). GHA has
    # no separate token; RUNNER_ARCH lumps this under ARM.
    "armv6l": "ARM",
    # .NET: Arm64 (Core 3.0) -- 64-bit ARM / AArch64
    "aarch64": "ARM64",
    # .NET: S390x (.NET 6) -- IBM Z, big-endian
    "s390x": "S390x",
    # .NET: Ppc64le (.NET 7) -- 64-bit little-endian POWER
    "ppc64le": "Ppc64le",
    # .NET: LoongArch64 (.NET 7)
    "loongarch64": "LoongArch64",
    # .NET: RiscV64 (.NET 8)
    "riscv64": "RiscV64",
    # .NET: Wasm (.NET 5) -- WebAssembly. LXD never reports this, but included
    # for completeness so the mapping mirrors the enum 1:1.
    "wasm32": "Wasm",
    "wasm64": "Wasm",
}


def _lxd_arch_to_gha(lxd_arch: str) -> str:
    """Translate an LXD architecture string into GHA's ``RUNNER_ARCH`` value.

    Unknown architectures pass through unchanged -- they still populate
    ``RUNNER_ARCH`` and ``runner.arch``, which is more useful than an empty
    string for workflows that grew their own detection.
    """
    return _LXD_ARCH_TO_GHA.get(lxd_arch, lxd_arch)


class _Env:
    """Per-environment state tracked by the plugin.

    Lifetime is bounded exclusively by ``Create`` -> ``Remove``. Every
    other RPC (``Start``, ``Exec``, ``CopyIn``, ``CopyOut``) reads
    ``_envs`` but never mutates it, even when the daemon reports the
    instance as gone (HTTP 404) or otherwise unreachable. Two reasons:

    * The plugin-Forgejo contract is: Forgejo creates and Forgejo
      removes. Silently dropping the record on a mid-RPC 404 would
      desync the two views of the world — Forgejo still thinks the
      env exists and will eventually call ``Remove`` on it. ``Remove``
      is idempotent, so that's not fatal, but the desync buys nothing
      in exchange.
    * A transient error that happens to surface as 404 must not
      silently invalidate a running job's environment record.

    An instance that disappears out-of-band (operator intervention,
    daemon reset) is an invariant violation the plugin surfaces
    loudly as ``INTERNAL`` on the affected RPC. The caller should
    respond by issuing ``Remove`` for that ``environment_id``.
    """

    __slots__ = ("executor", "instance_name", "job_image", "network", "services")

    def __init__(
        self,
        instance_name: str,
        executor: Executor,
        job_image: str = "",
        services: ServiceSet | None = None,
        network: str = "",
    ) -> None:
        self.instance_name = instance_name
        #: The workflow's ``container.image``, empty when the job has no
        #: ``container:`` block — the common case. When set, the job runs
        #: inside this image on the instance rather than on the instance
        #: directly.
        self.job_image = job_image
        #: Where this job's commands run, decided once by ``resolve()``
        #: before the environment is recorded and never reassigned. An
        #: environment with no executor is not a state worth modelling:
        #: it could only exist between construction and the fixup that
        #: replaced it, and anything reading it there would silently run
        #: the job in the wrong place.
        self.executor = executor
        #: The job's service containers and the network they share, or
        #: ``None`` when the workflow declared no ``services:``. Kept so
        #: ``Remove`` can tear them down: they outlive every other RPC,
        #: since a step may connect to one at any point.
        self.services = services
        #: The job's own managed bridge, created by ``Create`` so the
        #: instance cannot reach any other job's. Kept so ``Remove`` can
        #: delete it once the instance that used it is gone.
        self.network = network


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

    def _start_services(
        self,
        name: str,
        executor: Executor,
        requested: list[plugin_pb2.ServiceContainer],
    ) -> ServiceSet | None:
        """Bring a job's ``services:`` up, or return ``None`` if it has none.

        Services are containers whether or not the job itself is one, so
        they run on the runtime ``resolve()`` already found for the
        instance.

        A containerised job reaches a service by name, so its services
        share a user-defined network -- named after the instance, which
        is already unique per environment -- that the job container
        joins. A job on the instance reaches a service at ``localhost``
        on its published port, so its services need no network of their
        own. Isolation between jobs is the instance's concern, not this
        network's.
        """
        if not requested:
            return None

        if not executor.runtime:
            raise ExecutorError(
                f"no container runtime in instance {name!r}: the job's "
                "`services:` need one inside the instance. Apply a runtime "
                'profile (e.g. `profiles: "base,docker"`) or use an instance '
                "image that ships one",
                precondition=True,
            )

        services = ServiceSet(
            client=self._client,
            instance=name,
            runtime=executor.runtime,
            network=name if isinstance(executor, ContainerExecutor) else "",
        )
        services.create(
            [
                Service(
                    name=service.name,
                    image=service.image,
                    env=dict(service.env),
                    ports=list(service.ports),
                )
                for service in requested
            ]
        )
        return services

    def _read_env(self, env: _Env) -> dict[str, str]:
        """Return the environment a step of ``env`` inherits.

        Read at runtime by running ``env`` rather than from image
        metadata: ``inspect`` reports what an image *declares*, which is
        nothing for an image that bakes no ``ENV`` at all, while the
        process a step actually runs always has a ``PATH``. The
        executor's ``wrap()`` decides where that is, so the same probe
        answers for a container and for the bare instance.

        A failure is not worth losing a job over -- the runner falls
        back to its own default ``PATH`` when the mapping is empty.
        """
        command, environment, user, cwd = env.executor.wrap(["env"])
        try:
            rc, out, _ = self._client.exec_capture(
                env.instance_name,
                command,
                environment=environment,
                user=user,
                cwd=cwd,
            )
        except (httpx.HTTPError, BackendOperationError):
            log.exception("failed to read the environment of %s", env.instance_name)
            return {}
        if rc != 0:
            return {}

        read: dict[str, str] = {}
        for line in out.splitlines():
            key, sep, value = line.partition("=")
            if sep:
                read[key] = value
        return read

    def _discard(self, name: str, network: str) -> None:
        """Delete an instance Create is about to abandon, and its network.

        Best-effort on purpose: the caller is already failing and the
        gRPC error it is about to raise describes the real problem.
        Letting a cleanup error replace it would hide the cause, so a
        failure here is logged and swallowed -- an instance that outlives
        a failed Create is a smaller problem than an unreportable one.

        The instance goes first: the network cannot be deleted while the
        instance still references it.
        """
        try:
            self._client.remove_instance(name)
        except (httpx.HTTPError, BackendOperationError):
            log.exception("failed to remove instance %s after a failed Create", name)
        self._discard_network(network)

    def _discard_network(self, network: str) -> None:
        """Delete a job's network, best-effort, logging any failure.

        Split from :meth:`_discard` because a launch that never created
        the instance still has a network to reclaim. An empty name means
        no network was ever created, so there is nothing to reclaim.
        """
        if not network:
            return
        try:
            self._client.remove_network(network)
        except (httpx.HTTPError, BackendOperationError):
            log.exception("failed to remove network %s", network)

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

        # Every job gets its own bridge so one job's instance cannot
        # reach another's -- isolation is the instance's network, not
        # anything inside it. NAT stays on so package installs at boot
        # still work; IPv6 is turned off to avoid the AAAA-timeout trap
        # an unroutable ULA causes (see examples/profiles/base.yaml).
        network = _network_name()
        try:
            self._client.create_network(
                network,
                description=f"forgejo-lxd-runner job {name}",
                config={"ipv4.nat": "true", "ipv6.address": "none"},
            )
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd network create: {exc}")
            raise AssertionError("unreachable") from exc

        # ``start: true`` makes the daemon create *and* boot the instance in
        # a single operation. Create is the runner's ``docker create``: it
        # must leave behind an environment every later RPC can talk to, and
        # the only way to probe anything inside the instance is to have it
        # running by the time Create returns.
        config: dict[str, object] = {
            "name": name,
            "source": {"type": "image", "alias": image},
            "start": True,
            # Put the instance's NIC on its own network, overriding any
            # ``eth0`` an applied profile supplies.
            "devices": {"eth0": {"type": "nic", "network": network}},
        }
        # ``profiles`` backend option: comma-separated list of LXD profile
        # names to apply. Absent (or empty after parsing) -> LXD applies the
        # ``default`` profile, which is what most single-project setups want.
        # Explicit ``profiles: ""`` is treated as absence rather than "no
        # profiles" (an empty list disables the root disk and network).
        profiles_raw = request.backend_options.get("profiles", "")
        profiles = [p.strip() for p in profiles_raw.split(",") if p.strip()]
        if profiles:
            config["profiles"] = profiles
        try:
            self._client.launch_instance(config)
        except (httpx.HTTPError, BackendOperationError) as exc:
            # The instance never came up, so nothing references the
            # network yet -- drop it so a failed Create leaves nothing.
            self._discard_network(network)
            context.abort(grpc.StatusCode.INTERNAL, f"lxd create {image!r}: {exc}")
            raise AssertionError("unreachable") from exc

        # The instance is running by now, so the runtime inside it can
        # answer a probe and the job container can be created. Anything
        # that fails from here on has to delete the instance first:
        # Create never returned an environment_id, so Forgejo does not
        # know there is anything to Remove, and the instance would be
        # left behind for an operator to find.
        try:
            executor = resolve(self._client, name, request.image)
            services = self._start_services(name, executor, list(request.services))
            executor.create(
                f"{name}-job",
                workdir=_ROOT_PATH,
                mounts=mount_specs(list(_JOB_CONTAINER_MOUNTS)),
                network=services.network if services else "",
            )
        except ExecutorError as exc:
            self._discard(name, network)
            # A missing runtime is the operator's to fix in the Forgejo
            # config; an unresolvable image is the workflow's.
            code = (
                grpc.StatusCode.FAILED_PRECONDITION
                if exc.precondition
                else grpc.StatusCode.INVALID_ARGUMENT
            )
            context.abort(code, str(exc))
        except (httpx.HTTPError, BackendOperationError) as exc:
            self._discard(name, network)
            context.abort(grpc.StatusCode.INTERNAL, f"job container: {exc}")

        # The instance record carries the architecture the daemon settled
        # on, in LXD vocabulary; map it to GHA's RUNNER_ARCH. A failure
        # here is an instance we cannot describe, so tear it down like any
        # other post-launch failure.
        try:
            architecture = str(self._client.get_instance(name).get("architecture", ""))
        except (httpx.HTTPError, BackendOperationError) as exc:
            self._discard(name, network)
            context.abort(grpc.StatusCode.INTERNAL, f"lxd instance fetch: {exc}")
            raise AssertionError("unreachable") from exc

        with self._lock:
            self._envs[name] = _Env(
                instance_name=name,
                executor=executor,
                job_image=request.image,
                services=services,
                network=network,
            )

        log.info("created environment %s from image %s on %s", name, image, executor)

        # TODO: discover os and expose a knob for the paths.
        return plugin_pb2.CreateResponse(
            environment_id=name,
            root_path=_ROOT_PATH,
            act_path=_ACT_PATH,
            tool_cache_path=_TOOL_CACHE_PATH,
            temp_path=_TEMP_PATH,
            os="Linux",
            arch=_lxd_arch_to_gha(architecture),
        )

    def Start(  # noqa: N802
        self,
        request: plugin_pb2.StartRequest,
        context: grpc.ServicerContext,
    ) -> Iterator[plugin_pb2.StartOutput]:
        env = self._lookup(context, request.environment_id)
        name = env.instance_name
        # Create launches the instance with ``start: true``, so by the
        # time Forgejo calls Start there is nothing left to bring up —
        # this is the ``docker start`` of an already-running container.
        # All that remains is to confirm the environment is still usable
        # and fail loudly if it is not, rather than letting the first
        # Exec report a confusing error.
        try:
            state = self._client.get_instance_state(name)
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd start: {exc}")
        if state.get("status_code") != _STATUS_RUNNING:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"instance {name!r} is not running (status {state.get('status')!r})",
            )

        # The job container was created by Create; all that is left is to
        # start it. Unlike the instance, it is not started on creation --
        # `docker create` leaves it stopped, which is exactly the split
        # this RPC pair exists to express.
        try:
            env.executor.start()
        except ExecutorError as exc:
            context.abort(grpc.StatusCode.INTERNAL, str(exc))
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"job container: {exc}")

        log.info("started environment %s on %s", request.environment_id, env.executor)
        yield plugin_pb2.StartOutput(
            start_complete=plugin_pb2.StartComplete(image_env=self._read_env(env)),
        )

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
            command, environ, uid, cwd = env.executor.wrap(
                list(request.command),
                environment=environ,
                user=uid,
                cwd=cwd,
            )
            for kind, payload in self._client.exec_stream(
                env.instance_name,
                command,
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

        # Create the destination and every missing level above it. The
        # daemon's file API is ``mkdir``, not ``mkdir -p``: pushing a
        # directory whose parent is absent fails with 404, and a plain
        # instance has nothing under ``/root`` yet. Existing levels are
        # harmless to re-push, so walk top-down unconditionally rather
        # than probing each one first.
        self._push_directory_p(instance, dest_path)

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

    def _push_directory_p(self, instance: str, path: str) -> None:
        """Create ``path`` and any missing parents, ``mkdir -p`` style."""

        parts = [p for p in path.strip("/").split("/") if p]
        for i in range(len(parts)):
            self._client.push_directory(instance, "/" + "/".join(parts[: i + 1]))

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
        headers telling us which), so one ``pull_file`` call both
        classifies an entry and fetches it — no separate stat round-trip.
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
            log.debug("remove: environment %s already gone from map", env_id)
            return plugin_pb2.RemoveResponse()

        name = env.instance_name
        # Tear the container down first: deleting the instance would take
        # it with it, but only the runtime can report a container that
        # refused to die, and that is worth a log line before the
        # evidence is destroyed.
        try:
            env.executor.cleanup()
        except (httpx.HTTPError, BackendOperationError):
            log.exception("failed to remove the job container of %s", name)

        if env.services is not None:
            # After the job container, which was attached to their
            # network: it has to leave before the network can go.
            try:
                env.services.cleanup()
            except (httpx.HTTPError, BackendOperationError):
                log.exception("failed to remove the services of %s", name)

        try:
            self._client.remove_instance(name)
        except (httpx.HTTPError, BackendOperationError) as exc:
            context.abort(grpc.StatusCode.INTERNAL, f"lxd remove: {exc}")

        # The instance is gone, so nothing references its network now;
        # delete it. A failure here must not fail Remove -- the instance,
        # the thing Forgejo cares about, is already gone -- so it is
        # logged rather than raised. An env injected without a network
        # (e.g. a test) has an empty name, which is a no-op.
        self._discard_network(env.network)

        log.info("removed environment %s", env_id)
        return plugin_pb2.RemoveResponse()
