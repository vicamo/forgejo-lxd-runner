"""Where a job's commands run: on the instance, or in a container on it.

A job has exactly one execution context. Usually that is the instance
itself; when a workflow sets ``jobs.<id>.container.image`` it is a
container on that instance instead — GitHub's model, where ``runs-on:``
picks the machine and ``container:`` optionally containerises the job on
it. Running on the instance is not the absence of a container, it is the
default context, and ``actions/runner`` treats it the same way
(``container == null`` means "run on the host", with
``RequireJobContainer`` for admins who forbid it).

So both are one interface. :func:`resolve` returns a
:class:`HostExecutor` or a :class:`ContainerExecutor`, and the RPCs call
the same four methods either way — no branching on whether an image was
set, and no path where the host case is the one somebody forgot.

The nested container is started idle (``tail -f /dev/null``) and each
step is ``exec``'d into it, mirroring the runner's
``ContainerOperationProvider``, which sets ``ContainerEntryPoint =
"tail"`` for exactly this reason: steps arrive one at a time, so the
container must outlive any single command.

Bind-mounts use *identical* paths on both sides. That is what keeps
``CopyIn`` / ``CopyOut`` unaware of any of this: they write to the
instance, and the container sees the result at the same path.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field

from .client import BackendClient

#: Runtimes we know how to drive, in probe order. Both are CLI-compatible
#: for the verbs used here (``pull`` / ``run`` / ``exec`` / ``rm``), so
#: selection is all that differs. Docker first: when an image ships both,
#: it is the one with a daemon already running.
_RUNTIMES = ("docker", "podman")

#: Docker's socket is not accepting connections the instant the instance
#: reports Running — systemd still has to start the unit. Podman has no
#: daemon, so its probe succeeds immediately and this budget is unused.
_DAEMON_READY_TIMEOUT = 60.0
_DAEMON_POLL_INTERVAL = 2.0

#: A virtual-machine reports Running once the hypervisor is up, but its
#: exec/file endpoints are served by the agent *inside* the guest, which
#: is not connected until the guest OS boots -- until then the daemon
#: answers exec with 404. Instance state reports ``processes`` as ``-1``
#: while no agent is connected and a real count once one is, so the same
#: wait covers a cold VM boot and is a no-op for a container (whose agent
#: is the daemon itself and reports ready at once).
_AGENT_READY_TIMEOUT = 120.0
_AGENT_READY_POLL = 2.0

#: Default budget for the operator-supplied ``system-ready`` command. A
#: first-boot provisioning run (cloud-init installing a container runtime
#: and its dependencies) is minutes, not seconds, so the default is
#: generous; operators tune it per label with ``system-ready-timeout``.
_SYSTEM_READY_TIMEOUT = 600.0
_SYSTEM_READY_POLL = 5.0

#: Keeps the container alive without running anything, so steps can be
#: exec'd into it one at a time. Matches what the runner uses for its own
#: job containers.
_ENTRY_POINT = ("tail", "-f", "/dev/null")

#: Matches the runner's own alias rule (`sanitizeNetworkAlias`): a
#: service is reached by its workflow name, so the name we give the
#: container has to survive the same transformation the runner would
#: have applied on a Docker backend.
_ALIAS_UNSAFE = re.compile("[^a-z0-9-]")


class ExecutorError(RuntimeError):
    """A job's execution context could not be prepared.

    ``precondition`` distinguishes operator misconfiguration (no runtime
    inside the instance — the caller maps it to ``FAILED_PRECONDITION``)
    from a bad workflow input such as an unresolvable image reference
    (mapped to ``INVALID_ARGUMENT``).
    """

    def __init__(self, message: str, *, precondition: bool = False) -> None:
        super().__init__(message)
        self.precondition = precondition


@dataclass
class HostExecutor:
    """Runs a job's commands on the instance itself.

    The default when a workflow has no ``container:`` block. Every method
    is a no-op or an identity: there is nothing to pull, nothing to
    start, no image environment to inherit, and nothing to tear down
    that ``Remove`` does not already handle by deleting the instance.
    """

    client: BackendClient
    instance: str
    #: The container runtime available inside the instance, or ``""``
    #: when it ships none. Unused by this executor -- a job running on
    #: the instance needs no runtime to run its own steps -- but a job
    #: can still want one for what runs *alongside* it.
    runtime: str = ""
    #: The LXD project the instance lives in, or ``None`` for the
    #: daemon's default. Every instance exec this executor issues must
    #: carry it, or the daemon looks for the instance in the wrong
    #: project and reports it missing.
    project: str | None = None

    def create(
        self,
        name: str,
        *,
        workdir: str,
        mounts: list[str],
        network: str = "",
        cap_add: list[str] | None = None,
        cap_drop: list[str] | None = None,
    ) -> None:
        """Nothing to create — the instance is the execution context.

        ``mounts`` is irrelevant here: the paths the caller would bind
        are already the instance's own filesystem. So is ``network``:
        a job on the instance reaches its services over the instance's
        own stack, by the ports they publish onto it.

        ``cap_add`` / ``cap_drop`` are container-runtime knobs with no
        meaning for a job that runs as the instance itself: there is no
        inner container whose capability set to adjust, and the
        instance's own capabilities are the profile's business. Ignored,
        as the proto permits for a backend that cannot apply them.
        """

    def start(self) -> None:
        """Nothing to start; the instance is already running."""

    def wrap(
        self,
        command: list[str],
        *,
        environment: dict[str, str] | None = None,
        user: str | None = None,
        cwd: str | None = None,
    ) -> tuple[list[str], dict[str, str] | None, int | None, str | None]:
        """Run ``command`` as given, directly in the instance.

        The instance exec already applies ``environment`` and ``cwd``, so
        they are handed back untouched for the caller to pass on.

        ``user`` is resolved here: LXD's instance exec takes a numeric UID
        only, so a name is looked up against the instance's own
        ``/etc/passwd`` via ``getent``. A numeric value is passed straight
        through -- LXD runs an unmapped UID even with no passwd entry, and
        an orphan numeric id is a legitimate request.
        """
        uid = self._resolve_uid(user)
        return command, environment, uid, cwd

    def _resolve_uid(self, user: str | None) -> int | None:
        """Translate an ``ExecRequest.user`` value to a numeric UID.

        ``None``/empty -> ``None`` (the daemon's default user). An
        all-digits value is already a UID. A name is resolved inside the
        instance; an unknown name is a bad request, raised as a
        non-precondition ``ExecutorError`` for the caller to map to
        ``INVALID_ARGUMENT``.
        """
        if not user:
            return None
        if user.isdigit():
            return int(user)
        rc, out, _ = self.client.exec_capture(
            self.instance, ["getent", "passwd", user], project=self.project
        )
        # getent exits 2 when the key is not found; any non-zero means we
        # have no UID to run as.
        if rc != 0 or ":" not in out:
            raise ExecutorError(f"unknown user {user!r} in instance")
        # passwd line: name:passwd:uid:gid:gecos:home:shell
        return int(out.split(":")[2])

    def cleanup(self) -> None:
        """Nothing to remove; the instance outlives this object."""

    def __str__(self) -> str:  # pragma: no cover — diagnostics only
        return f"instance {self.instance}"


@dataclass
class ContainerExecutor:
    """Runs a job's commands inside a container on the instance."""

    client: BackendClient
    instance: str
    image: str
    runtime: str
    container: str = ""
    #: The LXD project the instance lives in; every ``<runtime>`` call
    #: runs through an instance exec, so it must carry the same project
    #: scope the instance was created under.
    project: str | None = None
    #: Populated by :meth:`create`; see the note there on why it is not
    #: read lazily.

    def _run(self, *args: str) -> tuple[int, str, str]:
        return self.client.exec_capture(self.instance, [self.runtime, *args], project=self.project)

    @staticmethod
    def _last_line(out: str, err: str) -> str:
        detail = (err.strip() or out.strip()).splitlines()
        return detail[-1] if detail else "no output"

    def create(
        self,
        name: str,
        *,
        workdir: str,
        mounts: list[str],
        network: str = "",
        cap_add: list[str] | None = None,
        cap_drop: list[str] | None = None,
    ) -> None:
        """Pull the image and create an idle container named ``name``.

        Created but not started, mirroring the ``Create`` RPC this serves:
        ``Start`` is what makes the container runnable. The image is local
        once the pull returns, so the baked ``ENV`` is read here and cached
        — ``Start`` then reports it without a second round trip.

        ``mounts`` are bound at identical paths inside the container, so
        the layout promised in ``CreateResponse`` stays true on both
        sides. ``network``, when the job has services, joins the
        container to theirs so a step can reach them by name.

        ``cap_add`` / ``cap_drop`` are the workflow's capability requests
        for the job container, passed straight to ``--cap-add`` /
        ``--cap-drop``. This is where they belong: the runtime applies
        them to the inner container, the one the job's steps actually run
        in.
        """
        rc, out, err = self._run("pull", self.image)
        if rc != 0:
            raise ExecutorError(
                f"{self.runtime} pull {self.image!r} failed: {self._last_line(out, err)}"
            )

        args = ["create", "--name", name, "--workdir", workdir]
        if network:
            args += ["--network", network]
        for mount in mounts:
            args += ["--volume", mount]
        for cap in cap_add or []:
            args += ["--cap-add", cap]
        for cap in cap_drop or []:
            args += ["--cap-drop", cap]
        args += [self.image, *_ENTRY_POINT]

        rc, out, err = self._run(*args)
        if rc != 0:
            raise ExecutorError(
                f"{self.runtime} create {self.image!r} failed: {self._last_line(out, err)}"
            )
        self.container = name

    def start(self) -> None:
        """Start the container created by :meth:`create`."""
        rc, out, err = self._run("start", self.container)
        if rc != 0:
            raise ExecutorError(
                f"{self.runtime} start {self.container!r} failed: {self._last_line(out, err)}"
            )

    def wrap(
        self,
        command: list[str],
        *,
        environment: dict[str, str] | None = None,
        user: str | None = None,
        cwd: str | None = None,
    ) -> tuple[list[str], dict[str, str] | None, int | None, str | None]:
        """Return the argv that runs ``command`` inside the container.

        ``environment``, ``user`` and ``cwd`` become ``--env``,
        ``--user`` and ``--workdir`` flags and are returned as ``None``:
        applied to the outer instance exec they would configure the
        ``<runtime>`` process itself, leaving the job unaffected.

        ``user`` is passed to ``<runtime> exec --user`` verbatim -- a name
        or a numeric id. Unlike LXD's instance exec, the container runtime
        resolves a name against the *container's* ``/etc/passwd``, which is
        the right one for a job running inside it.

        Built rather than executed so the caller can stream it through
        ``exec_stream``: Exec is a streaming RPC and must not buffer a
        step's output.
        """
        args = [self.runtime, "exec"]
        for key, value in (environment or {}).items():
            args += ["--env", f"{key}={value}"]
        if user:
            args += ["--user", user]
        if cwd:
            args += ["--workdir", cwd]
        return [*args, self.container, *command], None, None, None

    def cleanup(self) -> None:
        """Force-remove the container; already gone counts as success."""
        if self.container:
            self._run("rm", "--force", self.container)

    def __str__(self) -> str:  # pragma: no cover — diagnostics only
        return f"{self.image} via {self.runtime} in {self.instance}"


@dataclass(frozen=True)
class Service:
    """One entry of a workflow's ``services:`` block."""

    name: str
    image: str
    env: dict[str, str]
    ports: list[str]

    @property
    def alias(self) -> str:
        """The hostname steps use to reach this service.

        The runner sets ``ManagesOwnNetworking``, so it attaches no
        aliases of its own and injects no hostnames into a step's
        environment: whatever name we give the service here *is* the
        address a workflow depends on. It must therefore be the
        service's own name, sanitised exactly as the runner's Docker
        backend would have.
        """
        return _ALIAS_UNSAFE.sub("_", self.name.lower())


@dataclass
class ServiceSet:
    """The service containers of one job.

    Services are containers whether or not the job itself is one. How a
    step reaches them depends on where the step runs, and that is what
    ``network`` selects:

    * A step **on the instance** reaches a service at ``localhost`` on
      the port it publishes -- the instance is the service's own docker
      host. ``network`` is empty: the services need no shared network of
      their own, only their published ports.
    * A step **in a container** reaches a service by name, which the
      default bridge will not resolve. ``network`` names a user-defined
      network the services are aliased on and the job container joins,
      so ``name`` resolves to the right service.

    Instance isolation is the LXD instance's concern, not docker's: a
    service network exists for name resolution, never to wall a job off
    from its neighbours.
    """

    client: BackendClient
    instance: str
    runtime: str
    network: str = ""
    #: The LXD project the instance lives in, passed through to every
    #: instance exec that drives the service containers.
    project: str | None = None
    containers: list[str] = field(default_factory=list)

    def _run(self, *args: str) -> tuple[int, str, str]:
        return self.client.exec_capture(self.instance, [self.runtime, *args], project=self.project)

    @staticmethod
    def _last_line(out: str, err: str) -> str:
        detail = (err.strip() or out.strip()).splitlines()
        return detail[-1] if detail else "no output"

    def create(self, services: list[Service]) -> None:
        """Bring every service up, each publishing its ports.

        Started here rather than in ``Start``: a service exists to be
        connected to, and the job's first step may do so immediately.
        Unlike the job container there is nothing to exec into later,
        so there is no reason to hold one created-but-stopped.

        With a ``network`` the services are aliased on it for a
        container job to reach by name; without one they rely on their
        published ports alone.
        """
        if self.network:
            rc, out, err = self._run("network", "create", self.network)
            if rc != 0:
                raise ExecutorError(
                    f"{self.runtime} network create {self.network!r} failed: "
                    f"{self._last_line(out, err)}"
                )

        for service in services:
            rc, out, err = self._run("pull", service.image)
            if rc != 0:
                raise ExecutorError(
                    f"{self.runtime} pull {service.image!r} for service "
                    f"{service.name!r} failed: {self._last_line(out, err)}"
                )

            name = f"{self.instance}-{service.alias}"
            args = ["run", "--detach", "--name", name]
            if self.network:
                args += ["--network", self.network, "--network-alias", service.alias]
            for key, value in service.env.items():
                args += ["--env", f"{key}={value}"]
            for port in service.ports:
                args += ["--publish", port]
            args.append(service.image)

            rc, out, err = self._run(*args)
            if rc != 0:
                raise ExecutorError(
                    f"{self.runtime} run {service.image!r} for service "
                    f"{service.name!r} failed: {self._last_line(out, err)}"
                )
            self.containers.append(name)

    def cleanup(self) -> None:
        """Force-remove every service, then the network if it had one.

        Best-effort and in that order: a network cannot go while a
        container is still attached to it.
        """
        for name in self.containers:
            self._run("rm", "--force", name)
        if self.network:
            self._run("network", "rm", self.network)


Executor = HostExecutor | ContainerExecutor


def wait_agent_ready(client: BackendClient, instance: str, *, project: str | None = None) -> None:
    """Block until ``instance``'s guest agent can serve exec.

    A container reports ready at once; a virtual-machine only once its
    guest OS has booted far enough to connect the agent. Instance state
    carries ``processes`` as ``-1`` until then, so poll it rather than
    letting the first exec race the agent and fail with a 404.
    """
    deadline = time.monotonic() + _AGENT_READY_TIMEOUT
    while True:
        state = client.get_instance_state(instance, project=project)
        if state.get("processes", -1) >= 0:
            return
        if time.monotonic() >= deadline:
            raise ExecutorError(
                f"instance {instance!r} did not connect its agent within "
                f"{_AGENT_READY_TIMEOUT:.0f}s",
                precondition=True,
            )
        time.sleep(_AGENT_READY_POLL)


_SYSTEM_READY_BUILTINS = {
    "builtin:cloud-init": ["cloud-init", "status", "--wait", "--long"],
    "builtin:cloud-init-strict": ["cloud-init", "status", "--wait", "--long"],
    "builtin:systemd": ["systemctl", "is-system-running", "--wait"],
    "builtin:systemd-strict": ["systemctl", "is-system-running", "--wait"],
}


def validate_system_ready(command: str) -> None:
    """Reject unknown reserved preset names before allocating an instance."""
    if command.startswith("builtin:") and command not in _SYSTEM_READY_BUILTINS:
        raise ExecutorError(f"unknown system-ready builtin {command!r}")


def wait_system_ready(
    client: BackendClient,
    instance: str,
    command: str,
    *,
    timeout: float = _SYSTEM_READY_TIMEOUT,
    project: str | None = None,
) -> None:
    """Block until a shell command or named readiness preset succeeds.

    The agent serves exec early in boot, but an image that provisions
    itself at first boot (cloud-init installing a container runtime, say)
    is not done then: a job that needs what the provisioning installs
    would race it and fail. ``command`` is the operator's definition of
    "this instance is ready to run jobs" -- e.g. ``cloud-init status
    --wait`` -- run through ``sh -c`` and polled until it succeeds.

    ``builtin:cloud-init`` accepts completion with exit 0 or 2, logging
    recoverable errors; ``builtin:cloud-init-strict`` requires exit 0.
    ``builtin:systemd`` accepts running or degraded,
    logging failed units; ``builtin:systemd-strict`` accepts only running.
    Raw shell commands still require exit 0. Unknown builtins are rejected.

    An empty ``command`` is "nothing to wait for" and returns at once, so
    images without a provisioning layer (the common minimal case) pay
    nothing. A command that never succeeds within ``timeout`` raises so
    the operator sees what it was waiting on rather than a later, vaguer
    failure.
    """
    validate_system_ready(command)
    if not command:
        return

    argv = _SYSTEM_READY_BUILTINS.get(command, ["sh", "-c", command])
    deadline = time.monotonic() + timeout
    while True:
        rc, out, err = client.exec_capture(instance, argv, project=project)
        ready = rc == 0
        warning = ""
        if command == "builtin:cloud-init":
            ready = rc in (0, 2)
            if rc == 2:
                warning = (
                    f"cloud-init completed with recoverable errors: {out.strip()} {err.strip()}"
                )
        elif command in ("builtin:systemd", "builtin:systemd-strict"):
            state = out.strip()
            ready = rc == 0 and state == "running"
            if command == "builtin:systemd" and rc == 1 and state == "degraded":
                ready = True
                _, failed, diagnostics = client.exec_capture(
                    instance, ["systemctl", "--failed", "--no-pager", "--plain"], project=project
                )
                warning = (
                    f"systemd boot completed in degraded state: "
                    f"{failed.strip()} {diagnostics.strip()}"
                )
        if ready:
            if warning:
                logging.getLogger(__name__).warning("instance %s: %s", instance, warning)
            return
        if time.monotonic() >= deadline:
            raise ExecutorError(
                f"system-ready command {command!r} did not succeed in instance "
                f"{instance!r} within {timeout:.0f}s (exit {rc}): "
                f"{err.strip() or out.strip() or 'no output'}",
                precondition=True,
            )
        time.sleep(_SYSTEM_READY_POLL)


def detect_runtime(client: BackendClient, instance: str, *, project: str | None = None) -> str:
    """Return the container runtime available inside ``instance``, or ``""``.

    Probe for each known runtime and wait for the one found to answer,
    which for docker means waiting on the daemon socket: the instance
    reports Running before systemd has finished starting the unit.

    Finding none is a fact about the instance, not a failure: a job
    that runs on the instance needs no runtime. Only a job that asks
    for one turns its absence into an error, so that the message can
    say what wanted it.
    """
    available = [
        runtime
        for runtime in _RUNTIMES
        if client.exec_capture(instance, ["sh", "-c", f"command -v {runtime}"], project=project)[0]
        == 0
    ]
    if not available:
        return ""

    runtime = available[0]
    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT
    while True:
        rc, _, err = client.exec_capture(instance, [runtime, "version"], project=project)
        if rc == 0:
            return runtime
        if time.monotonic() >= deadline:
            raise ExecutorError(
                f"{runtime} is installed in instance {instance!r} but did not "
                f"become ready within {_DAEMON_READY_TIMEOUT:.0f}s: "
                f"{err.strip() or 'no output'}",
                precondition=True,
            )
        time.sleep(_DAEMON_POLL_INTERVAL)


def resolve(
    client: BackendClient,
    instance: str,
    image: str,
    *,
    project: str | None = None,
    system_ready: str = "",
    system_ready_timeout: float = _SYSTEM_READY_TIMEOUT,
) -> Executor:
    """Pick the execution context for a job.

    Empty ``image`` -- the common case -- runs on the instance.
    Otherwise the job is containerised on the instance's runtime.

    The runtime is detected either way: what a job runs *in* does not
    change what the instance *has*, and an execution context that knows
    its instance's runtime can act on it whatever the job asked for.

    ``project`` is the LXD project the instance was created under; the
    executor carries it so its instance execs land in the right place.

    ``system_ready``, when set, is an operator-supplied command polled
    until it exits 0 before the runtime is probed: an image that installs
    its runtime at first boot is not ready the instant the agent answers,
    and detecting the runtime before provisioning finishes would wrongly
    report it absent.
    """
    wait_agent_ready(client, instance, project=project)
    wait_system_ready(client, instance, system_ready, timeout=system_ready_timeout, project=project)
    runtime = detect_runtime(client, instance, project=project)

    if not image:
        return HostExecutor(client=client, instance=instance, runtime=runtime, project=project)

    if not runtime:
        raise ExecutorError(
            f"no container runtime in instance {instance!r}: the job's "
            f"`container.image` needs {' or '.join(_RUNTIMES)} inside the "
            'instance. Apply a runtime profile (e.g. `profiles: "base,docker"`) '
            "or use an instance image that ships one",
            precondition=True,
        )

    return ContainerExecutor(
        client=client,
        instance=instance,
        image=image,
        runtime=runtime,
        project=project,
    )


def mount_specs(paths: list[str]) -> list[str]:
    """Build ``--volume`` specs mapping each path onto itself.

    Identical paths on both sides are the whole trick: Forgejo is told
    one filesystem layout in ``CreateResponse`` and it stays true inside
    the job container, so nothing else has to translate paths.

    Not shell-quoted — these go into argv, where a quoted path with a
    space in it would arrive with the quotes as part of the name.
    """
    return [f"{p}:{p}" for p in paths]
