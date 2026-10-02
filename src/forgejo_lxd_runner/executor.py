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

import time
from dataclasses import dataclass

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

#: Keeps the container alive without running anything, so steps can be
#: exec'd into it one at a time. Matches what the runner uses for its own
#: job containers.
_ENTRY_POINT = ("tail", "-f", "/dev/null")


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

    def create(self, name: str, *, workdir: str, mounts: list[str]) -> None:
        """Nothing to create — the instance is the execution context.

        ``mounts`` is irrelevant here: the paths the caller would bind
        are already the instance's own filesystem.
        """

    def start(self) -> None:
        """Nothing to start; the instance is already running."""

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
    #: Populated by :meth:`create`; see the note there on why it is not
    #: read lazily.

    def _run(self, *args: str) -> tuple[int, str, str]:
        return self.client.exec_capture(self.instance, [self.runtime, *args])

    @staticmethod
    def _last_line(out: str, err: str) -> str:
        detail = (err.strip() or out.strip()).splitlines()
        return detail[-1] if detail else "no output"

    def create(self, name: str, *, workdir: str, mounts: list[str]) -> None:
        """Pull the image and create an idle container named ``name``.

        Created but not started, mirroring the ``Create`` RPC this serves:
        ``Start`` is what makes the container runnable. The image is local
        once the pull returns, so the baked ``ENV`` is read here and cached
        — ``Start`` then reports it without a second round trip.

        ``mounts`` are bound at identical paths inside the container, so
        the layout promised in ``CreateResponse`` stays true on both
        sides.
        """
        rc, out, err = self._run("pull", self.image)
        if rc != 0:
            raise ExecutorError(
                f"{self.runtime} pull {self.image!r} failed: {self._last_line(out, err)}"
            )

        args = ["create", "--name", name, "--workdir", workdir]
        for mount in mounts:
            args += ["--volume", mount]
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

    def cleanup(self) -> None:
        """Force-remove the container; already gone counts as success."""
        if self.container:
            self._run("rm", "--force", self.container)

    def __str__(self) -> str:  # pragma: no cover — diagnostics only
        return f"{self.image} via {self.runtime} in {self.instance}"


Executor = HostExecutor | ContainerExecutor


def resolve(client: BackendClient, instance: str, image: str) -> Executor:
    """Pick the execution context for a job.

    Empty ``image`` — the common case — runs on the instance. Otherwise
    probe for a container runtime and wait for it to answer, which for
    docker means waiting on the daemon socket.
    """
    if not image:
        return HostExecutor(client=client, instance=instance)

    available = [
        runtime
        for runtime in _RUNTIMES
        if client.exec_capture(instance, ["sh", "-c", f"command -v {runtime}"])[0] == 0
    ]
    if not available:
        raise ExecutorError(
            f"no container runtime in instance {instance!r}: the job sets "
            f"`container.image`, which needs {' or '.join(_RUNTIMES)} inside the "
            'instance. Apply a runtime profile (e.g. `profiles: "base,docker"`) '
            "or use an instance image that ships one",
            precondition=True,
        )

    runtime = available[0]
    deadline = time.monotonic() + _DAEMON_READY_TIMEOUT
    while True:
        rc, _, err = client.exec_capture(instance, [runtime, "version"])
        if rc == 0:
            return ContainerExecutor(
                client=client,
                instance=instance,
                image=image,
                runtime=runtime,
            )
        if time.monotonic() >= deadline:
            raise ExecutorError(
                f"{runtime} is installed in instance {instance!r} but did not "
                f"become ready within {_DAEMON_READY_TIMEOUT:.0f}s: "
                f"{err.strip() or 'no output'}",
                precondition=True,
            )
        time.sleep(_DAEMON_POLL_INTERVAL)


def mount_specs(paths: list[str]) -> list[str]:
    """Build ``--volume`` specs mapping each path onto itself.

    Identical paths on both sides are the whole trick: Forgejo is told
    one filesystem layout in ``CreateResponse`` and it stays true inside
    the job container, so nothing else has to translate paths.

    Not shell-quoted — these go into argv, where a quoted path with a
    space in it would arrive with the quotes as part of the name.
    """
    return [f"{p}:{p}" for p in paths]
