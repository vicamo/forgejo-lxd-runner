"""Unit tests for the job execution context (host vs nested container)."""

from __future__ import annotations

import itertools
from typing import Any
from unittest.mock import MagicMock

import pytest

from forgejo_lxd_runner import executor as executor_module
from forgejo_lxd_runner.executor import (
    ContainerExecutor,
    ExecutorError,
    HostExecutor,
    mount_specs,
    resolve,
)

INSTANCE = "forgejo-test"


def make_client(responses: list[tuple[int, str, str]] | None = None) -> MagicMock:
    """A client whose exec_capture replays ``responses`` in order.

    Once the script runs out the last response repeats, so a test about
    a runtime that never recovers does not accidentally see it recover.
    An empty script means "everything succeeds".
    """
    client = MagicMock()
    client.calls = []

    queue = list(responses or [])

    def exec_capture(name: str, command: list[str], **kwargs: Any) -> tuple[int, str, str]:
        client.calls.append(command)
        if not queue:
            return (0, "", "")
        return queue.pop(0) if len(queue) > 1 else queue[0]

    client.exec_capture.side_effect = exec_capture
    return client


# ---------------------------------------------------------------------------
# resolve


def test_resolve_without_image_runs_on_the_host() -> None:
    """No `container:` block is the common case, not an error."""
    client = make_client()

    executor = resolve(client, INSTANCE, "")

    assert isinstance(executor, HostExecutor)
    # The runtime is still detected: what the job runs in does not
    # change what the instance has.
    assert executor.runtime == "docker"


def test_resolve_records_a_missing_runtime_for_a_host_job() -> None:
    """An instance with no runtime is a fact to carry, not a failure."""
    client = make_client([(1, "", ""), (1, "", "")])

    executor = resolve(client, INSTANCE, "")

    assert isinstance(executor, HostExecutor)
    assert executor.runtime == ""


@pytest.mark.parametrize("runtime", ["docker", "podman"])
def test_resolve_detects_either_runtime(runtime: str) -> None:
    """Both runtimes drive the same verbs, so either one suffices."""
    found = 0 if runtime == "docker" else 1
    responses = [(0, "", "") if i == found else (1, "", "not found") for i in range(2)]
    responses.append((0, "version ok", ""))
    client = make_client(responses)

    executor = resolve(client, INSTANCE, "node:20")

    assert isinstance(executor, ContainerExecutor)
    assert executor.runtime == runtime
    assert executor.image == "node:20"


def test_resolve_prefers_docker_when_both_present() -> None:
    """Ambiguity resolves to the one with a daemon already running."""
    client = make_client([(0, "", ""), (0, "", ""), (0, "", "")])

    executor = resolve(client, INSTANCE, "node:20")

    assert isinstance(executor, ContainerExecutor)
    assert executor.runtime == "docker"


def test_resolve_without_any_runtime_is_a_precondition_failure() -> None:
    """Operator misconfiguration, fixed in Forgejo config, not the workflow."""
    client = make_client([(1, "", ""), (1, "", "")])

    with pytest.raises(ExecutorError) as excinfo:
        resolve(client, INSTANCE, "node:20")

    assert excinfo.value.precondition is True
    # The message has to tell the operator what to actually do.
    assert "profiles" in str(excinfo.value)


def test_resolve_waits_for_a_daemon_that_is_not_ready_yet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Docker's socket lags the instance being Running; retry, don't fail."""
    monkeypatch.setattr(executor_module.time, "sleep", lambda _: None)
    client = make_client(
        [
            (0, "", ""),  # command -v docker
            (1, "", ""),  # command -v podman
            (1, "", "Cannot connect to the Docker daemon"),
            (1, "", "Cannot connect to the Docker daemon"),
            (0, "Server: ...", ""),
        ]
    )

    executor = resolve(client, INSTANCE, "node:20")

    assert isinstance(executor, ContainerExecutor)


def test_resolve_gives_up_on_a_daemon_that_never_answers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A runtime that never becomes ready must not hang a job forever."""
    monkeypatch.setattr(executor_module.time, "sleep", lambda _: None)
    clock = itertools.count(0.0, 1000.0)
    monkeypatch.setattr(executor_module.time, "monotonic", lambda: next(clock))
    client = make_client([(0, "", ""), (1, "", ""), (1, "", "daemon down")])

    with pytest.raises(ExecutorError) as excinfo:
        resolve(client, INSTANCE, "node:20")

    assert excinfo.value.precondition is True
    assert "daemon down" in str(excinfo.value)


# ---------------------------------------------------------------------------
# HostExecutor — the null object


def test_host_executor_is_inert() -> None:
    """Every method is a no-op; the instance is already the context."""
    client = make_client()
    executor = HostExecutor(client=client, instance=INSTANCE)

    executor.create("ignored", workdir="/work", mounts=[])
    executor.start()
    executor.cleanup()
    assert executor.wrap(["echo", "hi"]) == (["echo", "hi"], None, None, None)
    # Critically: it never touches the instance.
    assert client.calls == []


# ---------------------------------------------------------------------------
# ContainerExecutor


def test_create_pulls_then_creates_an_idle_container() -> None:
    """Idle, because steps arrive one at a time through Exec."""
    client = make_client([(0, "", ""), (0, "", ""), (0, "[]", "")])
    executor = ContainerExecutor(
        client=client,
        instance=INSTANCE,
        image="node:20",
        runtime="docker",
    )

    executor.create("job-abc", workdir="/work", mounts=["/work:/work", "/tmp:/tmp"])

    pull, create = client.calls[:2]
    assert pull == ["docker", "pull", "node:20"]
    assert create[:5] == ["docker", "create", "--name", "job-abc", "--workdir"]
    # The entrypoint keeps the container alive between steps.
    assert create[-3:] == ["tail", "-f", "/dev/null"]
    assert "--volume" in create and "/work:/work" in create
    assert executor.container == "job-abc"
    # Created, not started: that is Start's job.
    assert not any(call[:2] == ["docker", "start"] for call in client.calls)


def test_start_starts_the_created_container() -> None:
    client = make_client()
    executor = ContainerExecutor(
        client=client, instance=INSTANCE, image="node:20", runtime="docker", container="job-abc"
    )

    executor.start()

    assert client.calls == [["docker", "start", "job-abc"]]


def test_start_reports_a_container_that_will_not_start() -> None:
    client = make_client([(1, "", "Error response from daemon: no such container")])
    executor = ContainerExecutor(
        client=client, instance=INSTANCE, image="node:20", runtime="docker", container="job-abc"
    )

    with pytest.raises(ExecutorError, match="no such container"):
        executor.start()


def test_create_reports_a_failed_pull_as_a_job_error() -> None:
    """A bad image reference is workflow input, not infrastructure."""
    client = make_client([(1, "", "Error response from daemon: manifest unknown")])
    executor = ContainerExecutor(
        client=client, instance=INSTANCE, image="nope:404", runtime="docker"
    )

    with pytest.raises(ExecutorError) as excinfo:
        executor.create("job-abc", workdir="/work", mounts=[])

    assert excinfo.value.precondition is False
    assert "manifest unknown" in str(excinfo.value)


def test_create_reports_a_failed_create() -> None:
    client = make_client([(0, "", ""), (1, "", "conflict: name already in use")])
    executor = ContainerExecutor(
        client=client, instance=INSTANCE, image="node:20", runtime="docker"
    )

    with pytest.raises(ExecutorError, match="already in use"):
        executor.create("job-abc", workdir="/work", mounts=[])


def test_cleanup_force_removes_the_container() -> None:
    client = make_client()
    executor = ContainerExecutor(
        client=client,
        instance=INSTANCE,
        image="node:20",
        runtime="docker",
        container="job-abc",
    )

    executor.cleanup()

    assert client.calls == [["docker", "rm", "--force", "job-abc"]]


def test_cleanup_without_a_started_container_does_nothing() -> None:
    """create() may have failed before the container existed."""
    client = make_client()
    executor = ContainerExecutor(
        client=client, instance=INSTANCE, image="node:20", runtime="docker"
    )

    executor.cleanup()

    assert client.calls == []


# ---------------------------------------------------------------------------
# mount_specs


def test_mount_specs_maps_paths_onto_themselves() -> None:
    """Identical paths are what keep CopyIn/CopyOut container-unaware."""
    assert mount_specs(["/work", "/opt/hostedtoolcache"]) == [
        "/work:/work",
        "/opt/hostedtoolcache:/opt/hostedtoolcache",
    ]


def test_mount_specs_does_not_shell_quote() -> None:
    """These are argv entries; quoting would become part of the path."""
    assert mount_specs(["/a b"]) == ["/a b:/a b"]


def test_wrap_execs_into_the_container() -> None:
    client = make_client()
    executor = ContainerExecutor(
        client=client,
        instance=INSTANCE,
        image="node:20",
        runtime="podman",
        container="job-abc",
    )

    assert executor.wrap(["sh", "-c", "echo hi"]) == (
        ["podman", "exec", "job-abc", "sh", "-c", "echo hi"],
        None,
        None,
        None,
    )


def test_wrap_moves_env_user_and_cwd_onto_the_container() -> None:
    client = make_client()
    executor = ContainerExecutor(
        client=client,
        instance=INSTANCE,
        image="node:20",
        runtime="docker",
        container="job-abc",
    )

    argv, environment, user, cwd = executor.wrap(
        ["env"],
        environment={"FOO": "bar"},
        user=1000,
        cwd="/work",
    )

    assert argv == [
        "docker",
        "exec",
        "--env",
        "FOO=bar",
        "--user",
        "1000",
        "--workdir",
        "/work",
        "job-abc",
        "env",
    ]
    # Handed to the container, so nothing is left for the outer exec:
    # applied there they would configure the docker CLI, not the job.
    assert (environment, user, cwd) == (None, None, None)


def test_wrap_passes_host_env_user_and_cwd_straight_through() -> None:
    executor = HostExecutor(client=make_client(), instance=INSTANCE)

    assert executor.wrap(["env"], environment={"FOO": "bar"}, user=1000, cwd="/work") == (
        ["env"],
        {"FOO": "bar"},
        1000,
        "/work",
    )


def test_wrap_omits_flags_it_was_not_given() -> None:
    executor = ContainerExecutor(
        client=make_client(),
        instance=INSTANCE,
        image="node:20",
        runtime="docker",
        container="job-abc",
    )

    # A zero UID is a real user (root) and must not be dropped as falsy.
    argv, _, _, _ = executor.wrap(["id"], user=0)

    assert argv == ["docker", "exec", "--user", "0", "job-abc", "id"]
