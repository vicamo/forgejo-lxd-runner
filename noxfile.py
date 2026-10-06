"""Nox sessions for forgejo-lxd-runner.

Run everything (lint + type + tests on the current interpreter):

    nox

Individual sessions:

    nox -s proto           # regenerate gRPC stubs from proto/
    nox -s lint            # ruff
    nox -s type            # mypy
    nox -s tests           # pytest on the current interpreter
    nox -s tests-3.12      # pytest on a specific interpreter
    nox -s e2e             # pytest against a live LXD or Incus daemon
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover - only reached on 3.10
    import tomli as tomllib  # type: ignore[import-not-found, no-redef]

import nox

nox.options.sessions = ["lint", "type", "tests"]
nox.options.reuse_existing_virtualenvs = True


def _supported_pythons() -> list[str]:
    """CPython X.Y series to test against.

    The lower bound is taken from ``project.requires-python`` in
    ``pyproject.toml`` so that the noxfile and the packaging metadata cannot
    drift. The upper bound is the interpreter running nox itself: newer
    series get picked up automatically as soon as a matching ``pythonX.Y``
    lands on ``PATH``. Series that aren't installed locally are skipped so
    ``nox -s tests`` doesn't error out on a missing interpreter.
    """

    pyproject = tomllib.loads(Path("pyproject.toml").read_text())
    spec = pyproject["project"]["requires-python"]
    match = re.search(r">=?\s*(\d+)\.(\d+)", spec)
    if not match:
        raise RuntimeError(f"cannot parse a lower bound out of requires-python={spec!r}")
    lo_major, lo_minor = int(match.group(1)), int(match.group(2))
    hi_major, hi_minor = sys.version_info[:2]
    if hi_major != lo_major:
        raise RuntimeError(f"unexpected major version jump {lo_major} -> {hi_major}")
    candidates = [f"{lo_major}.{y}" for y in range(lo_minor, hi_minor + 1)]
    return [v for v in candidates if shutil.which(f"python{v}")]


PYTHON_VERSIONS = _supported_pythons()


@nox.session
def proto(session: nox.Session) -> None:
    """Regenerate gRPC Python stubs from proto/*.proto."""
    session.install("grpcio-tools>=1.51.1")
    session.run("python", "tools/generate_proto.py")


@nox.session
def lint(session: nox.Session) -> None:
    session.install("ruff>=0.5")
    session.run("ruff", "check", ".")
    session.run("ruff", "format", "--check", ".")


@nox.session
def fmt(session: nox.Session) -> None:
    """Apply ruff formatting and auto-fixable lint rules in place."""
    session.install("ruff>=0.5")
    session.run("ruff", "check", "--fix", ".")
    session.run("ruff", "format", ".")


@nox.session
def type(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run("mypy", "src")


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    session.install("-e", ".[dev]")
    session.run("pytest", "tests/unit", "tests/grpc", "tests/test_version.py", *session.posargs)


@nox.session
def e2e(session: nox.Session) -> None:
    """Run e2e tests against a live LXD or Incus daemon on this host.

    The socket path is intentionally NOT passed in — the client's
    autodetect logic is part of what we're exercising.
    """
    session.install("-e", ".[dev]")
    session.run("pytest", "tests/e2e", *session.posargs)
