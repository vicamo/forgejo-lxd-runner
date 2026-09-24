"""Nox sessions for forgejo-lxd-runner.

Run everything (lint + type + tests on the current interpreter):

    nox

Individual sessions:

    nox -s proto           # regenerate gRPC stubs from proto/
    nox -s lint            # ruff
    nox -s type            # mypy
    nox -s tests           # pytest on the current interpreter
    nox -s tests-3.12      # pytest on a specific interpreter
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

PROTO_SRC = Path("proto")
PROTO_OUT = Path("src/forgejo_lxd_runner/proto")


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
    session.install("grpcio-tools>=1.60")

    # Wipe previously generated packages, keep the hand-written __init__.py.
    for child in PROTO_OUT.iterdir() if PROTO_OUT.exists() else []:
        if child.is_dir():
            shutil.rmtree(child)

    PROTO_OUT.mkdir(parents=True, exist_ok=True)
    (PROTO_OUT / "__init__.py").touch()

    proto_files = sorted(str(p) for p in PROTO_SRC.rglob("*.proto"))
    if not proto_files:
        session.error(f"no .proto files under {PROTO_SRC}/")

    session.run(
        "python",
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_SRC}",
        f"--python_out={PROTO_OUT}",
        f"--pyi_out={PROTO_OUT}",
        f"--grpc_python_out={PROTO_OUT}",
        *proto_files,
    )

    # protoc doesn't create package __init__.py files for intermediate dirs.
    for pkg_dir in PROTO_OUT.rglob("*"):
        if pkg_dir.is_dir():
            (pkg_dir / "__init__.py").touch()

    # protoc emits absolute imports rooted at the proto package (e.g.
    # ``from plugin.v1alpha import plugin_pb2``). Rewrite them to be
    # rooted at our Python package so nothing depends on sys.path shape.
    package_prefix = ".".join(PROTO_OUT.relative_to("src").parts)
    for py in PROTO_OUT.rglob("*_pb2*.py"):
        text = py.read_text()
        # Only two shapes protoc uses at import time.
        text = text.replace("from plugin.v1alpha ", f"from {package_prefix}.plugin.v1alpha ")
        text = text.replace("import plugin.v1alpha.", f"import {package_prefix}.plugin.v1alpha.")
        py.write_text(text)


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
