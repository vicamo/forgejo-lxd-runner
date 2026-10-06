"""The usage doc (``docs/usage.md``) must not drift from the code.

Two independent checks keep ``docs/usage.md`` honest:

* **CLI help** -- the fenced block under the ``cli-help`` marker is the
  verbatim output of ``build_parser().format_help()`` rendered at a pinned
  width. Changing a flag or its help text without regenerating the doc
  fails here.
* **Backend options** -- the table under the ``backend-options`` marker must
  list exactly the keys ``server.py`` reads from ``request.backend_options``.
  The key set is recovered by AST-scanning for ``…backend_options.get("…")``
  literals, so adding or removing a backend option without touching the doc
  fails here. (Descriptions are hand-written; this check guards the key set,
  not the prose.)

Regenerate the doc after an intentional change with
``python -m tests.unit.test_usage_docs`` -- running this module as a
script rewrites both generated blocks in place.
"""

from __future__ import annotations

import ast
import os
import re
import unittest.mock
from pathlib import Path

from forgejo_lxd_runner import __main__

REPO_ROOT = Path(__file__).resolve().parents[2]
DOC_PATH = REPO_ROOT / "docs" / "usage.md"
SERVER_PATH = REPO_ROOT / "src" / "forgejo_lxd_runner" / "server.py"

# argparse wraps help text to the terminal width; pin it so the golden
# output is reproducible regardless of where the test runs.
_HELP_COLUMNS = "80"


def _generated_block(doc: str, marker: str) -> str:
    """Return the text between ``BEGIN GENERATED: <marker>`` and its ``END``."""
    pattern = (
        rf"<!-- BEGIN GENERATED: {re.escape(marker)} -->\n"
        rf"(.*?)\n"
        rf"<!-- END GENERATED: {re.escape(marker)} -->"
    )
    match = re.search(pattern, doc, re.DOTALL)
    assert match is not None, f"marker {marker!r} not found in {DOC_PATH}"
    return match.group(1)


def _render_cli_help() -> str:
    """The ``cli-help`` block as it should appear, from the live parser."""
    with unittest.mock.patch.dict(os.environ, {"COLUMNS": _HELP_COLUMNS}):
        help_text = __main__.build_parser().format_help()
    return f"```console\n$ forgejo-lxd-runner --help\n{help_text}```"


def _documented_backend_option_keys(doc: str) -> set[str]:
    """Keys from the first column of the backend-options table."""
    block = _generated_block(doc, "backend-options")
    keys: set[str] = set()
    for line in block.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4:
            continue
        name = cells[0]
        # Skip the header row and the separator row.
        if not (name.startswith("`") and name.endswith("`")):
            continue
        keys.add(name.strip("`"))
    return keys


def _code_backend_option_keys() -> set[str]:
    """Keys ``server.py`` reads via ``…backend_options.get("<key>")``.

    AST-scans for ``.get("literal")`` calls whose receiver is an attribute
    access ending in ``backend_options`` (``request.backend_options`` today,
    but robust to the receiver being renamed), so the set reflects what the
    code actually consults, not a hand-maintained list.
    """
    tree = ast.parse(SERVER_PATH.read_text())
    keys: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "get"):
            continue
        receiver = func.value
        if not (isinstance(receiver, ast.Attribute) and receiver.attr == "backend_options"):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            keys.add(first.value)
    return keys


def test_cli_help_block_matches_parser() -> None:
    """The documented ``--help`` output is the parser's live rendering."""
    doc = DOC_PATH.read_text()
    documented = _generated_block(doc, "cli-help")
    assert documented == _render_cli_help(), (
        "docs/usage.md CLI help is stale; regenerate with `python -m tests.unit.test_usage_docs`"
    )


def test_backend_option_keys_match_code() -> None:
    """The documented option keys are exactly those ``server.py`` reads."""
    documented = _documented_backend_option_keys(DOC_PATH.read_text())
    in_code = _code_backend_option_keys()
    assert documented == in_code, (
        f"backend-options table is out of sync with server.py: "
        f"documented-only={documented - in_code}, code-only={in_code - documented}"
    )


def test_ast_scan_finds_the_known_keys() -> None:
    """Guard the scanner itself: it must recover the keys we know exist.

    Without this, a scanner bug that returned an empty set would make
    ``test_backend_option_keys_match_code`` pass only if the doc were also
    emptied -- a silent mutual failure. Pin a known key so the extractor
    can't regress to finding nothing.
    """
    keys = _code_backend_option_keys()
    assert {"project", "type", "ephemeral"} <= keys


def _regenerate() -> None:
    """Rewrite both generated blocks in ``docs/usage.md`` in place."""
    doc = DOC_PATH.read_text()
    doc = re.sub(
        r"(<!-- BEGIN GENERATED: cli-help -->\n).*?(\n<!-- END GENERATED: cli-help -->)",
        lambda m: m.group(1) + _render_cli_help() + m.group(2),
        doc,
        flags=re.DOTALL,
    )
    DOC_PATH.write_text(doc)


if __name__ == "__main__":
    _regenerate()
    print(f"regenerated {DOC_PATH}")
