"""Unit tests for the LXD → GHA architecture mapping."""

from __future__ import annotations

import pytest

from forgejo_lxd_runner.server import _lxd_arch_to_gha


@pytest.mark.parametrize(
    ("lxd_arch", "gha_arch"),
    [
        ("i686", "X86"),
        ("i386", "X86"),
        ("x86_64", "X64"),
        ("armv7l", "ARM"),
        ("armv6l", "ARM"),
        ("aarch64", "ARM64"),
        ("s390x", "S390x"),
        ("ppc64le", "Ppc64le"),
        ("loongarch64", "LoongArch64"),
        ("riscv64", "RiscV64"),
        ("wasm32", "Wasm"),
        ("wasm64", "Wasm"),
    ],
)
def test_known_archs_are_mapped(lxd_arch: str, gha_arch: str) -> None:
    assert _lxd_arch_to_gha(lxd_arch) == gha_arch


def test_unknown_arch_passes_through() -> None:
    assert _lxd_arch_to_gha("sparc64") == "sparc64"
    assert _lxd_arch_to_gha("") == ""
