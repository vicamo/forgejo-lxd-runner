"""Unit tests for the LXD → GHA architecture and OS mappings."""

from __future__ import annotations

import pytest

from forgejo_lxd_runner.server import _lxd_arch_to_gha, _lxd_os_to_gha


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


@pytest.mark.parametrize(
    ("image_os", "gha_os"),
    [
        # The two non-Linux OSes LXD actually ships.
        ("freebsd", "FreeBSD"),
        ("FreeBSD", "FreeBSD"),  # case-insensitive
        ("windows", "Windows"),
        ("Windows", "Windows"),
        # Every distro LXD's ``images:`` remote carries — all Linux.
        ("ubuntu", "Linux"),
        ("debian", "Linux"),
        ("alpine", "Linux"),
        ("archlinux", "Linux"),
        ("fedora", "Linux"),
        ("centos", "Linux"),
        ("rocky", "Linux"),
        ("almalinux", "Linux"),
        ("opensuse", "Linux"),
        ("voidlinux", "Linux"),
        ("nixos", "Linux"),
        ("gentoo", "Linux"),
        ("oracle", "Linux"),
        ("openwrt", "Linux"),
        ("plamo", "Linux"),
        ("slackware", "Linux"),
        # Missing metadata: default to Linux.
        ("", "Linux"),
        # Anything unknown falls through to Linux — safer than empty.
        ("haiku", "Linux"),
    ],
)
def test_os_mapping(image_os: str, gha_os: str) -> None:
    assert _lxd_os_to_gha(image_os) == gha_os
