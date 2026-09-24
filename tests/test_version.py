"""Sanity checks that live outside the unit/grpc/e2e tiers."""

from forgejo_lxd_runner import __version__


def test_version() -> None:
    assert __version__
