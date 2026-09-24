"""Unit tests for the --log-level CLI flag."""

from __future__ import annotations

import pytest

from forgejo_lxd_runner.__main__ import build_parser


def test_log_level_defaults_to_info() -> None:
    args = build_parser().parse_args([])
    assert args.log_level == "INFO"


@pytest.mark.parametrize(
    "given,expected",
    [
        ("debug", "DEBUG"),
        ("Info", "INFO"),
        ("WARNING", "WARNING"),
        ("error", "ERROR"),
        ("critical", "CRITICAL"),
    ],
)
def test_log_level_is_case_insensitive(given: str, expected: str) -> None:
    args = build_parser().parse_args(["--log-level", given])
    assert args.log_level == expected


def test_log_level_rejects_unknown_value(capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--log-level", "TRACE"])
    err = capsys.readouterr().err
    assert "invalid choice" in err
