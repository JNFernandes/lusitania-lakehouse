"""The command line: each command is recognised with the right defaults."""
import pytest

from lusitania.cli import build_parser


def test_gold_defaults_to_local_build():
    args = build_parser().parse_args(["gold"])
    assert (args.target, args.command) == ("local", "build")


def test_gold_on_databricks():
    args = build_parser().parse_args(["gold", "--target", "databricks", "--command", "test"])
    assert (args.target, args.command) == ("databricks", "test")


def test_unknown_target_is_rejected():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["run", "--target", "fabric"])


@pytest.mark.parametrize("command", ["run", "bronze", "silver", "inspect"])
def test_every_command_exists(command):
    assert callable(build_parser().parse_args([command]).func)
