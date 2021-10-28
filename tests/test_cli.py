from click.testing import CliRunner
from scripts.sync import cli


def test_sync():
    runner = CliRunner()
    result = runner.invoke(cli, ["--debug", "sync"], catch_exceptions=False)
    assert result.exit_code == 0
