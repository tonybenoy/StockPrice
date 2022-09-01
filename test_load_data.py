from typer.testing import CliRunner

from load_data import app

runner = CliRunner()


def test_app_invalid_option():
    result = runner.invoke(app, ["--invalid"])
    assert "--help" in result.stdout
    assert result.exit_code == 2


def test_app_missing_params():
    result = runner.invoke(
        app,
        ["--symbol", "AAPL", "--start-date", "2020-01-01", "--last-date", "2020-01-01"],
    )
    assert "Missing option '--currency'" in result.stdout
    assert result.exit_code == 2


def test_app_invalid_params():
    result = runner.invoke(
        app,
        [
            "--symbol",
            "AAPL",
            "--start-date",
            "2020-01-01",
            "--last-date",
            "41",
            "--currency",
            "USD",
        ],
    )
    assert (
        "41 is not a valid date,Could not match input '41' to any of the following"
        in result.stdout
    )
    assert result.exit_code == 1
