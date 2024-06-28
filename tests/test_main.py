from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_cli_happy_path(caplog):
    """
    Test the happy path, just need to ensure that the CLI starts up and calculates the input parameters correctly.
    """

    runner = CliRunner()
    runner.invoke(
        cli,
        [
            "--dbt-project-dir",
            "dbt_project",
        ],
    )
    assert "Running dbt_bouncer (0.0.0)..." in caplog.text


def test_cli_dbt_dir_doesnt_exist():

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dbt-project-dir",
            "non-existent-directory",
        ],
    )
    assert type(result.exception) in [SystemExit]


def test_cli_manifest_doesnt_exist(tmp_path):

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--dbt-project-dir",
            tmp_path,
        ],
    )
    assert type(result.exception) in [RuntimeError]
    assert (
        result.exception.args[0]
        == f"No manifest.json found at {tmp_path / 'target/manifest.json'}."
    )
