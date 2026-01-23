from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_init_command_creates_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, ["init"])

    assert result.exit_code == 0
    config_file = tmp_path / "dbt-bouncer.yml"
    assert config_file.exists()
    assert "dbt_artifacts_dir: target" in config_file.read_text()


def test_init_command_fails_if_file_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.touch()

    runner = CliRunner()
    result = runner.invoke(cli, ["init"])

    assert result.exit_code == 1
