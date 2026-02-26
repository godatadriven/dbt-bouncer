from typer.testing import CliRunner

from dbt_bouncer.main import app


def test_init_command_creates_file(tmp_path, monkeypatch):
    """Test init with all default values (pressing Enter for all prompts)."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    # 3 prompts (artifacts dir, check descriptions, check unique tests, check naming)
    result = runner.invoke(app, ["init"], input="\n\n\n\n")

    assert result.exit_code == 0
    config_file = tmp_path / "dbt-bouncer.yml"
    assert config_file.exists()
    assert "dbt_artifacts_dir: target" in config_file.read_text()


def test_init_command_fails_if_file_exists(tmp_path, monkeypatch):
    """Test that if the file exists and user declines overwrite, it aborts."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.touch()

    runner = CliRunner()
    # 3 prompts + 'n' to decline overwrite
    result = runner.invoke(app, ["init"], input="\n\n\n\nn\n")

    assert result.exit_code == 1  # Aborted
    assert "Aborted" in result.stdout


def test_init_command_overwrites_if_confirmed(tmp_path, monkeypatch):
    """Test that if file exists and user confirms overwrite, it succeeds."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text("old content")

    runner = CliRunner()
    # 3 prompts + 'y' to confirm overwrite
    result = runner.invoke(app, ["init"], input="\n\n\n\ny\n")

    assert result.exit_code == 0
    assert "Created" in result.stdout
    # Verify it was actually overwritten
    assert "old content" not in config_file.read_text()


def test_init_command_custom_responses(tmp_path, monkeypatch):
    """Test providing specific non-default answers."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    # custom_target, no descriptions, yes unique tests, no naming
    result = runner.invoke(app, ["init"], input="custom_target\nn\ny\nn\n")

    assert result.exit_code == 0
    config_file = tmp_path / "dbt-bouncer.yml"
    content = config_file.read_text()
    assert "custom_target" in content
    assert "check_model_description_populated" not in content
    assert "check_model_has_unique_test" in content
    assert "check_model_names" not in content
