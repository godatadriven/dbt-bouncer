"""Tests for bundled configuration presets."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from dbt_bouncer import presets
from dbt_bouncer.cli.run.utils import _resolve_config_contents
from dbt_bouncer.enums import ConfigFileSource, PresetName
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.main import app
from dbt_bouncer.presets import load_preset_contents, read_preset_text

PRESET_NAMES = [p.value for p in PresetName]


@pytest.mark.parametrize("preset", PRESET_NAMES)
def test_preset_file_exists(preset):
    """Every preset name maps to a bundled YAML file."""
    assert (Path(presets.__file__).parent / f"{preset}.yml").exists()


@pytest.mark.parametrize("preset", PRESET_NAMES)
def test_load_preset_contents_has_checks(preset):
    """Each preset defines a non-empty list of manifest checks."""
    contents = load_preset_contents(preset)
    assert isinstance(contents["manifest_checks"], list)
    assert len(contents["manifest_checks"]) > 0


@pytest.mark.parametrize("preset", PRESET_NAMES)
def test_read_preset_text_preserves_comments(preset):
    """The raw preset text keeps the header comments for scaffolding."""
    text = read_preset_text(preset)
    assert text.startswith(f'# dbt-bouncer "{preset}" preset.')


@pytest.mark.parametrize("preset", PRESET_NAMES)
def test_preset_is_schema_valid(preset):
    """Each bundled preset passes `dbt-bouncer validate`.

    This guards against a mistyped check name or a missing required parameter.
    """
    preset_file = Path(presets.__file__).parent / f"{preset}.yml"
    result = CliRunner().invoke(app, ["validate", "--config-file", str(preset_file)])
    assert result.exit_code == 0, result.output


def test_unknown_preset_raises_clear_error():
    """An unknown preset name fails early with a clear error."""
    with pytest.raises(DbtBouncerConfigError, match="Unknown preset"):
        read_preset_text("not-a-preset")


def test_resolve_config_contents_uses_preset():
    """A preset is used when no explicit config file is provided."""
    contents, config_file_path = _resolve_config_contents(
        Path("dbt-bouncer.yml"), ConfigFileSource.DEFAULT, PresetName.STRICT
    )
    assert "manifest_checks" in contents
    assert config_file_path.name == "dbt-bouncer.yml"


def test_resolve_config_contents_preset_ignored_for_explicit_file(
    tmp_path, monkeypatch, caplog
):
    """An explicit --config-file wins over --preset, with a warning."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "my-config.yml"
    config_file.write_text("manifest_checks:\n  - name: check_model_has_unique_test\n")

    contents, _ = _resolve_config_contents(
        config_file, ConfigFileSource.COMMANDLINE, PresetName.STRICT
    )
    assert contents["manifest_checks"][0]["name"] == "check_model_has_unique_test"
    assert "Ignoring `--preset strict`" in caplog.text


@pytest.mark.parametrize("preset", PRESET_NAMES)
def test_init_preset_scaffolds_file(preset, tmp_path, monkeypatch):
    """`init --preset` writes the preset to dbt-bouncer.yml non-interactively."""
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["init", "--preset", preset])

    assert result.exit_code == 0, result.output
    config_file = tmp_path / "dbt-bouncer.yml"
    assert config_file.exists()
    assert config_file.read_text() == read_preset_text(preset)


def test_init_preset_declines_overwrite(tmp_path, monkeypatch):
    """`init --preset` aborts when the user declines to overwrite."""
    monkeypatch.chdir(tmp_path)
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text("old content")

    result = CliRunner().invoke(app, ["init", "--preset", "minimal"], input="n\n")

    assert result.exit_code == 1
    assert "old content" in config_file.read_text()
