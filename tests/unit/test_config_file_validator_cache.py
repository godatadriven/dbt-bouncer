"""Tests for the validated-conf disk cache in `dbt_bouncer.configuration_file.validator`.

Every failure mode here is designed to be silent: a bad cache must degrade to a
full rebuild, never to a wrong config or a crash. That makes these branches
invisible in normal use and worth pinning down explicitly.
"""

import logging
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest

from dbt_bouncer.configuration_file.validator import (
    _CONF_CACHE_FORMAT_VERSION,
    _base_field_names,
    _conf_cache_enabled,
    _load_cached_conf,
    _write_cached_conf,
)
from dbt_bouncer.exceptions import DbtBouncerConfigError


def _payload(checks=None, version=_CONF_CACHE_FORMAT_VERSION):
    return {
        "v": version,
        "base": {},
        "checks": checks if checks is not None else {},
    }


class TestLoadCachedConf:
    """Tests for `_load_cached_conf`, which must return None on anything suspect."""

    def test_missing_file_returns_none(self, tmp_path):
        """A cache file that was never written yields a rebuild."""
        assert _load_cached_conf(tmp_path / "absent.json", set(), None) is None

    def test_unreadable_file_returns_none(self, tmp_path):
        """A cache path that cannot be read yields a rebuild rather than propagating the OSError."""
        # A directory exists at the path, so `read_bytes` raises IsADirectoryError.
        cache_path = tmp_path / "conf.json"
        cache_path.mkdir()

        assert _load_cached_conf(cache_path, set(), None) is None

    def test_corrupt_json_returns_none(self, caplog, tmp_path):
        """A truncated or corrupt cache file yields a rebuild."""
        cache_path = tmp_path / "conf.json"
        cache_path.write_bytes(b'{"v": 1, "base": {}, "chec')

        with caplog.at_level(logging.DEBUG):
            assert _load_cached_conf(cache_path, set(), None) is None

        assert "Conf cache unreadable, rebuilding." in caplog.text

    @pytest.mark.parametrize(
        "version",
        [
            pytest.param(_CONF_CACHE_FORMAT_VERSION - 1, id="older_format"),
            pytest.param(_CONF_CACHE_FORMAT_VERSION + 1, id="newer_format"),
            pytest.param(None, id="version_absent"),
        ],
    )
    def test_format_version_mismatch_returns_none(self, tmp_path, version):
        """A payload written by a different cache format is ignored."""
        cache_path = tmp_path / "conf.json"
        cache_path.write_bytes(orjson.dumps(_payload(version=version)))

        assert _load_cached_conf(cache_path, set(), None) is None

    def test_unresolved_check_class_returns_none(self, caplog, tmp_path):
        """A cached check whose class is not in the loaded set yields a rebuild instead of a partial config."""
        cache_path = tmp_path / "conf.json"
        cache_path.write_bytes(
            orjson.dumps(
                _payload(
                    checks={
                        "manifest_checks": [
                            {
                                "_module": "some.uninstalled.plugin",
                                "_qualname": "CheckGoneAway",
                                "data": {},
                            }
                        ]
                    }
                )
            )
        )

        with caplog.at_level(logging.DEBUG):
            result = _load_cached_conf(
                cache_path, {"check_model_description_populated"}, None
            )

        assert result is None
        assert (
            "Conf cache references unresolved check class some.uninstalled.plugin.CheckGoneAway"
            in caplog.text
        )


class TestWriteCachedConf:
    """Tests for `_write_cached_conf`, which must never raise into the caller."""

    def test_unserialisable_field_is_skipped(self, caplog, tmp_path):
        """A base field orjson cannot encode skips the write rather than raising."""
        cache_path = tmp_path / "conf.json"
        config = SimpleNamespace(**{_base_field_names()[0]: object()})

        with caplog.at_level(logging.DEBUG):
            _write_cached_conf(cache_path, config)

        assert not cache_path.exists()
        assert "Conf cache write failed during serialisation." in caplog.text

    def test_unwritable_path_is_skipped(self, caplog, tmp_path):
        """An unwritable cache location skips the write rather than raising."""
        # A regular file where the parent directory should be, so `mkdir` raises.
        blocked = tmp_path / "blocked"
        blocked.write_text("not a directory")
        cache_path = blocked / "conf.json"

        with caplog.at_level(logging.DEBUG):
            _write_cached_conf(cache_path, SimpleNamespace())

        assert not cache_path.exists()
        assert "Conf cache write failed." in caplog.text

    def test_round_trips_an_empty_config(self, tmp_path):
        """A config with no checks writes a payload that loads back."""
        cache_path = tmp_path / "conf.json"

        _write_cached_conf(cache_path, SimpleNamespace())

        assert cache_path.exists()
        written = orjson.loads(cache_path.read_bytes())
        assert written["v"] == _CONF_CACHE_FORMAT_VERSION
        assert _load_cached_conf(cache_path, set(), None) is not None

    def test_temporary_file_is_not_left_behind(self, tmp_path):
        """The write goes via a temp file that is renamed, leaving no `.tmp` residue."""
        cache_path = tmp_path / "conf.json"

        _write_cached_conf(cache_path, SimpleNamespace())

        assert list(tmp_path.glob("*.tmp")) == []


class TestConfCacheEnabled:
    """Tests for the `DBT_BOUNCER_DISABLE_CONF_CACHE` escape hatch."""

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("1", id="one"),
            pytest.param("true", id="true"),
            pytest.param("TRUE", id="uppercase_true"),
            pytest.param("yes", id="yes"),
        ],
    )
    def test_disabled_by_truthy_values(self, monkeypatch, value):
        """Any documented truthy value disables the cache, case-insensitively."""
        monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", value)

        assert _conf_cache_enabled() is False

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("", id="empty"),
            pytest.param("0", id="zero"),
            pytest.param("false", id="false"),
            # Only the three documented values disable it; anything else is a no-op.
            pytest.param("on", id="undocumented_value"),
        ],
    )
    def test_enabled_otherwise(self, monkeypatch, value):
        """Values outside the documented set leave the cache enabled."""
        monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", value)

        assert _conf_cache_enabled() is True

    def test_enabled_when_unset(self, monkeypatch):
        """The cache is on by default."""
        monkeypatch.delenv("DBT_BOUNCER_DISABLE_CONF_CACHE", raising=False)

        assert _conf_cache_enabled() is True


class TestValidateConfFullScanFallback:
    """Tests for the full-scan fallback in `validate_conf`."""

    @staticmethod
    def _validate(monkeypatch, config_file_contents):
        # The conf cache is keyed on the config contents, not on the registry
        # state these tests reach into, so bypass it.
        monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", "1")
        from dbt_bouncer.configuration_file import validator

        return validator.validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config_file_contents),
        )

    @pytest.mark.parametrize(
        "config_file_contents",
        [
            pytest.param({}, id="no_categories"),
            pytest.param({"manifest_checks": []}, id="empty_category"),
        ],
    )
    def test_falls_back_to_full_scan(self, monkeypatch, config_file_contents):
        """With no check names to extract, validation uses the full registry scan and still succeeds."""
        assert self._validate(monkeypatch, config_file_contents) is not None

    @pytest.mark.parametrize(
        ("config_file_contents", "match"),
        [
            # Neither entry yields a check name, so both reach the full-scan
            # fallback before failing validation on the entry itself.
            pytest.param(
                {"manifest_checks": [{}]},
                "Unable to extract tag using discriminator 'name'",
                id="entry_without_name_or_code",
            ),
            pytest.param(
                {"manifest_checks": ["not_a_dict"]},
                "Input should be a valid dictionary",
                id="entry_is_not_a_dict",
            ),
        ],
    )
    def test_malformed_entry_reports_a_useful_error(
        self, monkeypatch, config_file_contents, match
    ):
        """A check entry with no usable name is skipped during name extraction and rejected by validation."""
        with pytest.raises(DbtBouncerConfigError, match=match):
            self._validate(monkeypatch, config_file_contents)


def test_cache_dir_is_not_created_as_a_side_effect_of_loading(tmp_path):
    """Loading from a cache path in a directory that does not exist does not create it."""
    cache_path = tmp_path / "nested" / "conf.json"

    assert _load_cached_conf(cache_path, set(), None) is None
    assert not Path(tmp_path / "nested").exists()
