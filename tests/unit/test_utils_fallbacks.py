"""Tests for the degradation paths in `dbt_bouncer.utils`.

These branches recover from a corrupt cache, an unwritable cache directory or a
check module that will not import. They fail quietly and slowly rather than
loudly, so a regression here would not surface in ordinary use.
"""

import importlib
import logging
import sys
from types import ModuleType

import pytest

from dbt_bouncer import utils

FAKE_MAP = {
    "check_fake": {
        "module": "dbt_bouncer.checks.manifest.models.naming",
        "category": "manifest_checks",
    }
}


@pytest.fixture
def cache_dir(monkeypatch, tmp_path):
    """Point the on-disk check-registry cache at a temporary directory.

    Returns:
        Path: The directory the cache now writes to.

    """
    path = tmp_path / "cache"
    monkeypatch.setattr(utils, "get_cache_dir", lambda: path)
    return path


class TestCheckModuleMapCache:
    """Tests for the disk cache behind `_get_check_module_map_cached`."""

    def test_corrupt_cache_is_rebuilt(self, caplog, monkeypatch, cache_dir):
        """A cache file that is not valid JSON is discarded and the map rebuilt."""
        monkeypatch.setattr(utils, "_build_check_module_map", lambda: FAKE_MAP)
        assert utils._get_check_module_map_cached() == FAKE_MAP
        (cache_file,) = cache_dir.glob("check_registry_*.json")
        cache_file.write_bytes(b"{not json")

        with caplog.at_level(logging.DEBUG):
            assert utils._get_check_module_map_cached() == FAKE_MAP

        assert "Check registry cache corrupted, rebuilding." in caplog.text

    def test_unwritable_cache_dir_still_returns_the_map(self, monkeypatch, tmp_path):
        """An unwritable cache location degrades to an uncached rebuild rather than raising."""
        # A regular file where the cache directory should be, so `mkdir` raises.
        blocked = tmp_path / "blocked"
        blocked.write_text("not a directory")
        monkeypatch.setattr(utils, "get_cache_dir", lambda: blocked)
        monkeypatch.setattr(utils, "_build_check_module_map", lambda: FAKE_MAP)

        assert utils._get_check_module_map_cached() == FAKE_MAP

    @pytest.mark.usefixtures("cache_dir")
    def test_second_call_reads_from_disk(self, monkeypatch):
        """A valid cache file short-circuits the rebuild."""
        monkeypatch.setattr(utils, "_build_check_module_map", lambda: FAKE_MAP)
        utils._get_check_module_map_cached()

        def _fail():
            raise AssertionError("_build_check_module_map should not be called again")

        monkeypatch.setattr(utils, "_build_check_module_map", _fail)

        assert utils._get_check_module_map_cached() == FAKE_MAP


class TestGetCheckObjectsForNames:
    """Tests for the targeted-import path and its fallbacks."""

    def test_unimportable_module_is_warned_and_skipped(self, caplog, monkeypatch):
        """A mapped module that will not import warns and yields no checks, rather than aborting the run."""
        monkeypatch.setattr(
            utils,
            "_get_check_module_map_cached",
            lambda _custom_checks_dir=None: {
                "check_fake": {
                    "module": "dbt_bouncer.checks.definitely_not_a_module",
                    "category": "manifest_checks",
                }
            },
        )

        with caplog.at_level(logging.WARNING):
            result = utils.get_check_objects_for_names(frozenset({"check_fake"}))

        assert result == []
        assert (
            "Failed to import check module: dbt_bouncer.checks.definitely_not_a_module"
            in caplog.text
        )

    def test_name_absent_from_cache_falls_back_to_full_scan(self, caplog, monkeypatch):
        """A requested name the cache does not know about triggers the full scan."""
        sentinel = [object()]
        monkeypatch.setattr(
            utils, "_get_check_module_map_cached", lambda _custom_checks_dir=None: {}
        )
        monkeypatch.setattr(
            utils, "get_check_objects", lambda _custom_checks_dir=None: sentinel
        )

        with caplog.at_level(logging.DEBUG):
            result = utils.get_check_objects_for_names(frozenset({"check_fake"}))

        assert result is sentinel
        assert "Falling back to full scan." in caplog.text

    def test_known_name_loads_only_its_module(self, monkeypatch):
        """A name present in the cache imports just the module that holds it."""
        monkeypatch.setattr(
            utils,
            "_get_check_module_map_cached",
            lambda _custom_checks_dir=None: FAKE_MAP,
        )

        result = utils.get_check_objects_for_names(frozenset({"check_fake"}))

        assert result
        assert all(c.__module__ == FAKE_MAP["check_fake"]["module"] for c in result)


@pytest.fixture
def _uncached_get_check_objects():
    """Clear `get_check_objects`'s `lru_cache` either side of a test.

    The session-scoped model-rebuild fixture populates it before any test runs,
    and this test deliberately loads a broken module set, which must not be left
    in the cache for later tests.
    """
    utils.get_check_objects.cache_clear()
    yield
    utils.get_check_objects.cache_clear()


class TestGetCheckObjects:
    """Tests for the full-scan check loader."""

    @pytest.mark.usefixtures("_uncached_get_check_objects")
    def test_unimportable_internal_module_is_warned_and_skipped(
        self, caplog, monkeypatch
    ):
        """One broken internal check module warns without preventing the others from loading."""
        broken = "dbt_bouncer.checks.manifest.models.naming"
        real_import_module = importlib.import_module

        def _import_module(name, *args, **kwargs):
            if name == broken:
                raise ImportError(f"simulated failure importing {name}")
            return real_import_module(name, *args, **kwargs)

        monkeypatch.setattr(utils.importlib, "import_module", _import_module)

        with caplog.at_level(logging.WARNING):
            result = utils.get_check_objects()

        assert f"Failed to import internal check module: {broken}" in caplog.text
        # Every other check module still loaded.
        assert result
        assert not any(c.__module__ == broken for c in result)


class TestLoadEntryPointChecks:
    """Tests for third-party check discovery via entry points."""

    @staticmethod
    def _entry_point(target, name="thirdparty", module="thirdparty.checks"):
        class _FakeEntryPoint:
            def __init__(self):
                self.name = name
                self.module = module

            def load(self):
                return target

        return _FakeEntryPoint()

    def test_non_module_target_is_warned_and_skipped(self, caplog, monkeypatch):
        """An entry point resolving to something that is neither a module nor a Check class warns."""
        monkeypatch.setattr(
            utils, "_check_entry_points", lambda: (self._entry_point(42),)
        )
        check_objects = []

        with caplog.at_level(logging.WARNING):
            utils._load_entry_point_checks(check_objects)

        assert check_objects == []
        assert "resolved to int, expected a module, package, or Check class" in (
            caplog.text
        )

    def test_unimportable_submodule_of_a_package_is_warned_and_skipped(
        self, caplog, monkeypatch, tmp_path
    ):
        """A package entry point whose submodule will not import warns and keeps walking."""
        (tmp_path / "broken.py").write_text("import definitely_not_installed\n")
        package = ModuleType("fake_check_pkg")
        package.__path__ = [str(tmp_path)]
        monkeypatch.setitem(sys.modules, "fake_check_pkg", package)
        monkeypatch.setattr(
            utils, "_check_entry_points", lambda: (self._entry_point(package),)
        )
        check_objects = []

        with caplog.at_level(logging.WARNING):
            utils._load_entry_point_checks(check_objects)

        assert check_objects == []
        assert (
            "Failed to import submodule `fake_check_pkg.broken` from entry point "
            "`thirdparty`." in caplog.text
        )

    def test_internal_entry_points_are_skipped(self, caplog, monkeypatch):
        """dbt-bouncer's own entry points are skipped, so the targeted-import fast path is not defeated."""
        loaded = []

        class _InternalEntryPoint:
            name = "manifest"
            module = "dbt_bouncer.checks.manifest"

            def load(self):
                loaded.append(self.module)
                raise AssertionError("internal entry points must not be loaded")

        monkeypatch.setattr(
            utils, "_check_entry_points", lambda: (_InternalEntryPoint(),)
        )
        check_objects = []

        with caplog.at_level(logging.DEBUG):
            utils._load_entry_point_checks(check_objects)

        assert loaded == []
        assert "Skipping internal entry point `manifest`." in caplog.text


class TestLoadConfigFromYaml:
    """Tests for `load_config_from_yaml`."""

    def test_missing_file_raises(self, tmp_path):
        """A config path that does not exist raises FileNotFoundError naming the path."""
        missing = tmp_path / "no_such_config.yml"

        with pytest.raises(FileNotFoundError, match="No config file found at"):
            utils.load_config_from_yaml(missing)
