import os
from types import ModuleType
from typing import Any, Literal
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.utils import (
    _ESCAPED_SEPARATOR,
    _SEPARATOR,
    create_github_comment_file,
    flatten,
    get_clean_model_name,
    get_package_version_number,
    make_markdown_table,
    object_in_path,
)


def test_create_github_comment_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=False)
        assert (
            (tmp_path / "github-comment.md").read_text()
            == "## **Failed `dbt-bouncer`** checks\n\n\n| Check name | Failure message |\n| :--- | :--- |\n| check_model_description_populated | message_1 |\n| check_model_description_populated | message_2 |\n\n\nSent from this [GitHub Action workflow run](https://github.com/None/actions/runs/None)."
        )


def test_create_github_comment_file_show_all_failures_false(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
            ["check_model_description_populated", "message_3"],
            ["check_model_description_populated", "message_4"],
            ["check_model_description_populated", "message_5"],
            ["check_model_description_populated", "message_6"],
            ["check_model_description_populated", "message_7"],
            ["check_model_description_populated", "message_8"],
            ["check_model_description_populated", "message_9"],
            ["check_model_description_populated", "message_10"],
            ["check_model_description_populated", "message_11"],
            ["check_model_description_populated", "message_12"],
            ["check_model_description_populated", "message_13"],
            ["check_model_description_populated", "message_14"],
            ["check_model_description_populated", "message_15"],
            ["check_model_description_populated", "message_16"],
            ["check_model_description_populated", "message_17"],
            ["check_model_description_populated", "message_18"],
            ["check_model_description_populated", "message_19"],
            ["check_model_description_populated", "message_20"],
            ["check_model_description_populated", "message_21"],
            ["check_model_description_populated", "message_22"],
            ["check_model_description_populated", "message_23"],
            ["check_model_description_populated", "message_24"],
            ["check_model_description_populated", "message_25"],
            ["check_model_description_populated", "message_26"],
            ["check_model_description_populated", "message_27"],
            ["check_model_description_populated", "message_28"],
            ["check_model_description_populated", "message_29"],
            ["check_model_description_populated", "message_30"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=False)
        assert len((tmp_path / "github-comment.md").read_text().split("\n")) == 35


def test_create_github_comment_file_show_all_failures_true(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
            ["check_model_description_populated", "message_3"],
            ["check_model_description_populated", "message_4"],
            ["check_model_description_populated", "message_5"],
            ["check_model_description_populated", "message_6"],
            ["check_model_description_populated", "message_7"],
            ["check_model_description_populated", "message_8"],
            ["check_model_description_populated", "message_9"],
            ["check_model_description_populated", "message_10"],
            ["check_model_description_populated", "message_11"],
            ["check_model_description_populated", "message_12"],
            ["check_model_description_populated", "message_13"],
            ["check_model_description_populated", "message_14"],
            ["check_model_description_populated", "message_15"],
            ["check_model_description_populated", "message_16"],
            ["check_model_description_populated", "message_17"],
            ["check_model_description_populated", "message_18"],
            ["check_model_description_populated", "message_19"],
            ["check_model_description_populated", "message_20"],
            ["check_model_description_populated", "message_21"],
            ["check_model_description_populated", "message_22"],
            ["check_model_description_populated", "message_23"],
            ["check_model_description_populated", "message_24"],
            ["check_model_description_populated", "message_25"],
            ["check_model_description_populated", "message_26"],
            ["check_model_description_populated", "message_27"],
            ["check_model_description_populated", "message_28"],
            ["check_model_description_populated", "message_29"],
            ["check_model_description_populated", "message_30"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=True)
        assert len((tmp_path / "github-comment.md").read_text().split("\n")) == 38


@pytest.mark.parametrize(
    ("unique_id", "expected_model_name"),
    [
        ("model.my_project.my_model", "my_model"),
        ("model.my_project.my_model.v1", "my_model_v1"),
    ],
)
def test_get_clean_model_name(unique_id, expected_model_name):
    assert get_clean_model_name(unique_id) == expected_model_name


@pytest.mark.parametrize(
    ("data_in", "data_out"),
    [
        (
            {"a": 1, "b": 2},
            {">>a": 1, ">>b": 2},
        ),
        (
            {"a": 1, "b": {"c": 3, "d": 4}},
            {">>a": 1, ">>b>c": 3, ">>b>d": 4},
        ),
        (
            {"a": 1, "b": [{"c": 3, "d": 4}]},
            {">>a": 1, ">>b>0>c": 3, ">>b>0>d": 4},
        ),
    ],
)
def test_flatten(data_in, data_out):
    assert flatten(data_in) == data_out


@pytest.mark.parametrize(
    ("version_a", "version_b", "expectation"),
    [
        ("1.6.0", "1.6.0", True),
        ("1.6.1", "1.6.0", True),
        ("1.5.9", "1.6.0", False),
        ("2024.11.06+2a3d725", "1.6.0", True),
    ],
)
def test_get_package_version_number(version_a, version_b, expectation):
    assert (
        get_package_version_number(version_a) >= get_package_version_number(version_b)
    ) == expectation


@pytest.mark.parametrize(
    ("data_in", "data_out"),
    [
        (
            [
                [
                    "col_1",
                ],
                [
                    "a",
                ],
                [
                    "b",
                ],
            ],
            """
| col_1 |
| :--- |
| a |
| b |
""",
        ),
        (
            [
                [
                    "col_1",
                    "col_2",
                ],
                [
                    "a",
                    "1",
                ],
                [
                    "b",
                    "2",
                ],
            ],
            """
| col_1 | col_2 |
| :--- | :--- |
| a | 1 |
| b | 2 |
""",
        ),
        (
            [
                [
                    "col_1",
                    "col_2",
                    "col_3",
                ],
                [
                    "a",
                    "1",
                ],
                [
                    "b",
                    "2",
                ],
                [
                    "c",
                    "3",
                ],
            ],
            """
| col_1 | col_2 | col_3 |
| :--- | :--- | :--- |
| a | 1 |
| b | 2 |
| c | 3 |
""",
        ),
    ],
)
def test_make_markdown_table(data_in, data_out):
    assert make_markdown_table(data_in) == data_out


@pytest.mark.parametrize(
    ("include_pattern", "path", "output"),
    [
        ("^staging", "staging/model_1.sql", True),
        ("^staging", "staging\\model_1.sql", True),  # Windows path format
        (None, "staging/model_1.sql", True),
        ("^staging", "model_1.sql", False),
        ("^staging", "intermediate/model_1.sql", False),
    ],
)
def test_object_in_path(include_pattern, path, output):
    assert object_in_path(include_pattern, path) == output


# --- Tests for _extract_checks_from_module ---


class TestExtractChecksFromModule:
    """Tests for the _extract_checks_from_module helper function."""

    def test_extracts_check_classes(self):
        """Test that Check* classes are extracted from a module."""
        import types

        from dbt_bouncer.utils import _extract_checks_from_module

        # Create a mock module with a Check class
        mock_module = types.ModuleType("test_module")
        mock_module.__name__ = "test_module"

        class CheckExample:
            pass

        CheckExample.__module__ = "test_module"
        mock_module.CheckExample = CheckExample  # type: ignore[attr-defined]

        check_objects: list[Any] = []
        _extract_checks_from_module(mock_module, "test_module", check_objects)

        assert len(check_objects) == 1
        assert check_objects[0] is CheckExample

    def test_ignores_non_check_classes(self):
        """Test that non-Check* classes are ignored."""
        import types

        from dbt_bouncer.utils import _extract_checks_from_module

        mock_module = types.ModuleType("test_module")

        class SomeHelper:
            pass

        SomeHelper.__module__ = "test_module"
        mock_module.SomeHelper = SomeHelper  # type: ignore[attr-defined]

        check_objects: list[Any] = []
        _extract_checks_from_module(mock_module, "test_module", check_objects)

        assert len(check_objects) == 0

    def test_ignores_imported_check_classes(self):
        """Test that Check* classes imported from other modules are ignored."""
        import types

        from dbt_bouncer.utils import _extract_checks_from_module

        mock_module = types.ModuleType("test_module")

        class CheckImported:
            pass

        # Simulate an imported class by setting a different __module__
        CheckImported.__module__ = "other_module"
        mock_module.CheckImported = CheckImported  # type: ignore[attr-defined]

        check_objects: list[Any] = []
        _extract_checks_from_module(mock_module, "test_module", check_objects)

        assert len(check_objects) == 0


# --- Tests for _load_custom_checks ---


class TestLoadCustomChecks:
    """Tests for the _load_custom_checks helper function."""

    def test_loads_custom_check_from_directory(self, tmp_path):
        """Test loading a custom check from a directory."""
        from dbt_bouncer.utils import _load_custom_checks

        # Create directory structure: custom_checks/manifest/check_custom.py
        custom_dir = tmp_path / "custom_checks"
        manifest_dir = custom_dir / "manifest"
        manifest_dir.mkdir(parents=True)

        check_file = manifest_dir / "check_custom.py"
        check_file.write_text(
            """
class CheckCustomExample:
    pass
"""
        )

        check_objects: list[Any] = []
        _load_custom_checks(custom_dir, check_objects)

        assert len(check_objects) == 1
        assert check_objects[0].__name__ == "CheckCustomExample"

    def test_warns_on_invalid_check_file(self, tmp_path, caplog):
        """Test that a warning is logged and the file is skipped when a custom check file is invalid."""
        import logging

        from dbt_bouncer.utils import _load_custom_checks

        custom_dir = tmp_path / "custom_checks"
        manifest_dir = custom_dir / "manifest"
        manifest_dir.mkdir(parents=True)

        check_file = manifest_dir / "check_broken.py"
        check_file.write_text("this is not valid python syntax !!!")

        check_objects: list[Any] = []
        with caplog.at_level(logging.WARNING):
            # Should not raise — the file is skipped with a warning
            _load_custom_checks(custom_dir, check_objects)

        assert any("check_broken.py" in msg for msg in caplog.messages)
        assert any("skipped" in msg.lower() for msg in caplog.messages)

    def test_warns_on_nonexistent_directory(self, tmp_path, caplog):
        """Test that a warning is logged when the custom check directory does not exist."""
        import logging

        from dbt_bouncer.utils import _load_custom_checks

        nonexistent_dir = tmp_path / "does_not_exist"

        check_objects: list[Any] = []
        with caplog.at_level(logging.WARNING):
            _load_custom_checks(nonexistent_dir, check_objects)

        assert "does not exist" in caplog.text


def test_flatten_key_with_separator():
    """Keys containing the path separator are escaped and distinguished from nested paths."""
    flat_nested = flatten({"a": {"b": 1}})
    flat_with_sep = flatten({"a>b": 1})

    # A key containing > must not produce the same flat key as a nested dict
    assert flat_with_sep != flat_nested

    # The separator in the key is escaped; the path separator is not
    expected_key = f"{_SEPARATOR}{_SEPARATOR}a{_ESCAPED_SEPARATOR}b"
    separator_key = f"{_SEPARATOR}{_SEPARATOR}a{_SEPARATOR}b"

    assert expected_key in flat_with_sep
    assert separator_key not in flat_with_sep
    assert separator_key in flat_nested


# -- Helpers for entry point tests --


class CheckEntryPointFake(BaseCheck):
    """Fake check for entry point testing."""

    name: Literal["check_entry_point_fake"]

    def execute(self) -> None:
        """No-op execute for testing."""


def _make_fake_ep(name: str, target: object) -> MagicMock:
    """Create a fake entry point that loads to the given target.

    Returns:
        A MagicMock mimicking an importlib.metadata entry point.

    """
    ep = MagicMock()
    ep.name = name
    ep.load.return_value = target
    return ep


# -- Entry point tests --


def test_load_entry_point_checks_discovers_from_module():
    """Entry point pointing to a module discovers Check* classes."""
    from dbt_bouncer.utils import _load_entry_point_checks

    fake_module = ModuleType("fake_plugin.checks")
    fake_module.CheckEntryPointFake = CheckEntryPointFake  # type: ignore[attr-defined]
    original_module = CheckEntryPointFake.__module__
    CheckEntryPointFake.__module__ = "fake_plugin.checks"

    ep = _make_fake_ep("fake_plugin", fake_module)
    check_objects: list[type[BaseCheck]] = []
    with patch("dbt_bouncer.utils.entry_points", return_value=[ep]):
        _load_entry_point_checks(check_objects)

    CheckEntryPointFake.__module__ = original_module
    assert CheckEntryPointFake in check_objects


def test_load_entry_point_checks_discovers_single_class():
    """Entry point pointing directly to a class is added."""
    from dbt_bouncer.utils import _load_entry_point_checks

    ep = _make_fake_ep("direct_class", CheckEntryPointFake)
    check_objects: list[type[BaseCheck]] = []
    with patch("dbt_bouncer.utils.entry_points", return_value=[ep]):
        _load_entry_point_checks(check_objects)

    assert CheckEntryPointFake in check_objects


def test_load_entry_point_checks_handles_import_error():
    """Entry points that fail to load are skipped with a warning."""
    from dbt_bouncer.utils import _load_entry_point_checks

    ep = _make_fake_ep("broken_plugin", None)
    ep.load.side_effect = ImportError("no such module")

    check_objects: list[type[BaseCheck]] = []
    with patch("dbt_bouncer.utils.entry_points", return_value=[ep]):
        _load_entry_point_checks(check_objects)

    assert check_objects == []


def test_load_entry_point_checks_walks_packages():
    """Entry point pointing to a package recursively discovers submodule checks."""
    from dbt_bouncer.utils import _load_entry_point_checks

    # Simulate a package with a submodule containing a check class
    fake_package = ModuleType("fake_pkg")
    fake_package.__path__ = ["/fake/path"]  # type: ignore[attr-defined]
    fake_package.__name__ = "fake_pkg"

    fake_submodule = ModuleType("fake_pkg.sub")
    fake_submodule.CheckEntryPointFake = CheckEntryPointFake  # type: ignore[attr-defined]
    original_module = CheckEntryPointFake.__module__
    CheckEntryPointFake.__module__ = "fake_pkg.sub"

    def mock_walk(_path, **_kwargs):
        yield (None, "fake_pkg.sub", False)

    check_objects: list[type[BaseCheck]] = []
    with (
        patch(
            "dbt_bouncer.utils.entry_points",
            return_value=[_make_fake_ep("fake_pkg", fake_package)],
        ),
        patch("dbt_bouncer.utils.pkgutil.walk_packages", side_effect=mock_walk),
        patch("importlib.import_module", return_value=fake_submodule),
    ):
        _load_entry_point_checks(check_objects)

    CheckEntryPointFake.__module__ = original_module
    assert CheckEntryPointFake in check_objects


def test_get_check_objects_includes_entry_points():
    """get_check_objects() integrates entry point checks."""
    from dbt_bouncer.utils import get_check_objects

    fake_module = ModuleType("ep_plugin.checks")
    fake_module.CheckEntryPointFake = CheckEntryPointFake  # type: ignore[attr-defined]
    original_module = CheckEntryPointFake.__module__
    CheckEntryPointFake.__module__ = "ep_plugin.checks"

    ep = _make_fake_ep("ep_plugin", fake_module)
    get_check_objects.cache_clear()

    with patch("dbt_bouncer.utils.entry_points", return_value=[ep]):
        results = get_check_objects()

    CheckEntryPointFake.__module__ = original_module
    get_check_objects.cache_clear()
    assert CheckEntryPointFake in results


def test_get_check_objects_deduplicates():
    """Same class from multiple sources appears only once."""
    from dbt_bouncer.utils import get_check_objects

    fake_module = ModuleType("dedup_plugin.checks")
    fake_module.CheckEntryPointFake = CheckEntryPointFake  # type: ignore[attr-defined]
    original_module = CheckEntryPointFake.__module__
    CheckEntryPointFake.__module__ = "dedup_plugin.checks"

    # Create two entry points that both resolve to the same class
    ep1 = _make_fake_ep("plugin1", fake_module)
    ep2 = _make_fake_ep("plugin2", fake_module)
    get_check_objects.cache_clear()

    with patch("dbt_bouncer.utils.entry_points", return_value=[ep1, ep2]):
        results = get_check_objects()

    CheckEntryPointFake.__module__ = original_module
    get_check_objects.cache_clear()
    count = sum(1 for cls in results if cls is CheckEntryPointFake)
    assert count == 1
