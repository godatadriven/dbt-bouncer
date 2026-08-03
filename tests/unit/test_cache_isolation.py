"""Tests for the autouse `_clear_module_caches` fixture in `tests/conftest.py`.

Each assertion here holds only because the fixture ran first, whatever executed
before. A cache that stops being cleared makes exactly one of these fail, rather
than surfacing as an unrelated test mysteriously reading stale state.
"""

import pytest

from dbt_bouncer import runner, utils
from dbt_bouncer.checks.manifest import check_macros


@pytest.mark.parametrize(
    "cache_name",
    [
        pytest.param("_check_entry_points", id="check_entry_points"),
        pytest.param("_check_entry_point_names", id="check_entry_point_names"),
        # Populated by the session-scoped `_rebuild_all_check_models` fixture
        # before any test runs, so this is empty only if it is being cleared.
        pytest.param("get_check_objects", id="get_check_objects"),
    ],
)
def test_lru_cache_is_empty_at_test_start(cache_name: str) -> None:
    """Each memoised loader in `dbt_bouncer.utils` starts every test with an empty cache."""
    assert getattr(utils, cache_name).cache_info().currsize == 0


@pytest.mark.parametrize(
    ("module", "attribute"),
    [
        pytest.param(check_macros, "_USED_MACROS_CACHE", id="used_macros"),
        pytest.param(runner, "_CLASS_ITERATE_CACHE", id="class_iterate"),
    ],
)
def test_dict_cache_is_empty_at_test_start(module, attribute: str) -> None:
    """Each module-level dict cache starts every test empty."""
    assert getattr(module, attribute) == {}


def test_populating_a_cache_does_not_leak_out_of_this_test() -> None:
    """Populating the registry cache here is undone by the fixture's teardown."""
    utils.get_check_objects()

    assert utils.get_check_objects.cache_info().currsize == 1
