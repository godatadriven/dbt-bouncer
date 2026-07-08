import pytest

from dbt_bouncer.testing import check_fails, check_passes


def _seed_catalog_node(stats: dict) -> dict:
    """Build a minimal catalog node dict for a seed with the given stats.

    Returns:
        dict: A catalog node dict suitable for passing to ``check_passes`` /
        ``check_fails`` as the ``catalog_node`` override.

    """
    return {
        "stats": stats,
        "unique_id": "seed.package_name.raw_customers",
    }


def _row_stat(value: int, key: str = "row_count") -> dict:
    """Build a stats dict containing a row-count stat under ``key``.

    Returns:
        dict: A ``stats`` block with ``has_stats=True`` and the row count value.

    """
    return {
        "has_stats": {
            "id": "has_stats",
            "include": False,
            "label": "Has Stats?",
            "value": True,
        },
        key: {
            "id": key,
            "include": True,
            "label": "Row Count",
            "value": value,
        },
    }


def _byte_stat(value: int, key: str = "bytes") -> dict:
    """Build a stats dict containing a byte-count stat under ``key``.

    Returns:
        dict: A ``stats`` block with ``has_stats=True`` and the byte count value.

    """
    return {
        "has_stats": {
            "id": "has_stats",
            "include": False,
            "label": "Has Stats?",
            "value": True,
        },
        key: {
            "id": key,
            "include": True,
            "label": "Approximate Size",
            "value": value,
        },
    }


class TestCheckSeedMaxBytes:
    @pytest.mark.parametrize("stat_key", ["bytes", "num_bytes", "size"])
    def test_pass_under_limit(self, stat_key):
        check_passes(
            "check_seed_max_bytes",
            catalog_node=_seed_catalog_node(_byte_stat(500, key=stat_key)),
            max_bytes=1024,
        )

    @pytest.mark.parametrize("stat_key", ["bytes", "num_bytes", "size"])
    def test_fail_over_limit(self, stat_key):
        check_fails(
            "check_seed_max_bytes",
            catalog_node=_seed_catalog_node(_byte_stat(2048, key=stat_key)),
            max_bytes=1024,
        )

    @pytest.mark.parametrize(
        ("catalog_node", "check_fn"),
        [
            pytest.param(
                # The check uses strict ``>`` so a seed exactly at the limit passes.
                _seed_catalog_node(_byte_stat(1024)),
                check_passes,
                id="pass_at_exact_limit",
            ),
            pytest.param(
                # A model catalog node with no byte stat must not raise.
                {
                    "stats": {},
                    "unique_id": "model.package_name.model_1",
                },
                check_passes,
                id="non_seed_catalog_node_is_skipped",
            ),
        ],
    )
    def test_check_seed_max_bytes(self, catalog_node, check_fn):
        # ``byte_stat_keys`` is intentionally omitted so these cases exercise the
        # check's default stat-key handling.
        check_fn(
            "check_seed_max_bytes",
            catalog_node=catalog_node,
            max_bytes=1024,
        )

    @pytest.mark.parametrize(
        ("catalog_node", "check_fn"),
        [
            pytest.param(
                # Long-tail adapter that exposes ``size_bytes`` instead of any default key.
                _seed_catalog_node(_byte_stat(500, key="size_bytes")),
                check_passes,
                id="custom_byte_stat_key_pass",
            ),
            pytest.param(
                _seed_catalog_node(_byte_stat(2048, key="size_bytes")),
                check_fails,
                id="custom_byte_stat_key_fail",
            ),
        ],
    )
    def test_check_seed_max_bytes_custom_stat_key(self, catalog_node, check_fn):
        check_fn(
            "check_seed_max_bytes",
            catalog_node=catalog_node,
            max_bytes=1024,
            byte_stat_keys=["size_bytes"],
        )

    def test_missing_stats_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node=_seed_catalog_node({}),
                max_bytes=1024,
            )

    def test_null_stats_raises_runtime_error(self):
        # Adapter where the ``stats`` field is serialised as ``null`` rather than ``{}``.
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node={
                    "stats": None,
                    "unique_id": "seed.package_name.raw_customers",
                },
                max_bytes=1024,
            )

    def test_has_stats_false_raises_runtime_error(self):
        # DuckDB-style: ``has_stats`` present and false, no other keys.
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node=_seed_catalog_node(
                    {
                        "has_stats": {
                            "id": "has_stats",
                            "include": False,
                            "label": "Has Stats?",
                            "value": False,
                        }
                    }
                ),
                max_bytes=1024,
            )

    def test_invalid_max_bytes_raises_value_error(self):
        with pytest.raises(ValueError, match="must be positive"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node=_seed_catalog_node(_byte_stat(100)),
                max_bytes=0,
            )

    def test_empty_byte_stat_keys_raises_value_error(self):
        with pytest.raises(ValueError, match="must not be empty"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node=_seed_catalog_node(_byte_stat(100)),
                max_bytes=1024,
                byte_stat_keys=[],
            )

    def test_default_keys_no_longer_match_when_user_narrows(self):
        # Stat is under a default key, but user has narrowed to a single non-matching key.
        with pytest.raises(RuntimeError, match=r"\['size_bytes'\]"):
            check_passes(
                "check_seed_max_bytes",
                catalog_node=_seed_catalog_node(_byte_stat(100, key="bytes")),
                max_bytes=1024,
                byte_stat_keys=["size_bytes"],
            )


class TestCheckSeedMaxRowCount:
    @pytest.mark.parametrize("stat_key", ["row_count", "num_rows", "rows"])
    def test_pass_under_limit(self, stat_key):
        check_passes(
            "check_seed_max_row_count",
            catalog_node=_seed_catalog_node(_row_stat(50, key=stat_key)),
            max_row_count=100,
        )

    @pytest.mark.parametrize("stat_key", ["row_count", "num_rows", "rows"])
    def test_fail_over_limit(self, stat_key):
        check_fails(
            "check_seed_max_row_count",
            catalog_node=_seed_catalog_node(_row_stat(200, key=stat_key)),
            max_row_count=100,
        )

    @pytest.mark.parametrize(
        ("catalog_node", "check_fn"),
        [
            pytest.param(
                # The check uses strict ``>`` so a seed exactly at the limit passes.
                _seed_catalog_node(_row_stat(100)),
                check_passes,
                id="pass_at_exact_limit",
            ),
            pytest.param(
                {
                    "stats": {},
                    "unique_id": "model.package_name.model_1",
                },
                check_passes,
                id="non_seed_catalog_node_is_skipped",
            ),
        ],
    )
    def test_check_seed_max_row_count(self, catalog_node, check_fn):
        # ``row_stat_keys`` is intentionally omitted so these cases exercise the
        # check's default stat-key handling.
        check_fn(
            "check_seed_max_row_count",
            catalog_node=catalog_node,
            max_row_count=100,
        )

    @pytest.mark.parametrize(
        ("catalog_node", "check_fn"),
        [
            pytest.param(
                # Long-tail adapter that exposes ``record_count`` instead of any default key.
                _seed_catalog_node(_row_stat(50, key="record_count")),
                check_passes,
                id="custom_row_stat_key_pass",
            ),
            pytest.param(
                _seed_catalog_node(_row_stat(200, key="record_count")),
                check_fails,
                id="custom_row_stat_key_fail",
            ),
        ],
    )
    def test_check_seed_max_row_count_custom_stat_key(self, catalog_node, check_fn):
        check_fn(
            "check_seed_max_row_count",
            catalog_node=catalog_node,
            max_row_count=100,
            row_stat_keys=["record_count"],
        )

    def test_missing_stats_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node=_seed_catalog_node({}),
                max_row_count=100,
            )

    def test_null_stats_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node={
                    "stats": None,
                    "unique_id": "seed.package_name.raw_customers",
                },
                max_row_count=100,
            )

    def test_has_stats_false_raises_runtime_error(self):
        with pytest.raises(RuntimeError, match="does not expose"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node=_seed_catalog_node(
                    {
                        "has_stats": {
                            "id": "has_stats",
                            "include": False,
                            "label": "Has Stats?",
                            "value": False,
                        }
                    }
                ),
                max_row_count=100,
            )

    def test_invalid_max_row_count_raises_value_error(self):
        with pytest.raises(ValueError, match="must be positive"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node=_seed_catalog_node(_row_stat(10)),
                max_row_count=-1,
            )

    def test_empty_row_stat_keys_raises_value_error(self):
        with pytest.raises(ValueError, match="must not be empty"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node=_seed_catalog_node(_row_stat(10)),
                max_row_count=100,
                row_stat_keys=[],
            )

    def test_default_keys_no_longer_match_when_user_narrows(self):
        with pytest.raises(RuntimeError, match=r"\['record_count'\]"):
            check_passes(
                "check_seed_max_row_count",
                catalog_node=_seed_catalog_node(_row_stat(10, key="row_count")),
                max_row_count=100,
                row_stat_keys=["record_count"],
            )


class TestCheckSeedColumnsAreAllDocumented:
    @pytest.mark.parametrize(
        ("ctx_seeds", "check_fn"),
        [
            pytest.param(
                [
                    {
                        "alias": "raw_customers",
                        "columns": {
                            "id": {"name": "id"},
                            "first_name": {"name": "first_name"},
                            "last_name": {"name": "last_name"},
                        },
                        "fqn": ["package_name", "raw_customers"],
                        "name": "raw_customers",
                        "original_file_path": "seeds/raw_customers.csv",
                        "path": "raw_customers.csv",
                        "unique_id": "seed.package_name.raw_customers",
                    }
                ],
                check_passes,
                id="all_columns_documented",
            ),
            pytest.param(
                [
                    {
                        "alias": "raw_customers",
                        "columns": {
                            "id": {"name": "id"},
                            "first_name": {"name": "first_name"},
                        },
                        "fqn": ["package_name", "raw_customers"],
                        "name": "raw_customers",
                        "original_file_path": "seeds/raw_customers.csv",
                        "path": "raw_customers.csv",
                        "unique_id": "seed.package_name.raw_customers",
                    }
                ],
                check_fails,
                id="missing_last_name_column",
            ),
        ],
    )
    def test_check_seed_columns_are_all_documented(self, ctx_seeds, check_fn):
        check_fn(
            "check_seed_columns_are_all_documented",
            catalog_node={
                "columns": {
                    "id": {"name": "id", "type": "INTEGER", "index": 1},
                    "first_name": {
                        "name": "first_name",
                        "type": "VARCHAR",
                        "index": 2,
                    },
                    "last_name": {
                        "name": "last_name",
                        "type": "VARCHAR",
                        "index": 3,
                    },
                },
                "metadata": {
                    "database": "dbt",
                    "name": "raw_customers",
                    "schema": "main",
                    "type": "BASE TABLE",
                },
                "unique_id": "seed.package_name.raw_customers",
            },
            ctx_seeds=ctx_seeds,
        )
