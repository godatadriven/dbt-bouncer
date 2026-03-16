import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelLatestVersionSpecified:
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param({"latest_version": 2}, id="latest_version_integer"),
            pytest.param({"latest_version": "stable"}, id="latest_version_string"),
        ],
    )
    def test_passes(self, model):
        check_passes("check_model_latest_version_specified", model=model)

    def test_fails_missing_latest_version(self):
        check_fails(
            "check_model_latest_version_specified",
            model={"latest_version": None},
        )


class TestCheckModelVersionAllowed:
    @pytest.mark.parametrize(
        ("model", "version_pattern"),
        [
            pytest.param(
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "schema": "dbt_jdoe_stg_domain",
                    "unique_id": "model.package_name.stg_model_1",
                    "version": 1,
                },
                r"[0-9]\d*",
                id="allowed_version_1",
            ),
            pytest.param(
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "schema": "dbt_jdoe_stg_domain",
                    "unique_id": "model.package_name.stg_model_1",
                    "version": 10,
                },
                r"[0-9]\d*",
                id="allowed_version_10",
            ),
            pytest.param(
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "schema": "dbt_jdoe_stg_domain",
                    "unique_id": "model.package_name.stg_model_1",
                    "version": 100,
                },
                r"[0-9]\d*",
                id="allowed_version_100",
            ),
        ],
    )
    def test_passes(self, model, version_pattern):
        check_passes(
            "check_model_version_allowed",
            model=model,
            version_pattern=version_pattern,
        )

    def test_fails_disallowed_version(self):
        check_fails(
            "check_model_version_allowed",
            model={
                "alias": "stg_model_1",
                "fqn": ["package_name", "stg_model_1"],
                "name": "stg_model_1",
                "original_file_path": "models/staging/stg_model_1.sql",
                "path": "staging/stg_model_1.sql",
                "schema": "dbt_jdoe_stg_domain",
                "unique_id": "model.package_name.stg_model_1",
                "version": "golden",
            },
            version_pattern=r"[0-9]\d*",
        )


_VERSIONED_NODES = {
    "model.package_name.stg_model_1": {
        "alias": "stg_model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "stg_model_1"],
        "name": "stg_model_1",
        "original_file_path": "models/staging/stg_model_1.sql",
        "package_name": "package_name",
        "path": "staging/stg_model_1.sql",
        "resource_type": "model",
        "schema": "dbt_jdoe_stg_domain",
        "unique_id": "model.package_name.stg_model_1",
        "version": 1,
    },
}

_VERSIONED_MODEL = {
    "alias": "stg_model_1",
    "checksum": {"name": "sha256", "checksum": ""},
    "columns": {
        "col_1": {
            "index": 1,
            "name": "col_1",
            "type": "INTEGER",
        },
    },
    "fqn": ["package_name", "stg_model_1"],
    "name": "stg_model_1",
    "original_file_path": "models/staging/stg_model_1.sql",
    "package_name": "package_name",
    "path": "staging/stg_model_1.sql",
    "resource_type": "model",
    "schema": "dbt_jdoe_stg_domain",
    "unique_id": "model.package_name.stg_model_1",
    "version": 1,
}


class TestCheckModelVersionPinnedInRef:
    def test_passes_pinned_version(self):
        check_passes(
            "check_model_version_pinned_in_ref",
            model=_VERSIONED_MODEL,
            ctx_manifest_obj={
                "child_map": {
                    "model.package_name.stg_model_1": [
                        "model.package_name.stg_model_2",
                    ],
                },
                "nodes": {
                    **_VERSIONED_NODES,
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": 1}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            },
        )

    def test_fails_unpinned_version(self):
        check_fails(
            "check_model_version_pinned_in_ref",
            model=_VERSIONED_MODEL,
            ctx_manifest_obj={
                "child_map": {
                    "model.package_name.stg_model_1": [
                        "model.package_name.stg_model_2",
                    ],
                },
                "nodes": {
                    **_VERSIONED_NODES,
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": None}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            },
        )
