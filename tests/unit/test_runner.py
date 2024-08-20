import json
import warnings
from pathlib import Path

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parser import parse_manifest
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4

from dbt_bouncer.parsers import DbtBouncerModel
from dbt_bouncer.runner import runner


def test_runner_coverage(caplog, tmp_path):
    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                {"catalog_checks": [], "run_results_checks": [], "index": 0}
            ]
        },
        catalog_nodes=[],
        catalog_sources=[],
        create_pr_comment_file=False,
        exposures=[],
        macros=[],
        manifest_obj=parse_manifest(
            {
                "child_map": {},
                "disabled": {},
                "docs": {},
                "exposures": {},
                "group_map": {},
                "groups": {},
                "macros": {},
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                    "project_name": "dbt_bouncer_test_project",
                },
                "metrics": {},
                "nodes": {},
                "parent_map": {},
                "saved_queries": {},
                "selectors": {},
                "semantic_models": {},
                "sources": {},
                "unit_tests": {},
            }
        ),
        models=[
            DbtBouncerModel(
                **{
                    "model": Nodes4(
                        **{
                            "access": "public",
                            "alias": "stg_payments",
                            "checksum": {"name": "sha256", "checksum": ""},
                            "columns": {
                                "col_1": {
                                    "index": 1,
                                    "name": "col_1",
                                    "type": "INTEGER",
                                },
                            },
                            "description": "This is a description",
                            "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                            "name": "stg_payments",
                            "original_file_path": "models/staging/stg_payments.sql",
                            "package_name": "dbt_bouncer_test_project",
                            "path": "staging/stg_payments.sql",
                            "resource_type": "model",
                            "schema": "main",
                            "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                        }
                    ),
                    "path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=tmp_path / "coverage.json",
        run_results=[],
        sources=[],
        tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert (tmp_path / "coverage.json").exists()
    assert len(coverage) == 1
    assert f"Saving coverage file to `{tmp_path}/coverage.json`" in caplog.text


def test_runner_failure():
    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                {"catalog_checks": [], "run_results_checks": [], "index": 0}
            ]
        },
        catalog_nodes=[],
        catalog_sources=[],
        create_pr_comment_file=False,
        exposures=[],
        macros=[],
        manifest_obj=parse_manifest(
            {
                "child_map": {},
                "disabled": {},
                "docs": {},
                "exposures": {},
                "group_map": {},
                "groups": {},
                "macros": {},
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                    "project_name": "dbt_bouncer_test_project",
                },
                "metrics": {},
                "nodes": {},
                "parent_map": {},
                "saved_queries": {},
                "selectors": {},
                "semantic_models": {},
                "sources": {},
                "unit_tests": {},
            }
        ),
        models=[
            DbtBouncerModel(
                **{
                    "model": Nodes4(
                        **{
                            "access": "public",
                            "alias": "stg_payments",
                            "checksum": {"name": "sha256", "checksum": ""},
                            "columns": {
                                "col_1": {
                                    "index": 1,
                                    "name": "col_1",
                                    "type": "INTEGER",
                                },
                            },
                            "description": "",
                            "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                            "name": "stg_payments",
                            "original_file_path": "models/staging/stg_payments.sql",
                            "package_name": "dbt_bouncer_test_project",
                            "path": "staging/stg_payments.sql",
                            "resource_type": "model",
                            "schema": "main",
                            "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                        }
                    ),
                    "path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=None,
        run_results=[],
        sources=[],
        tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )
    assert results[0] == 1


def test_runner_success():
    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                {"catalog_checks": [], "run_results_checks": [], "index": 0}
            ]
        },
        catalog_nodes=[],
        catalog_sources=[],
        create_pr_comment_file=False,
        exposures=[],
        macros=[],
        manifest_obj=parse_manifest(
            {
                "child_map": {},
                "disabled": {},
                "docs": {},
                "exposures": {},
                "group_map": {},
                "groups": {},
                "macros": {},
                "metadata": {
                    "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
                    "project_name": "dbt_bouncer_test_project",
                },
                "metrics": {},
                "nodes": {},
                "parent_map": {},
                "saved_queries": {},
                "selectors": {},
                "semantic_models": {},
                "sources": {},
                "unit_tests": {},
            }
        ),
        models=[
            DbtBouncerModel(
                **{
                    "model": Nodes4(
                        **{
                            "access": "public",
                            "alias": "stg_payments",
                            "checksum": {"name": "sha256", "checksum": ""},
                            "columns": {
                                "col_1": {
                                    "index": 1,
                                    "name": "col_1",
                                    "type": "INTEGER",
                                },
                            },
                            "description": "This is a description",
                            "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                            "name": "stg_payments",
                            "original_file_path": "models/staging/stg_payments.sql",
                            "package_name": "dbt_bouncer_test_project",
                            "path": "staging/stg_payments.sql",
                            "resource_type": "model",
                            "schema": "main",
                            "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                        }
                    ),
                    "path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=None,
        run_results=[],
        sources=[],
        tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )
    assert results[0] == 0
