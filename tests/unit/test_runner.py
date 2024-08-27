import json
import warnings
from pathlib import Path

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parser import parse_manifest
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4

from unittest.mock import MagicMock

from click.globals import push_context

from dbt_bouncer.checks.manifest.check_models import CheckModelDescriptionPopulated
from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.parsers import DbtBouncerModel
from dbt_bouncer.runner import runner


def test_runner_coverage(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    ctx = MagicMock(obj={"verbosity": 3})
    push_context(ctx)

    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                CheckModelDescriptionPopulated(
                    **{
                        "exclude": None,
                        "include": None,
                        "index": 0,
                        "name": "check_model_description_populated",
                    }
                )
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
                    "original_file_path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=tmp_path / "coverage.json",
        run_results=[],
        sources=[],
        tests=[],
        unit_tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert (tmp_path / "coverage.json").exists()
    assert len(coverage) == 1
    assert f"Saving coverage file to `{tmp_path}/coverage.json`" in caplog.text


def test_runner_failure():
    ctx = MagicMock(obj={"verbosity": 3})
    push_context(ctx)

    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                CheckModelDescriptionPopulated(
                    **{
                        "exclude": None,
                        "include": None,
                        "index": 0,
                        "name": "check_model_description_populated",
                    }
                )
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
                    "original_file_path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=None,
        run_results=[],
        sources=[],
        tests=[],
        unit_tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )
    assert results[0] == 1


def test_runner_success():
    ctx = MagicMock(obj={"verbosity": 3})
    push_context(ctx)

    results = runner(
        bouncer_config={
            "check_model_description_populated": [
                CheckModelDescriptionPopulated(
                    **{
                        "exclude": None,
                        "include": None,
                        "index": 0,
                        "name": "check_model_description_populated",
                    }
                )
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
                    "original_file_path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                }
            )
        ],
        output_file=None,
        run_results=[],
        sources=[],
        tests=[],
        unit_tests=[],
        checks_dir=Path("./src/dbt_bouncer/checks"),
    )
    assert results[0] == 0
