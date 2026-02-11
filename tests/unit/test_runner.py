import json
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import click
import pytest
from click.globals import push_context

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes4,
)
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerModel,
)
from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.main import cli
from dbt_bouncer.runner import runner


def test_runner_coverage(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        results = runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
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
                                "config": {"meta": None},
                                "description": "This is a description",
                                "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=tmp_path / "coverage.json",
            output_format="json",
            output_only_failures=False,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert (tmp_path / "coverage.json").exists()
    assert len(coverage) == 1
    assert f"Saving coverage file to `{tmp_path}/coverage.json`".replace(
        "\\", "/"
    ) in caplog.text.replace("\\", "/")


def test_runner_failure():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        results = runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
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
                                "config": {"meta": None},
                                "description": "",
                                "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=None,
            output_format="json",
            output_only_failures=False,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    assert results[0] == 1


def test_runner_skip(tmp_path):
    configure_console_logging(verbosity=0)
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        results = runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
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
                                "config": {"meta": None},
                                "description": "This is a description",
                                "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
                DbtBouncerModel(
                    **{
                        "model": Nodes4(
                            **{
                                "access": "public",
                                "alias": "stg_orders",
                                "checksum": {"name": "sha256", "checksum": ""},
                                "columns": {
                                    "col_1": {
                                        "index": 1,
                                        "name": "col_1",
                                        "type": "INTEGER",
                                    },
                                },
                                "config": {
                                    "meta": {
                                        "dbt-bouncer": {
                                            "skip_checks": [
                                                "check_model_description_populated"
                                            ]
                                        }
                                    }
                                },
                                "description": None,
                                "fqn": ["dbt_bouncer_test_project", "stg_orders"],
                                "name": "stg_orders",
                                "original_file_path": "models/staging/stg_orders.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_orders.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                            },
                        ),
                        "original_file_path": "models/staging/stg_orders.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                    },
                ),
            ],
            output_file=tmp_path / "coverage.json",
            output_format="json",
            output_only_failures=False,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert len(coverage) == 1


def test_runner_success():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        results = runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
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
                                "config": {"meta": None},
                                "description": "This is a description",
                                "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=None,
            output_format="json",
            output_only_failures=False,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    assert results[0] == 0


def test_runner_windows(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    ctx = MagicMock(obj={"verbosity": 3})
    push_context(ctx)
    from dbt_bouncer.config_file_parser import (
        DbtBouncerConfAllCategories as DbtBouncerConf,
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parser import parse_manifest

    results = runner(
        bouncer_config=DbtBouncerConf(
            **{
                "manifest_checks": [
                    {
                        "exclude": None,
                        "include": None,
                        "index": 0,
                        "name": "check_model_documented_in_same_directory",
                    },
                ]
            }
        ),
        catalog_nodes=[],
        catalog_sources=[],
        check_categories=["manifest_checks"],
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
            },
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
                            "config": {"meta": None},
                            "description": "This is a description",
                            "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                            "name": "stg_payments",
                            "original_file_path": "models\\staging\\stg_payments.sql",
                            "package_name": "dbt_bouncer_test_project",
                            "patch_path": "dbt_bouncer_test_project://models\\staging\\_stg__models.yml",
                            "path": "staging\\stg_payments.sql",
                            "resource_type": "model",
                            "schema": "main",
                            "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                        },
                    ),
                    "original_file_path": "models/staging/stg_payments.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                },
            ),
        ],
        output_file=tmp_path / "coverage.json",
        output_format="json",
        output_only_failures=False,
        run_results=[],
        seeds=[],
        semantic_models=[],
        snapshots=[],
        show_all_failures=False,
        sources=[],
        tests=[],
        unit_tests=[],
    )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert (tmp_path / "coverage.json").exists()
    assert len(coverage) == 1
    assert f"Saving coverage file to `{tmp_path}/coverage.json`".replace(
        "\\", "/"
    ) in caplog.text.replace("\\", "/")


def test_runner_check_id(tmp_path):
    configure_console_logging(verbosity=0)
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        results = runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
            ),
            models=[
                DbtBouncerModel(
                    **{
                        "model": Nodes4(
                            **{
                                "access": "public",
                                "alias": "stg_payments_v1",
                                "checksum": {"name": "sha256", "checksum": ""},
                                "columns": {
                                    "col_1": {
                                        "index": 1,
                                        "name": "col_1",
                                        "type": "INTEGER",
                                    },
                                },
                                "config": {"meta": None},
                                "description": "This is a description",
                                "fqn": [
                                    "dbt_bouncer_test_project",
                                    "stg_payments",
                                    "v1",
                                ],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments_v1.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments_v1.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments.v1",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments_v1.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments.v1",
                    },
                ),
                DbtBouncerModel(
                    **{
                        "model": Nodes4(
                            **{
                                "access": "public",
                                "alias": "stg_payments_v2",
                                "checksum": {"name": "sha256", "checksum": ""},
                                "columns": {
                                    "col_1": {
                                        "index": 1,
                                        "name": "col_1",
                                        "type": "INTEGER",
                                    },
                                },
                                "config": {"meta": None},
                                "description": "This is a description",
                                "fqn": [
                                    "dbt_bouncer_test_project",
                                    "stg_payments",
                                    "v2",
                                ],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments_v2.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments_v2.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments.v2",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments_v2.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments.v2",
                    },
                ),
            ],
            output_file=tmp_path / "output.json",
            output_format="json",
            output_only_failures=False,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    with Path.open(tmp_path / "output.json", "r") as f:
        output = json.load(f)

    check_run_ids = [x["check_run_id"] for x in output]
    assert "check_model_description_populated:0:stg_payments_v1" in check_run_ids
    assert "check_model_description_populated:0:stg_payments_v2" in check_run_ids

    assert results[0] == 0
    assert (tmp_path / "output.json").exists()


@pytest.mark.parametrize(
    ("output_only_failures", "num_checks"),
    [
        (False, 2),
        (True, 1),
    ],
)
def test_runner_output_only_failures(output_only_failures, num_checks, tmp_path):
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (
            DbtBouncerConfAllCategories as DbtBouncerConf,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest

        runner(
            bouncer_config=DbtBouncerConf(
                **{
                    "manifest_checks": [
                        {
                            "exclude": None,
                            "include": None,
                            "index": 0,
                            "name": "check_model_description_populated",
                        },
                    ]
                }
            ),
            catalog_nodes=[],
            catalog_sources=[],
            check_categories=["manifest_checks"],
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
                },
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
                                "config": {"meta": None},
                                "description": "",
                                "fqn": ["dbt_bouncer_test_project", "stg_payments"],
                                "name": "stg_payments",
                                "original_file_path": "models/staging/stg_payments.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_payments.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
                DbtBouncerModel(
                    **{
                        "model": Nodes4(
                            **{
                                "access": "public",
                                "alias": "stg_orders",
                                "checksum": {"name": "sha256", "checksum": ""},
                                "columns": {
                                    "col_1": {
                                        "index": 1,
                                        "name": "col_1",
                                        "type": "INTEGER",
                                    },
                                },
                                "config": {"meta": None},
                                "description": "This is a populated description",
                                "fqn": ["dbt_bouncer_test_project", "stg_orders"],
                                "name": "stg_orders",
                                "original_file_path": "models/staging/stg_orders.sql",
                                "package_name": "dbt_bouncer_test_project",
                                "path": "staging/stg_orders.sql",
                                "resource_type": "model",
                                "schema": "main",
                                "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                            },
                        ),
                        "original_file_path": "models/staging/stg_orders.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                    },
                ),
            ],
            output_file=Path(tmp_path / "output.json"),
            output_format="json",
            output_only_failures=output_only_failures,
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            show_all_failures=False,
            sources=[],
            tests=[],
            unit_tests=[],
        )

    with Path.open(tmp_path / "output.json", "r") as f:
        output = json.load(f)

    assert (tmp_path / "output.json").exists()
    assert len(output) == num_checks
