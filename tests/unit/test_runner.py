import json
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import typer
from typer.main import get_command

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Metadata,
    Nodes4,
)
from dbt_bouncer.artifact_parsers.parsers_catalog import (
    DbtBouncerCatalogNode,
)
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerManifest,
    DbtBouncerModel,
)
from dbt_bouncer.context import BouncerContext
from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.main import app
from dbt_bouncer.runner import _should_run_check, runner


def test_runner_coverage(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                    "output_file": tmp_path / "coverage.json",
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
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
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                    "output_file": None,
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
        )

    assert results[0] == 1


def test_runner_skip(tmp_path):
    configure_console_logging(verbosity=0)
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_orders",
                                        ],
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
                    "output_file": tmp_path / "coverage.json",
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
        )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    assert results[0] == 0
    assert len(coverage) == 1


def test_runner_success():
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                    "output_file": None,
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
        )

    assert results[0] == 0


def test_runner_windows(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    from dbt_bouncer.config_file_parser import (  # noqa: N812
        create_bouncer_conf_class as DbtBouncerConf,
    )

    DbtBouncerConf = DbtBouncerConf()  # noqa: N806

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

    results = runner(
        ctx=BouncerContext(
            **{
                "bouncer_config": DbtBouncerConf(
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
                "catalog_nodes": [],
                "catalog_sources": [],
                "check_categories": ["manifest_checks"],
                "create_pr_comment_file": False,
                "dry_run": False,
                "exposures": [],
                "macros": [],
                "manifest_obj": DbtBouncerManifest(
                    manifest=parse_manifest(
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
                    )
                ),
                "models": [
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
                                    "fqn": [
                                        "dbt_bouncer_test_project",
                                        "stg_payments",
                                    ],
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
                "output_file": tmp_path / "coverage.json",
                "output_format": "json",
                "output_only_failures": False,
                "run_results": [],
                "seeds": [],
                "semantic_models": [],
                "snapshots": [],
                "show_all_failures": False,
                "sources": [],
                "tests": [],
                "unit_tests": [],
            }
        )
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
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                    "output_file": tmp_path / "output.json",
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
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
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest

        runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
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
                    "catalog_nodes": [],
                    "catalog_sources": [],
                    "check_categories": ["manifest_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=parse_manifest(
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
                        )
                    ),
                    "models": [
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_orders",
                                        ],
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
                    "output_file": Path(tmp_path / "output.json"),
                    "output_format": "json",
                    "output_only_failures": output_only_failures,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
        )

    with Path.open(tmp_path / "output.json", "r") as f:
        output = json.load(f)

    assert (tmp_path / "output.json").exists()
    assert len(output) == num_checks


def test_runner_skip_catalog_check(tmp_path):
    """Test that skip_checks in model meta config works for catalog_checks.

    Scenario:
    - stg_payments: normal model with col_1 documented -> check runs and passes
    - stg_orders: model with skip_checks for check_columns_are_all_documented
      and undocumented col_2 in catalog -> check is skipped
    """
    configure_console_logging(verbosity=0)
    ctx = typer.Context(
        get_command(app),
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import (  # noqa: N812
            create_bouncer_conf_class as DbtBouncerConf,
        )

        DbtBouncerConf = DbtBouncerConf()  # noqa: N806

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
                Nodes as CatalogNodes,
            )

            from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
                ManifestLatest,
            )

        results = runner(
            ctx=BouncerContext(
                **{
                    "bouncer_config": DbtBouncerConf(
                        **{
                            "catalog_checks": [
                                {
                                    "exclude": None,
                                    "include": None,
                                    "index": 0,
                                    "name": "check_columns_are_all_documented",
                                },
                            ],
                        }
                    ),
                    "catalog_nodes": [
                        DbtBouncerCatalogNode(
                            catalog_node=CatalogNodes(
                                **{
                                    "columns": {
                                        "col_1": {
                                            "index": 1,
                                            "name": "col_1",
                                            "type": "INTEGER",
                                        },
                                    },
                                    "metadata": {
                                        "name": "stg_payments",
                                        "schema": "main",
                                        "type": "VIEW",
                                    },
                                    "stats": {},
                                    "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                                }
                            ),
                            original_file_path="models/staging/stg_payments.sql",
                            unique_id="model.dbt_bouncer_test_project.stg_payments",
                        ),
                        DbtBouncerCatalogNode(
                            catalog_node=CatalogNodes(
                                **{
                                    "columns": {
                                        "col_1": {
                                            "index": 1,
                                            "name": "col_1",
                                            "type": "INTEGER",
                                        },
                                        "col_2": {
                                            "index": 2,
                                            "name": "col_2",
                                            "type": "VARCHAR",
                                        },
                                    },
                                    "metadata": {
                                        "name": "stg_orders",
                                        "schema": "main",
                                        "type": "VIEW",
                                    },
                                    "stats": {},
                                    "unique_id": "model.dbt_bouncer_test_project.stg_orders",
                                }
                            ),
                            original_file_path="models/staging/stg_orders.sql",
                            unique_id="model.dbt_bouncer_test_project.stg_orders",
                        ),
                    ],
                    "catalog_sources": [],
                    "check_categories": ["catalog_checks"],
                    "create_pr_comment_file": False,
                    "dry_run": False,
                    "exposures": [],
                    "macros": [],
                    "manifest_obj": DbtBouncerManifest(
                        manifest=ManifestLatest(
                            **{
                                "metadata": Metadata(
                                    dbt_schema_version="https://schemas.getdbt.com/dbt/manifest/v12.json",
                                    project_name="dbt_bouncer_test_project",
                                    adapter_type="postgres",
                                ),
                                "child_map": {},
                                "disabled": {},
                                "docs": {},
                                "exposures": {},
                                "group_map": {},
                                "groups": {},
                                "macros": {},
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
                    ),
                    "models": [
                        DbtBouncerModel(
                            **{
                                "model": Nodes4(
                                    **{
                                        "access": "public",
                                        "alias": "stg_payments",
                                        "checksum": {"name": "sha256", "checksum": ""},
                                        "columns": {
                                            "col_1": {
                                                "description": "Column 1 description",
                                                "index": 1,
                                                "name": "col_1",
                                                "type": "INTEGER",
                                            },
                                        },
                                        "config": {"meta": None},
                                        "description": "Payments staging model",
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_payments",
                                        ],
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
                                                "description": "Column 1 description",
                                                "index": 1,
                                                "name": "col_1",
                                                "type": "INTEGER",
                                            },
                                        },
                                        "config": {
                                            "meta": {
                                                "dbt-bouncer": {
                                                    "skip_checks": [
                                                        "check_columns_are_all_documented"
                                                    ]
                                                }
                                            }
                                        },
                                        "description": "Orders staging model",
                                        "fqn": [
                                            "dbt_bouncer_test_project",
                                            "stg_orders",
                                        ],
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
                    "output_file": tmp_path / "coverage.json",
                    "output_format": "json",
                    "output_only_failures": False,
                    "run_results": [],
                    "seeds": [],
                    "semantic_models": [],
                    "snapshots": [],
                    "show_all_failures": False,
                    "sources": [],
                    "tests": [],
                    "unit_tests": [],
                }
            )
        )

    with Path.open(tmp_path / "coverage.json", "r") as f:
        coverage = json.load(f)

    # stg_orders check is skipped due to skip_checks, so no failures
    assert results[0] == 0
    # Only stg_payments check was executed (stg_orders was skipped)
    assert len(coverage) == 1


def test_should_run_check_include_pattern_no_match():
    """Test that check doesn't run when include pattern doesn't match."""
    from dbt_bouncer.utils import resource_in_path

    check = MagicMock()
    check.include = "^models/marts"
    check.exclude = None

    resource = MagicMock()
    resource.original_file_path = "models/staging/stg_orders.sql"

    assert resource_in_path(check, resource) is False


def test_should_run_check_exclude_pattern_matches():
    """Test that check doesn't run when exclude pattern matches."""
    from dbt_bouncer.utils import resource_in_path

    check = MagicMock()
    check.include = None
    check.exclude = ".*_tmp.*"

    resource = MagicMock()
    resource.original_file_path = "models/staging/stg_orders_tmp.sql"

    assert resource_in_path(check, resource) is False


def test_should_run_check_materialization_mismatch():
    """Test that check doesn't run when materialization doesn't match."""
    check = MagicMock()
    check.include = None
    check.exclude = None
    check.name = "test_check"
    check.materialization = "table"

    resource = MagicMock()
    resource.model.config.materialized = "view"
    resource.original_file_path = "models/staging/stg_orders.sql"

    assert _should_run_check(check, resource, frozenset({"model"}), []) is False


def test_should_run_check_materialization_matches():
    """Test that check runs when materialization matches."""
    check = MagicMock()
    check.include = None
    check.exclude = None
    check.name = "test_check"
    check.materialization = "view"

    resource = MagicMock()
    resource.model.config.materialized = "view"
    resource.original_file_path = "models/staging/stg_orders.sql"

    assert _should_run_check(check, resource, frozenset({"model"}), []) is True


def test_should_run_check_non_model_resource():
    """Test that materialization check is skipped for non-model resources."""
    check = MagicMock()
    check.include = None
    check.exclude = None
    check.name = "test_check"
    check.materialization = "table"

    resource = MagicMock()
    resource.original_file_path = "macros/my_macro.sql"

    # Should still run for non-model resources, materialization check is skipped
    assert _should_run_check(check, resource, frozenset({"macro"}), []) is True
