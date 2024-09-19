import json
import warnings
from pathlib import Path

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
        CatalogTable,  # noqa: F401
    )
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (  # noqa: F401
        Exposures,
        Macros,
        UnitTests,
    )


from unittest.mock import MagicMock

import click
from click.globals import push_context

from dbt_bouncer.checks.common import NestedDict  # noqa: F401
from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.main import cli
from dbt_bouncer.parsers import (  # noqa: F401
    DbtBouncerExposureBase,
    DbtBouncerManifest,
    DbtBouncerModel,
    DbtBouncerModelBase,
    DbtBouncerNodes4,
    DbtBouncerRunResultBase,
    DbtBouncerSemanticModelBase,
    DbtBouncerSourceBase,
    DbtBouncerTestBase,
)
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
        from dbt_bouncer.config_file_parser import DbtBouncerConf

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
                CatalogTable,  # noqa: F401
            )
            from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (  # noqa: F401
                Exposures,
                Macros,
                UnitTests,
            )

        from dbt_bouncer.checks.common import NestedDict  # noqa: F401
        from dbt_bouncer.parsers import (  # noqa: F401
            DbtBouncerExposureBase,
            DbtBouncerManifest,
            DbtBouncerModel,
            DbtBouncerModelBase,
            DbtBouncerNodes4,
            DbtBouncerRunResultBase,
            DbtBouncerSemanticModelBase,
            DbtBouncerSourceBase,
            DbtBouncerTestBase,
        )

        DbtBouncerConf.model_rebuild()

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
                        "model": DbtBouncerNodes4(
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
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=tmp_path / "coverage.json",
            run_results=[],
            semantic_models=[],
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
        from dbt_bouncer.config_file_parser import DbtBouncerConf

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
                CatalogTable,  # noqa: F401
            )
            from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (  # noqa: F401
                Exposures,
                Macros,
                UnitTests,
            )

        from dbt_bouncer.checks.common import NestedDict  # noqa: F401
        from dbt_bouncer.parsers import (  # noqa: F401
            DbtBouncerExposureBase,
            DbtBouncerManifest,
            DbtBouncerModel,
            DbtBouncerModelBase,
            DbtBouncerRunResultBase,
            DbtBouncerSemanticModelBase,
            DbtBouncerSourceBase,
            DbtBouncerTestBase,
        )

        DbtBouncerConf.model_rebuild()

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
                        "model": DbtBouncerNodes4(
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
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=None,
            run_results=[],
            semantic_models=[],
            sources=[],
            tests=[],
            unit_tests=[],
        )

    assert results[0] == 1


def test_runner_success():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        from dbt_bouncer.config_file_parser import DbtBouncerConf

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parser import parse_manifest
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
                CatalogTable,  # noqa: F401
            )
            from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (  # noqa: F401
                Exposures,
                Macros,
                UnitTests,
            )

        from dbt_bouncer.checks.common import NestedDict  # noqa: F401
        from dbt_bouncer.parsers import (  # noqa: F401
            DbtBouncerExposureBase,
            DbtBouncerManifest,
            DbtBouncerModel,
            DbtBouncerModelBase,
            DbtBouncerNodes4,
            DbtBouncerRunResultBase,
            DbtBouncerSemanticModelBase,
            DbtBouncerSourceBase,
            DbtBouncerTestBase,
        )

        DbtBouncerConf.model_rebuild()

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
                        "model": DbtBouncerNodes4(
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
                            },
                        ),
                        "original_file_path": "models/staging/stg_payments.sql",
                        "unique_id": "model.dbt_bouncer_test_project.stg_payments",
                    },
                ),
            ],
            output_file=None,
            run_results=[],
            semantic_models=[],
            sources=[],
            tests=[],
            unit_tests=[],
        )

    assert results[0] == 0


def test_runner_windows(caplog, tmp_path):
    configure_console_logging(verbosity=0)
    ctx = MagicMock(obj={"verbosity": 3})
    push_context(ctx)

    from dbt_bouncer.config_file_parser import DbtBouncerConf

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parser import parse_manifest
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            CatalogTable,  # noqa: F401
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (  # noqa: F401
            Exposures,
            Macros,
            UnitTests,
        )

    DbtBouncerConf.model_rebuild()
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
                    "model": DbtBouncerNodes4(
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
        run_results=[],
        semantic_models=[],
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
