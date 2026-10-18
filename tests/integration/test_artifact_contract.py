"""Integration and contract tests for dbt artifact compatibility and guards.

Covers:
- I1: Artifact contract matrix across all supported dbt lines and parse-only Fusion.
- I2: Vacuous-pass guards asserting fixtures are populated and empty catalogs exit non-zero.
"""

from pathlib import PurePath

import orjson
import pytest

from dbt_bouncer.cli.run.utils import run_bouncer
from dbt_bouncer.enums import CheckOutcome, ExitCode, OutputFormat
from dbt_bouncer.exceptions import DbtBouncerArtifactError
from dbt_bouncer.main import app
from tests.integration.constants import (
    ALL_ARTIFACT_DIRS,
    ALL_ARTIFACT_IDS,
    ARTIFACT_DIRS,
    ARTIFACT_IDS,
    DBT_112_TARGET,
    DBT_PROJECT_TARGET,
)

_EXPECTED_CATALOG_SCHEMA = "https://schemas.getdbt.com/dbt/catalog/v1.json"
_EXPECTED_MANIFEST_SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
_EXPECTED_RUN_RESULTS_SCHEMA = "https://schemas.getdbt.com/dbt/run-results/v6.json"

_PASSING_MODEL_CHECK = {
    "name": "check_model_names",
    "include": r"models/staging/crm/stg_orders\.sql",
    "model_name_pattern": "^stg_",
}

_PASSING_COLUMN_CHECK = {
    "name": "check_column_names",
    "include": r"models/staging/crm/stg_orders\.sql",
    "column_name_pattern": "[a-z_]+",
}

# All committed target directories that hold full fixtures (catalog + run_results).
_FULL_FIXTURE_TARGETS = [d / "target" for d in ARTIFACT_DIRS] + [DBT_PROJECT_TARGET]
_FULL_FIXTURE_IDS = [*ARTIFACT_IDS, "dbt_project"]


# -----------------------------------------------------------------------------
# I1: Artifact contract matrix
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("artifact_dir", ALL_ARTIFACT_DIRS, ids=ALL_ARTIFACT_IDS)
def test_artifact_schema_version_pinned(artifact_dir):
    """Pin metadata.dbt_schema_version for all supported lines so schema bumps turn red.

    Covers manifest schema v12, catalog schema v1, and run results schema v6
    per #1113 and the dbt-bouncer FAQ.
    """
    target = artifact_dir / "target"

    manifest = orjson.loads((target / "manifest.json").read_bytes())
    assert manifest["metadata"]["dbt_schema_version"] == _EXPECTED_MANIFEST_SCHEMA

    catalog_path = target / "catalog.json"
    if catalog_path.is_file():
        catalog = orjson.loads(catalog_path.read_bytes())
        assert catalog["metadata"]["dbt_schema_version"] == _EXPECTED_CATALOG_SCHEMA

    run_results_path = target / "run_results.json"
    if run_results_path.is_file():
        run_results = orjson.loads(run_results_path.read_bytes())
        assert (
            run_results["metadata"]["dbt_schema_version"]
            == _EXPECTED_RUN_RESULTS_SCHEMA
        )


@pytest.mark.parametrize("artifact_dir", ALL_ARTIFACT_DIRS, ids=ALL_ARTIFACT_IDS)
def test_artifact_contract_matrix_run_bouncer(
    artifact_dir, example_config, tmp_path, write_config
):
    """Run `run_bouncer` with `dbt-bouncer-example.yml` against each fixture.

    Asserts that across all supported lines (1.10, 1.11, 1.12, 2.0 and parse-only Fusion),
    dbt-bouncer ingests and checks the artifacts without artifact or internal errors.
    """
    target = artifact_dir / "target"
    example_config["dbt_artifacts_dir"] = str(target.resolve())

    # Only fixtures with an information schema can run info_schema_checks.
    if not (target / "info_schema" / "v1").is_dir():
        example_config.pop("info_schema_checks", None)

    # Parse-only fixtures emit only manifest.json; catalog and run_results are absent.
    if not (target / "catalog.json").is_file():
        example_config.pop("catalog_checks", None)
    if not (target / "run_results.json").is_file():
        example_config.pop("run_results_checks", None)

    config_file = write_config(example_config, name=f"{artifact_dir.name}.yml")
    results_file = tmp_path / f"{artifact_dir.name}_results.json"

    exit_code = run_bouncer(
        config_file=config_file,
        output_file=results_file,
        output_format=OutputFormat.JSON,
    )

    assert exit_code != ExitCode.ARTIFACT_ERROR
    assert results_file.exists()

    results = orjson.loads(results_file.read_bytes())
    assert len(results) > 0

    internal_errors = [
        r for r in results if r.get("outcome") == CheckOutcome.INTERNAL_ERROR.value
    ]
    assert internal_errors == [], f"Internal errors encountered: {internal_errors}"


# -----------------------------------------------------------------------------
# I2: Vacuous-pass guard
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("target_dir", _FULL_FIXTURE_TARGETS, ids=_FULL_FIXTURE_IDS)
def test_fixture_sanity_catalog_and_run_results_populated(target_dir):
    """Fixture sanity test: catalog.json has >0 nodes and run_results.json has >0 results."""
    catalog = orjson.loads((target_dir / "catalog.json").read_bytes())
    assert len(catalog.get("nodes", {})) > 0, f"{target_dir}/catalog.json has 0 nodes"

    run_results = orjson.loads((target_dir / "run_results.json").read_bytes())
    assert len(run_results.get("results", [])) > 0, (
        f"{target_dir}/run_results.json has 0 results"
    )


def test_empty_catalog_with_catalog_checks_exits_artifact_error(
    caplog, cli_runner, tmp_path, write_config
):
    """Running catalog checks against an empty catalog exits non-zero (ARTIFACT_ERROR)."""
    artifacts_dir = tmp_path / "target"
    artifacts_dir.mkdir()
    (artifacts_dir / "manifest.json").write_bytes(
        (DBT_112_TARGET / "manifest.json").read_bytes()
    )
    (artifacts_dir / "catalog.json").write_bytes(
        orjson.dumps({"metadata": {}, "nodes": {}, "sources": {}})
    )

    config_file = write_config(
        {
            "catalog_checks": [_PASSING_COLUMN_CHECK],
            "dbt_artifacts_dir": str(artifacts_dir),
        }
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    assert result.exit_code == ExitCode.ARTIFACT_ERROR, result.output
    assert "contains 0 nodes and 0 sources" in caplog.text


def test_empty_catalog_with_mixed_checks_guards_against_vacuous_pass(
    caplog, cli_runner, tmp_path, write_config
):
    """A run with passing manifest checks and configured catalog checks cannot pass vacuously.

    Even when all manifest checks pass, an empty catalog (0 nodes/sources) must fail the build
    rather than exiting 0 and concealing skipped catalog validation.
    """
    artifacts_dir = tmp_path / "target"
    artifacts_dir.mkdir()
    (artifacts_dir / "manifest.json").write_bytes(
        (DBT_112_TARGET / "manifest.json").read_bytes()
    )
    (artifacts_dir / "catalog.json").write_bytes(
        orjson.dumps({"metadata": {}, "nodes": {}, "sources": {}})
    )

    config_file = write_config(
        {
            "catalog_checks": [_PASSING_COLUMN_CHECK],
            "dbt_artifacts_dir": str(artifacts_dir),
            "manifest_checks": [_PASSING_MODEL_CHECK],
        }
    )

    result = cli_runner.invoke(
        app, ["run", "--config-file", PurePath(config_file).as_posix()]
    )

    # Must fail non-zero (ARTIFACT_ERROR), not pass with 0.
    assert result.exit_code == ExitCode.ARTIFACT_ERROR, result.output
    assert "contains 0 nodes and 0 sources" in caplog.text


def test_empty_catalog_programmatic_run_bouncer_raises_artifact_error(
    tmp_path, write_config
):
    """`run_bouncer` raises DbtBouncerArtifactError when catalog is empty and checks are configured."""
    artifacts_dir = tmp_path / "target"
    artifacts_dir.mkdir()
    (artifacts_dir / "manifest.json").write_bytes(
        (DBT_112_TARGET / "manifest.json").read_bytes()
    )
    (artifacts_dir / "catalog.json").write_bytes(
        orjson.dumps({"metadata": {}, "nodes": {}, "sources": {}})
    )

    config_file = write_config(
        {
            "catalog_checks": [_PASSING_COLUMN_CHECK],
            "dbt_artifacts_dir": str(artifacts_dir),
        }
    )

    with pytest.raises(DbtBouncerArtifactError, match="contains 0 nodes and 0 sources"):
        run_bouncer(config_file=config_file)
