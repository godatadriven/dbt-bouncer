import json
import warnings
from pathlib import Path

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.parsers import DbtBouncerManifest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "iterate_over_catalog_nodes: Tests that should run once per node in `catalog.json`",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_catalog_sources: Tests that should run once per source in `catalog.json`",
    )
    config.addinivalue_line(
        "markers", "iterate_over_exposures: Tests that should run once per exposure"
    )
    config.addinivalue_line("markers", "iterate_over_macros: Tests that should run once per macro")
    config.addinivalue_line(
        "markers",
        "iterate_over_models: Tests that should run once per model",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_run_results: Tests that should run once per run result in `run_results.json`",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_sources: Tests that should run once per source",
    )
    config.addinivalue_line(
        "filterwarnings",
        "ignore::UserWarning",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_unit_tests: Tests that should run once per unit test",
    )


@pytest.fixture(scope="session")
def manifest_obj():
    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))
    return DbtBouncerManifest(**{"manifest": manifest_obj})
