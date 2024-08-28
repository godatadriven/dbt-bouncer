import json
import warnings
from pathlib import Path

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.parsers import DbtBouncerManifest


@pytest.fixture(scope="session")
def manifest_obj():
    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))
    return DbtBouncerManifest(**{"manifest": manifest_obj})
