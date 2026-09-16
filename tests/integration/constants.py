"""Paths and shared config snippets used across the integration suite.

Kept out of `conftest.py` so test modules can import them without triggering a
second import of the conftest that pytest has already loaded as a plugin.
"""

from pathlib import Path

# Resolved at import time, before any test chdirs into a tmp_path.
DBT_PROJECT_TARGET = Path("dbt_project/target").resolve()
FIXTURES_DIR = Path("tests/fixtures").resolve()
DBT_112_TARGET = FIXTURES_DIR / "dbt_112" / "target"
EXAMPLE_CONFIG = Path("dbt-bouncer-example.yml").resolve()

# Every supported artifact format, oldest to newest. The low-numbered ones are
# frozen fixtures that are no longer rebuilt, so they guard against a parser
# change silently breaking older manifests.
ARTIFACT_DIRS = sorted(p for p in FIXTURES_DIR.iterdir() if p.is_dir())
ARTIFACT_IDS = [p.name for p in ARTIFACT_DIRS]

# A check that fails on every model: no model name starts with "zzz_".
FAILING_CHECK = [{"name": "check_model_names", "model_name_pattern": "^zzz_"}]
