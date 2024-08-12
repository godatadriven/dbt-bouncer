from pathlib import Path

from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.runner import runner

runner(
    bouncer_config={
        "check_model_description_populated": [
            {"catalog_checks": [], "run_results_checks": [], "index": 0}
        ]
    },
    catalog_nodes=[],
    catalog_sources=[],
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
        {
            "description": "This is a description",
            "path": "staging/stg_payments.sql",
            "unique_id": "model.dbt_bouncer_test_project.stg_payments",
        }
    ],
    output_file=None,
    run_results=[],
    send_pr_comment=False,
    sources=[],
    tests=[],
    checks_dir=Path("./src/dbt_bouncer/checks"),
)
