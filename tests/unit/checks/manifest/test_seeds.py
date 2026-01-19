import warnings
from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes as SeedsLatest,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

    from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
        DbtBouncerManifest,
        DbtBouncerSeed,
        DbtBouncerSeedBase,
    )


from dbt_bouncer.checks.manifest.check_seeds import CheckSeedNames

CheckSeedNames.model_rebuild()


@pytest.mark.parametrize(
    ("seed", "seed_name_pattern", "expectation"),
    [
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "package_name": "package_name",
                    "path": "raw_customers.sql",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            "^raw_",
            does_not_raise(),
        ),
        pytest.param(
            SeedsLatest(
                **{
                    "alias": "raw_customers",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {},
                    "fqn": ["package_name", "raw_customers"],
                    "name": "raw_customers",
                    "original_file_path": "seeds/raw_customers.sql",
                    "package_name": "package_name",
                    "path": "raw_customers.sql",
                    "resource_type": "seed",
                    "schema": "main",
                    "unique_id": "seed.package_name.raw_customers",
                }
            ),
            "^seed_",
            pytest.raises(DbtBouncerFailedCheckError),
        ),
    ],
)
def test_check_seed_names(seed, seed_name_pattern, expectation):
    with expectation:
        CheckSeedNames(
            seed=seed, seed_name_pattern=seed_name_pattern, name="check_seed_names"
        ).execute()
