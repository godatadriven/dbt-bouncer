from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest
from pydantic import ValidationError

from dbt_bouncer.config_validator import validate_config_file

invalid_configs = []
for f in Path("./tests/config_files/invalid").glob("*.yml"):
    invalid_configs.append(
        (
            f,
            pytest.raises(ValidationError),
        )
    )


@pytest.mark.parametrize("file, expectation", invalid_configs)
def test_validate_config_file_invalid(file, expectation):
    with expectation:
        validate_config_file(file=file)


valid_configs = []
for f in Path("./tests/config_files/valid").glob("*.yml"):
    valid_configs.append(
        (
            f,
            does_not_raise(),
        )
    )


@pytest.mark.parametrize("file, expectation", valid_configs)
def test_validate_config_file_valid(file, expectation):
    with expectation:
        validate_config_file(file=file)
