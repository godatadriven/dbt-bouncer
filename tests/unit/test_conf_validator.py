from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from dbt_bouncer.conf_validator import validate_conf

invalid_confs = [
    (
        f,
        pytest.raises(ValidationError),
    )
    for f in Path("./tests/unit/config_files/invalid").glob("*.yml")
]


@pytest.mark.parametrize(
    ("f", "expectation"),
    invalid_confs,
    ids=[f.stem for f, _ in invalid_confs],
)
def test_validate_conf_invalid(f, expectation):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    with expectation:
        validate_conf(config_file_contents=conf)


valid_confs = [
    (
        f,
        does_not_raise(),
    )
    for f in Path("./tests/unit/config_files/valid").glob("*.yml")
]


@pytest.mark.parametrize(
    ("f", "expectation"),
    valid_confs,
    ids=[f.stem for f, _ in valid_confs],
)
def test_validate_conf_valid(f, expectation):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    with expectation:
        validate_conf(config_file_contents=conf)