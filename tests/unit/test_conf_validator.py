from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from src.dbt_bouncer.conf_validator import validate_conf

invalid_confs = []
for f in Path("./tests/unit/confs/invalid").glob("*.yml"):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    invalid_confs.append(
        (
            conf,
            pytest.raises(ValidationError),
        )
    )


@pytest.mark.parametrize("conf, expectation", invalid_confs)
def test_validate_conf_invalid(conf, expectation):
    with expectation:
        validate_conf(conf=conf)


valid_confs = []
for f in Path("./tests/unit/confs/valid").glob("*.yml"):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    valid_confs.append(
        (
            conf,
            does_not_raise(),
        )
    )


@pytest.mark.parametrize("conf, expectation", valid_confs)
def test_validate_conf_valid(conf, expectation):
    with expectation:
        validate_conf(conf=conf)
