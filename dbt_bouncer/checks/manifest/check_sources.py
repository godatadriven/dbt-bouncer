import re

import pytest

from dbt_bouncer.utils import flatten, get_check_inputs


@pytest.mark.iterate_over_sources
def check_source_has_meta_keys(request, check_config=None, source=None) -> None:
    """
    The `meta` config for sources must have the specified keys.
    """

    input_vars = get_check_inputs(
        check_config=check_config,
        request=request,
        source=source,
    )
    check_config = input_vars["check_config"]
    source = input_vars["source"]

    keys_in_meta = list(flatten(source.get("meta")).keys())

    # Get required keys and convert to a list
    specified_keys = check_config["keys"]
    required_keys = [
        re.sub(r"(\>{1}\d{1,10})", "", f"{k}>{v}") for k, v in flatten(specified_keys).items()
    ]

    missing_keys = [x for x in required_keys if x not in keys_in_meta]
    assert (
        missing_keys == []
    ), f"{source['unique_id']} is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
