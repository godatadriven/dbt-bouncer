import re

import pytest

from dbt_bouncer.logger import logger
from dbt_bouncer.utils import flatten


@pytest.mark.iterate_over_sources
def check_source_has_meta_keys(request, check_config=None, source=None) -> None:
    """
    The `meta` config for sources must have the specified keys.
    """

    check_config = request.node.check_config if check_config is None else check_config
    source = request.node.source if source is None else source

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
