from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name


@check
def check_seed_columns_are_all_documented(catalog_node, ctx):
    """All columns in a seed CSV file should be included in the seed's properties file."""
    if catalog_node.unique_id is not None and catalog_node.unique_id.startswith(
        "seed."
    ):
        seed = next(s for s in ctx.seeds if s.unique_id == catalog_node.unique_id)

        seed_columns = seed.columns or {}
        undocumented_columns = [
            v.name
            for _, v in catalog_node.columns.items()
            if v.name not in seed_columns
        ]

        if undocumented_columns:
            fail(
                f"`{get_clean_model_name(seed.unique_id)}` has columns that are not included in the seed properties file: {undocumented_columns}"
            )
