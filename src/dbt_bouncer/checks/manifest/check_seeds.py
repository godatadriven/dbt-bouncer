import logging

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import (
    compile_pattern,
    get_clean_model_name,
    get_package_version_number,
    is_description_populated,
)


@check("check_seed_column_names", iterate_over="seed")
def check_seed_column_names(seed, *, seed_column_name_pattern: str):
    """Seed columns must have names that match the supplied regex."""
    compiled_pattern = compile_pattern(seed_column_name_pattern.strip())
    seed_columns = seed.columns or {}
    non_complying_columns = [
        col_name
        for col_name in seed_columns
        if compiled_pattern.match(str(col_name)) is None
    ]

    if non_complying_columns:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` has columns that do not match the supplied regex `{seed_column_name_pattern.strip()}`: {non_complying_columns}"
        )


@check("check_seed_columns_have_types", iterate_over="seed")
def check_seed_columns_have_types(seed):
    """Columns defined for seeds must have a `data_type` declared."""
    columns = seed.columns or {}
    untyped_columns = [
        col_name for col_name, col in columns.items() if not col.data_type
    ]
    if untyped_columns:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` has columns without a declared `data_type`: {untyped_columns}"
        )


@check("check_seed_description_populated", iterate_over="seed")
def check_seed_description_populated(
    seed, *, min_description_length: int | None = None
):
    """Seeds must have a populated description."""
    if not is_description_populated(
        seed.description or "", min_description_length or 4
    ):
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` does not have a populated description."
        )


@check("check_seed_has_unit_tests", iterate_over="seed")
def check_seed_has_unit_tests(seed, ctx, *, min_number_of_unit_tests: int = 1):
    """Seeds must have more than the specified number of unit tests."""
    manifest_obj = ctx.manifest_obj
    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        num_unit_tests = len(
            [
                t.unique_id
                for t in ctx.unit_tests
                if t.depends_on
                and t.depends_on.nodes
                and t.depends_on.nodes[0] == seed.unique_id
            ]
        )
        if num_unit_tests < min_number_of_unit_tests:
            fail(
                f"`{get_clean_model_name(seed.unique_id)}` has {num_unit_tests} unit tests, this is less than the minimum of {min_number_of_unit_tests}."
            )
    else:
        logging.warning(
            "This unit test check is only supported for dbt 1.8.0 and above."
        )


@check("check_seed_names", iterate_over="seed")
def check_seed_names(seed, *, seed_name_pattern: str):
    """Seed must have a name that matches the supplied regex."""
    compiled_pattern = compile_pattern(seed_name_pattern.strip())
    if compiled_pattern.match(str(seed.name)) is None:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` does not match the supplied regex `{seed_name_pattern.strip()}`."
        )
