import logging
from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import (
    compile_pattern,
    get_clean_model_name,
    get_package_version_number,
    is_description_populated,
)


@check
def check_seed_column_names(seed, *, seed_column_name_pattern: str):
    """Seed columns must have names that match the supplied regex.

    Parameters:
        seed_column_name_pattern (str): Regexp the column name must match.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_column_names
              seed_column_name_pattern: ^[a-z_]+$  # Lowercase with underscores only
        ```

    """
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


@check
def check_seed_columns_have_types(seed):
    """Columns defined for seeds must have a `data_type` declared.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_columns_have_types
        ```

    """
    columns = seed.columns or {}
    untyped_columns = [
        col_name for col_name, col in columns.items() if not col.data_type
    ]
    if untyped_columns:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` has columns without a declared `data_type`: {untyped_columns}"
        )


@check
def check_seed_description_populated(
    seed, *, min_description_length: Annotated[int, Field(gt=0)] | None = None
):
    """Seeds must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    if not is_description_populated(
        seed.description or "", min_description_length or 4
    ):
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` does not have a populated description."
        )


@check
def check_seed_has_unit_tests(
    seed, ctx, *, min_number_of_unit_tests: Annotated[int, Field(gt=0)] = 1
):
    """Seeds must have more than the specified number of unit tests.

    Parameters:
        min_number_of_unit_tests (int | None): The minimum number of unit tests that a seed must have.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        seed (SeedNode): The SeedNode object to check.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_has_unit_tests
              include: ^seeds/core
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_has_unit_tests
              min_number_of_unit_tests: 2
        ```

    """
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


@check
def check_seed_names(seed, *, seed_name_pattern: str):
    """Seed must have a name that matches the supplied regex.

    Parameters:
        seed_name_pattern (str): Regexp the seed name must match.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_names
              include: ^seeds
              seed_name_pattern: ^raw_
        ```

    """
    compiled_pattern = compile_pattern(seed_name_pattern.strip())
    if compiled_pattern.match(str(seed.name)) is None:
        fail(
            f"`{get_clean_model_name(seed.unique_id)}` does not match the supplied regex `{seed_name_pattern.strip()}`."
        )
