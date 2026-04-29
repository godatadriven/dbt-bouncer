"""Checks related to model file locations, names, and directory structure."""

from pathlib import Path

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check
def check_model_directories(
    model, *, include: str, permitted_sub_directories: list[str]
):
    """Only specified sub-directories are permitted.

    !!! info "Rationale"

        A well-structured dbt project organises models into predictable directories (e.g. `staging`, `intermediate`, `marts`). Enforcing permitted sub-directories prevents ad-hoc folders from proliferating, making the project layout consistent and navigable for all contributors.

    Parameters:
        include (str): Regex pattern to the directory to check.
        permitted_sub_directories (list[str]): List of permitted sub-directories.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
        - name: check_model_directories
          include: models
          permitted_sub_directories:
            - intermediate
            - marts
            - staging
        ```
        ```yaml
        # Restrict sub-directories within `./models/staging`
        - name: check_model_directories
          include: ^models/staging
          permitted_sub_directories:
            - crm
            - payments
        ```

    """
    compiled_include = compile_pattern(include.strip().rstrip("/"))
    clean_path = (
        Path(model.original_file_path).as_posix() if model.original_file_path else ""
    )
    matched_path = compiled_include.match(clean_path)
    if matched_path is None:
        fail("matched_path is None")
    path_after_match = clean_path[matched_path.end() + 1 :]
    directory_to_check = Path(path_after_match).parts[0]

    if directory_to_check.replace(".sql", "") == model.name:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is not located in a valid sub-directory ({permitted_sub_directories})."
        )
    elif directory_to_check not in permitted_sub_directories:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is located in the `{directory_to_check}` sub-directory, this is not a valid sub-directory ({permitted_sub_directories})."
        )


@check
def check_model_file_name(model, *, file_name_pattern: str):
    r"""Models must have a file name that matches the supplied regex.

    !!! info "Rationale"

        Consistent file naming conventions (e.g. including a version suffix for mart models) make it easy to identify a model's purpose and layer at a glance. Enforcing naming patterns in CI prevents deviations that accumulate over time and make the project harder to navigate.

    Parameters:
        file_name_pattern (str): Regexp the file name must match. Please account for the `.sql` extension.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_file_name
              description: Marts must include the model version in their file name.
              include: ^models/marts
              file_name_pattern: .*(v[0-9])\.sql$
        ```

    """
    compiled = compile_pattern(file_name_pattern.strip())
    file_name = Path(
        Path(model.original_file_path).as_posix() if model.original_file_path else ""
    ).name
    if compiled.match(file_name) is None:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is in a file that does not match the supplied regex `{file_name_pattern.strip()}`."
        )


@check
def check_model_property_file_location(model):
    """Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    !!! info "Rationale"

        dbt's official guidance recommends a specific naming and placement convention for YAML property files (e.g. `_staging__models.yml`). Following this convention ensures that property files are easy to locate, clearly scoped, and consistent with the broader dbt community's expectations.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_property_file_location
        ```

    """
    if not (
        hasattr(model, "patch_path")
        and model.patch_path
        and (Path(model.patch_path or "").as_posix() if model.patch_path or "" else "")
        is not None
    ):
        fail(f"`{get_clean_model_name(model.unique_id)}` is not documented.")

    original_path = Path(
        Path(model.original_file_path).as_posix() if model.original_file_path else ""
    )
    relevant_parts = original_path.parts[1:-1]

    mapped_parts = []
    for part in relevant_parts:
        if part == "staging":
            mapped_parts.append("stg")
        elif part == "intermediate":
            mapped_parts.append("int")
        elif part == "marts":
            continue
        else:
            mapped_parts.append(part)

    expected_substr = "_".join(mapped_parts)
    properties_yml_name = Path(
        Path(model.patch_path or "").as_posix() if model.patch_path or "" else ""
    ).name

    if not properties_yml_name.startswith("_"):
        fail(
            f"The properties file for `{get_clean_model_name(model.unique_id)}` (`{properties_yml_name}`) does not start with an underscore."
        )
    if expected_substr not in properties_yml_name:
        fail(
            f"The properties file for `{get_clean_model_name(model.unique_id)}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
        )
    if not properties_yml_name.endswith("__models.yml"):
        fail(
            f"The properties file for `{get_clean_model_name(model.unique_id)}` (`{properties_yml_name}`) does not end with `__models.yml`."
        )


@check
def check_model_schema_name(model, *, schema_name_pattern: str):
    """Models must have a schema name that matches the supplied regex.

    !!! info "Rationale"

        Consistent schema naming (e.g. `stg_payments` for staging models or `intermediate` for intermediate ones) makes it clear where a model lives in the transformation pipeline without inspecting its SQL. This also prevents models from landing in unexpected schemas in production due to misconfigured `dbt_project.yml` settings.

    Note that most setups will use schema names in development that are prefixed, for example:
        * dbt_jdoe_stg_payments
        * mary_stg_payments

    Please account for this if you wish to run `dbt-bouncer` against locally generated manifests.

    Parameters:
        schema_name_pattern (str): Regexp the schema name must match.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_schema_name
              include: ^models/intermediate
              schema_name_pattern: .*intermediate # Accounting for schemas like `dbt_jdoe_intermediate`.
            - name: check_model_schema_name
              include: ^models/staging
              schema_name_pattern: .*stg_.*
        ```

    """
    compiled = compile_pattern(schema_name_pattern.strip())
    if compiled.match(str(model.schema_)) is None:
        fail(
            f"`{model.schema_}` does not match the supplied regex `{schema_name_pattern.strip()})`."
        )
