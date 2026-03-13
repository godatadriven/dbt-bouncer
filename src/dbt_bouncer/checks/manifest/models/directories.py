"""Checks related to model file locations, names, and directory structure."""

from pathlib import Path

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import clean_path_str, compile_pattern, get_clean_model_name


@check
def check_model_directories(
    model, *, include: str, permitted_sub_directories: list[str]
):
    """Only specified sub-directories are permitted."""
    compiled_include = compile_pattern(include.strip().rstrip("/"))
    clean_path = clean_path_str(model.original_file_path)
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
    r"""Models must have a file name that matches the supplied regex."""
    compiled = compile_pattern(file_name_pattern.strip())
    file_name = Path(clean_path_str(model.original_file_path)).name
    if compiled.match(file_name) is None:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is in a file that does not match the supplied regex `{file_name_pattern.strip()}`."
        )


@check
def check_model_property_file_location(model):
    """Model properties files must follow the guidance provided by dbt."""
    if not (
        hasattr(model, "patch_path")
        and model.patch_path
        and clean_path_str(model.patch_path or "") is not None
    ):
        fail(f"`{get_clean_model_name(model.unique_id)}` is not documented.")

    original_path = Path(clean_path_str(model.original_file_path))
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
    properties_yml_name = Path(clean_path_str(model.patch_path or "")).name

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
    """Models must have a schema name that matches the supplied regex."""
    compiled = compile_pattern(schema_name_pattern.strip())
    if compiled.match(str(model.schema_)) is None:
        fail(
            f"`{model.schema_}` does not match the supplied regex `{schema_name_pattern.strip()})`."
        )
