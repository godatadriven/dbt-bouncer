import re
from typing import Any, Dict, List, Literal, Optional, Union

import pytest
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.logger import logger
from dbt_bouncer.utils import find_missing_meta_keys, get_check_inputs


class CheckModelAccess(BaseCheck):
    access: Literal["private", "protected", "public"]
    name: Literal["check_model_access"]


@pytest.mark.iterate_over_models
def check_model_access(request, check_config=None, model=None):
    """
    Models must have the specified access attribute. Requires dbt 1.7+.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        model["access"] == check_config["access"]
    ), f"{model['unique_id']} has `{model['access']}` access, it should have access `{check_config['access']}`."


class CheckModelDescriptionPopulated(BaseCheck):
    name: Literal["check_model_description_populated"]


@pytest.mark.iterate_over_models
def check_model_description_populated(request, model=None):
    """
    Models must have a populated description.
    """

    model = get_check_inputs(model=model, request=request)["model"]

    assert (
        len(model["description"].strip()) > 4
    ), f"{model['unique_id']} does not have a populated description."


class CheckModelsDocumentationCoverage(BaseCheck):
    name: Literal["check_model_documentation_coverage"]
    min_model_documentation_coverage_pct: int = Field(
        default=100,
        description="The minimum percentage of models that must have a populated description. Default: 100",
        ge=0,
        le=100,
    )


def check_model_documentation_coverage(models, request, check_config=None):
    """
    Set the minimum percentage of models that have a populated description.
    """

    min_model_documentation_coverage_pct = get_check_inputs(
        check_config=check_config, request=request
    )["check_config"]["min_model_documentation_coverage_pct"]

    num_models = len(models)
    models_with_description = []
    for model in models:
        if len(model["description"].strip()) > 4:
            models_with_description.append(model["unique_id"])

    num_models_with_descriptions = len(models_with_description)
    model_description_coverage_pct = (num_models_with_descriptions / num_models) * 100

    assert (
        model_description_coverage_pct >= min_model_documentation_coverage_pct
    ), f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {min_model_documentation_coverage_pct}%."


class CheckModelDocumentedInSameDirectory(BaseCheck):
    name: Literal["check_model_documented_in_same_directory"]


@pytest.mark.iterate_over_models
def check_model_documented_in_same_directory(request, model=None) -> None:
    """
    Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).
    """

    model = get_check_inputs(model=model, request=request)["model"]

    model_doc_dir = model["patch_path"][model["patch_path"].find("models") :].split("/")[1:-1]
    model_sql_dir = model["path"].split("/")[:-1]

    assert (
        model_doc_dir == model_sql_dir
    ), f"`{model['unique_id'].split('.')[-1]}` is documented in a different directory to the `.sql` file: {'/'.join(model_doc_dir)} vs {'/'.join(model_sql_dir)}."


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str = Field(
        description="The regexp pattern that should not be matched by the model code."
    )


@pytest.mark.iterate_over_models
def check_model_code_does_not_contain_regexp_pattern(request, check_config=None, model=None):
    """
    The raw code for a model must not match the specified regexp pattern.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        re.compile(check_config["regexp_pattern"].strip(), flags=re.DOTALL).match(
            model["raw_code"]
        )
        is None
    ), f"`{model['unique_id']}` contains a banned string: `{check_config['regexp_pattern'].strip()}`."


class CheckModelDependsOnMultipleSources(BaseCheck):
    name: Literal["check_model_depends_on_multiple_sources"]


@pytest.mark.iterate_over_models
def check_model_depends_on_multiple_sources(request, model=None):
    """
    Models cannot reference more than one source.
    """

    model = get_check_inputs(model=model, request=request)["model"]
    num_reffed_sources = sum(x.split(".")[0] == "source" for x in model["depends_on"]["nodes"])
    assert num_reffed_sources <= 1, f"`{model['unique_id']}` references more than one source."


class CheckModelDirectories(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: str = Field(
        description="Regex pattern to match the model path. Only model paths that match the pattern will be checked."
    )
    name: Literal["check_model_directories"]
    permitted_sub_directories: List[str] = Field(description="List of permitted sub-directories.")


@pytest.mark.iterate_over_models
def check_model_directories(request, check_config=None, model=None):
    """
    Only specified sub-directories are permitted.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    include = input_vars["check_config"]["include"]
    model = input_vars["model"]
    permitted_sub_directories = input_vars["check_config"]["permitted_sub_directories"]

    # Special case for `models` directory
    if include == "":
        assert (
            model["path"].split("/")[0] in permitted_sub_directories
        ), f"{model['unique_id']} is located in `{model['path'].split('/')[0]}`, this is not a valid sub- directory."
    else:
        matched_path = re.compile(include.strip()).match(model["path"])
        path_after_match = model["path"][matched_path.end() + 1 :]  # type: ignore[union-attr]

        assert (
            path_after_match.split("/")[0] in permitted_sub_directories
        ), f"{model['unique_id']} is located in `{model['path'].split('/')[0]}`, this is not a valid sub- directory."


class CheckModelHasMetaKeys(BaseCheck):
    keys: Optional[Union[Dict[str, Any], List[Any]]]
    name: Literal["check_model_has_meta_keys"]


@pytest.mark.iterate_over_models
def check_model_has_meta_keys(request, check_config=None, model=None) -> None:
    """
    The `meta` config for models must have the specified keys.
    """

    input_vars = get_check_inputs(
        check_config=check_config,
        model=model,
        request=request,
    )
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    missing_keys = find_missing_meta_keys(
        meta_config=model.get("meta"),
        required_keys=check_config["keys"],
    )
    assert (
        missing_keys == []
    ), f"{model['unique_id']} is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


class CheckModelHasNoUpstreamDependencies(BaseCheck):
    name: Literal["check_model_has_no_upstream_dependencies"]


@pytest.mark.iterate_over_models
def check_model_has_no_upstream_dependencies(request, model=None):
    """
    Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.
    """

    model = get_check_inputs(model=model, request=request)["model"]
    assert (
        len(model["depends_on"]["nodes"]) > 0
    ), f"`{model['unique_id']}` has no upstream dependencies, this likely indicates hard-coded tables references."


class CheckModelHasUniqueTest(BaseCheck):
    accepted_uniqueness_tests: Optional[List[str]] = Field(
        default=[
            "expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ],
        description="List of tests that are accepted as uniqueness tests. If not provided, defaults to `expect_compound_columns_to_be_unique`, `dbt_utils.unique_combination_of_columns` and `unique`.",
    )
    name: Literal["check_model_has_unique_test"]


@pytest.mark.iterate_over_models
def check_model_has_unique_test(request, tests, check_config=None, model=None):
    """
    Models must have a test for uniqueness of a column.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    accepted_uniqueness_tests = check_config["accepted_uniqueness_tests"]
    logger.debug(f"{accepted_uniqueness_tests=}")

    num_unique_tests = sum(
        test["attached_node"] == model["unique_id"]
        and test["test_metadata"].get("name") in accepted_uniqueness_tests
        for test in tests
    )
    assert (
        num_unique_tests >= 1
    ), f"{model['unique_id']} does not have a test for uniqueness of a column."


class CheckModelMaxFanout(BaseCheck):
    max_downstream_models: int = Field(
        default=3, description="The maximum number of permitted downstream models."
    )
    name: Literal["check_model_max_fanout"]


@pytest.mark.iterate_over_models
def check_model_max_fanout(models, request, check_config=None, model=None):
    """
    Models cannot have more than the specified number of downstream models (default: 3).
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    max_downstream_models = input_vars["check_config"]["max_downstream_models"]
    model = input_vars["model"]

    num_downstream_models = sum(model["unique_id"] in m["depends_on"]["nodes"] for m in models)

    assert (
        num_downstream_models <= max_downstream_models
    ), f"{model['unique_id']} has {num_downstream_models} downstream models, which is more than the permitted maximum of {max_downstream_models}."


class CheckModelMaxUpstreamDependencies(BaseCheck):
    max_upstream_macros: int = Field(
        default=5, description="The maximum number of permitted upstream macros."
    )
    max_upstream_models: int = Field(
        default=5, description="The maximum number of permitted upstream models."
    )
    max_upstream_sources: int = Field(
        default=1, description="The maximum number of permitted upstream sources."
    )
    name: Literal["check_model_max_upstream_dependencies"]


@pytest.mark.iterate_over_models
def check_model_max_upstream_dependencies(request, check_config=None, model=None):
    """
    Limit the number of upstream dependencies a model has. Default values are 5 for models, 5 for macros, and 1 for sources.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    max_upstream_macros = input_vars["check_config"]["max_upstream_macros"]
    max_upstream_models = input_vars["check_config"]["max_upstream_models"]
    max_upstream_sources = input_vars["check_config"]["max_upstream_sources"]
    model = input_vars["model"]

    num_upstream_macros = len([m for m in model["depends_on"]["macros"]])
    num_upstream_models = len(
        [m for m in model["depends_on"]["nodes"] if m.split(".")[0] == "model"]
    )
    num_upstream_sources = len(
        [m for m in model["depends_on"]["nodes"] if m.split(".")[0] == "source"]
    )

    assert (
        num_upstream_macros <= max_upstream_macros
    ), f"`{model['unique_id']}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {max_upstream_macros}."
    assert (
        num_upstream_models <= max_upstream_models
    ), f"`{model['unique_id']}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {max_upstream_models}."
    assert (
        num_upstream_sources <= max_upstream_sources
    ), f"`{model['unique_id']}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {max_upstream_sources}."


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str = Field(description="Regexp the model name must match.")


@pytest.mark.iterate_over_models
def check_model_names(request, check_config=None, model=None):
    """
    Models must have a name that matches the supplied regex.
    """

    input_vars = get_check_inputs(check_config=check_config, model=model, request=request)
    check_config = input_vars["check_config"]
    model = input_vars["model"]

    assert (
        re.compile(check_config["model_name_pattern"].strip()).match(model["name"]) is not None
    ), f"{model['unique_id']} does not match the supplied regex `({check_config['model_name_pattern'].strip()})`."


class CheckModelsTestCoverage(BaseCheck):
    name: Literal["check_model_test_coverage"]
    min_model_test_coverage_pct: int = Field(
        default=100,
        description="The minimum percentage of models that must have at least one test. Default: 100",
        ge=0,
        le=100,
    )


def check_model_test_coverage(models, request, tests, check_config=None):
    """
    Set the minimum percentage of models that have at least one test.
    """

    min_model_test_coverage_pct = get_check_inputs(check_config=check_config, request=request)[
        "check_config"
    ]["min_model_test_coverage_pct"]

    num_models = len(models)
    models_with_tests = []
    for model in models:
        for test in tests:
            if model["unique_id"] in test["depends_on"]["nodes"]:
                models_with_tests.append(model["unique_id"])
    num_models_with_tests = len(set(models_with_tests))
    model_test_coverage_pct = (num_models_with_tests / num_models) * 100

    assert (
        model_test_coverage_pct >= min_model_test_coverage_pct
    ), f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {min_model_test_coverage_pct}%."
