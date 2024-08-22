# mypy: disable-error-code="union-attr"

import re
from typing import List, Literal, Optional, Union

import pytest
from _pytest.fixtures import TopRequest
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.parsers import DbtBouncerManifest, DbtBouncerModel
from dbt_bouncer.utils import bouncer_check, find_missing_meta_keys


class CheckModelAccess(BaseCheck):
    access: Literal["private", "protected", "public"]
    name: Literal["check_model_access"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_access(
    request: TopRequest,
    access: Union[None, str] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    Models must have the specified access attribute. Requires dbt 1.7+.

    Receives:
        access (Literal["private", "protected", "public"]): The access level to check for.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            # Align with dbt best practices that marts should be `public`, everything else should be `protected`
            - name: check_model_access
              access: protected
              include: ^intermediate
            - name: check_model_access
              access: public
              include: ^marts
            - name: check_model_access
              access: protected
              include: ^staging
        ```
    """

    assert (
        model.access.value == access
    ), f"`{model.name}` has `{model.access.value}` access, it should have access `{access}`."


class CheckModelContractsEnforcedForPublicModel(BaseCheck):
    name: Literal["check_model_contract_enforced_for_public_model"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_contract_enforced_for_public_model(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Public models must have contracts enforced.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_contract_enforced_for_public_model
        ```
    """

    if model.access.value == "public":
        assert (
            model.contract.enforced is True
        ), f"`{model.name}` is a public model but does not have contracts enforced."


class CheckModelDescriptionPopulated(BaseCheck):
    name: Literal["check_model_description_populated"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_description_populated(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Models must have a populated description.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_description_populated
        ```
    """

    assert (
        len(model.description.strip()) > 4
    ), f"`{model.name}` does not have a populated description."


class CheckModelsDocumentationCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_model_documentation_coverage"]
    min_model_documentation_coverage_pct: int = Field(
        default=100,
        ge=0,
        le=100,
    )


@bouncer_check
def check_model_documentation_coverage(
    request: TopRequest,
    models: List[DbtBouncerModel],
    min_model_documentation_coverage_pct: Union[float, None] = None,
    **kwargs,
) -> None:
    """
    Set the minimum percentage of models that have a populated description.

    Receives:
        min_model_documentation_coverage_pct (float): The minimum percentage of models that must have a populated description. Default: 100.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_description_populated
              min_model_documentation_coverage_pct: 90
        ```
    """

    num_models = len(models)
    models_with_description = []
    for model in models:
        if len(model.description.strip()) > 4:
            models_with_description.append(model.unique_id)

    num_models_with_descriptions = len(models_with_description)
    model_description_coverage_pct = (num_models_with_descriptions / num_models) * 100

    assert (
        model_description_coverage_pct >= min_model_documentation_coverage_pct  # type: ignore[operator]
    ), f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {min_model_documentation_coverage_pct}%."


class CheckModelDocumentedInSameDirectory(BaseCheck):
    name: Literal["check_model_documented_in_same_directory"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_documented_in_same_directory(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_documented_in_same_directory
        ```
    """

    model_doc_dir = model.patch_path[model.patch_path.find("models") :].split("/")[1:-1]
    model_sql_dir = model.path.split("/")[:-1]

    assert (
        model_doc_dir == model_sql_dir
    ), f"`{model.name}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_code_does_not_contain_regexp_pattern(
    request: TopRequest,
    model: Union[DbtBouncerModel, None] = None,
    regexp_pattern: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    The raw code for a model must not match the specified regexp pattern.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_model_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```
    """

    assert (
        re.compile(regexp_pattern.strip(), flags=re.DOTALL).match(model.raw_code) is None
    ), f"`{model.name}` contains a banned string: `{regexp_pattern.strip()}`."


class CheckModelDependsOnMultipleSources(BaseCheck):
    name: Literal["check_model_depends_on_multiple_sources"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_depends_on_multiple_sources(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Models cannot reference more than one source.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_depends_on_multiple_sources
        ```
    """

    num_reffed_sources = sum(x.split(".")[0] == "source" for x in model.depends_on.nodes)
    assert num_reffed_sources <= 1, f"`{model.name}` references more than one source."


class CheckModelDirectories(BaseCheck):
    name: Literal["check_model_directories"]
    permitted_sub_directories: List[str]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_directories(
    request: TopRequest,
    include: Union[None, str] = None,
    model: Union[DbtBouncerModel, None] = None,
    permitted_sub_directories: Union[List[str], None] = None,
    **kwargs,
) -> None:
    """
    Only specified sub-directories are permitted.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked. Special case: if you want to check `./models`, pass "" to `include`.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        permitted_sub_directories (List[str]): List of permitted sub-directories.

    Example(s):
        ```yaml
        manifest_checks:
        # Special case for top level directories within `./models`, pass "" to `include`
        - name: check_model_directories
            include: ""
            permitted_sub_directories:
            - intermediate
            - marts
            - staging
        ```
        ```yaml
        # Restrict sub-directories within `./models/staging`
        - name: check_model_directories
            include: ^staging
            permitted_sub_directories:
            - crm
            - payments
        ```
    """

    # Special case for `models` directory
    if include == "":
        assert (
            model.path.split("/")[0] in permitted_sub_directories  # type: ignore[operator]
        ), f"`{model.name}` is located in `{model.path.split('/')[0]}`, this is not a valid sub-directory."
    else:
        matched_path = re.compile(include.strip()).match(model.path)
        path_after_match = model.path[matched_path.end() + 1 :]  # type: ignore[union-attr]

        assert (
            path_after_match.split("/")[0] in permitted_sub_directories  # type: ignore[operator]
        ), f"`{model.name}` is located in `{model.path.split('/')[0]}`, this is not a valid sub-directory."


class CheckModelHasContractsEnforced(BaseCheck):
    name: Literal["check_model_has_contracts_enforced"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_has_contracts_enforced(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Model must have contracts enforced.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_contracts_enforced
                include: ^marts
        ```
    """

    assert model.contract.enforced is True, f"`{model.name}` does not have contracts enforced."


class CheckModelHasMetaKeys(BaseCheck):
    keys: NestedDict
    name: Literal["check_model_has_meta_keys"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_has_meta_keys(
    request: TopRequest,
    keys: Union[NestedDict, None] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    The `meta` config for models must have the specified keys.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_meta_keys
              keys:
                - maturity
                - owner
        ```
    """

    missing_keys = find_missing_meta_keys(
        meta_config=model.meta,
        required_keys=keys,
    )
    assert (
        missing_keys == []
    ), f"`{model.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


class CheckModelHasNoUpstreamDependencies(BaseCheck):
    name: Literal["check_model_has_no_upstream_dependencies"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_has_no_upstream_dependencies(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_no_upstream_dependencies
        ```
    """

    assert (
        len(model.depends_on.nodes) > 0
    ), f"`{model.name}` has no upstream dependencies, this likely indicates hard-coded tables references."


class CheckModelHasTags(BaseCheck):
    name: Literal["check_model_has_tags"]
    tags: List[str] = Field(default=[])


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_has_tags(
    request: TopRequest,
    model: Union[DbtBouncerModel, None] = None,
    tags: Union[List[str], None] = None,
    **kwargs,
) -> None:
    """
    Models must have the specified tags.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        tags (List[str]): List of tags to check for.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_tags
              tags:
                - tag_1
                - tag_2
        ```
    """

    missing_tags = [tag for tag in tags if tag not in model.tags]
    assert not missing_tags, f"`{model.name}` is missing required tags: {missing_tags}."


class CheckModelHasUniqueTest(BaseCheck):
    accepted_uniqueness_tests: Optional[List[str]] = Field(
        default=[
            "expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ],
    )
    name: Literal["check_model_has_unique_test"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_has_unique_test(
    request: TopRequest,
    tests: List[DbtBouncerModel],
    accepted_uniqueness_tests: Union[List[str], None] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    Models must have a test for uniqueness of a column.

    Receives:
        accepted_uniqueness_tests (Optional[List[str]]): List of tests that are accepted as uniqueness tests. If not provided, defaults to `expect_compound_columns_to_be_unique`, `dbt_utils.unique_combination_of_columns` and `unique`.
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
          - name: check_model_has_unique_test
        ```
        ```yaml
        manifest_checks:
          # Example of allowing a custom uniqueness test
          - name: check_model_has_unique_test
            accepted_uniqueness_tests:
                - expect_compound_columns_to_be_unique
                - my_custom_uniqueness_test
                - unique
        ```
    """

    num_unique_tests = sum(
        test.attached_node == model.unique_id
        and test.test_metadata.name in accepted_uniqueness_tests  # type: ignore[operator]
        for test in tests
    )
    assert (
        num_unique_tests >= 1
    ), f"`{model.name}` does not have a test for uniqueness of a column."


class CheckModelMaxChainedViews(BaseCheck):
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    max_chained_views: int = Field(
        default=3,
    )
    name: Literal["check_model_max_chained_views"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_max_chained_views(
    manifest_obj: DbtBouncerManifest,
    models: List[DbtBouncerModel],
    request: TopRequest,
    materializations_to_include: Union[List[str], None] = None,
    max_chained_views: Union[int, None] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    Models cannot have more than the specified number of upstream dependents that are not tables (default: 3).

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check. If not provided, defaults to `ephemeral` and `view`.
        max_chained_views (Optional[int]): The maximum number of upstream dependents that are not tables. Default: 3
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_chained_views
              materializations_to_include:
                - ephemeral
                - my_custom_materialization
                - view
              max_chained_views: 5
        ```
    """

    def return_upstream_view_models(
        materializations,
        max_chained_views,
        models,
        model_unique_ids_to_check,
        package_name,
        depth=0,
    ):
        """
        Recursive function to return model unique_id's of upstream models that are views. Depth of recursion can be specified. If no models meet the criteria then an empty list is returned.
        """

        if depth == max_chained_views or model_unique_ids_to_check == []:
            return model_unique_ids_to_check

        relevant_upstream_models = []
        for model in model_unique_ids_to_check:
            upstream_nodes = list(
                [m2 for m2 in models if m2.unique_id == model][0].depends_on.nodes
            )
            if upstream_nodes != []:
                upstream_models = [
                    m
                    for m in upstream_nodes
                    if m.split(".")[0] == "model" and m.split(".")[1] == package_name
                ]
                for i in upstream_models:
                    if [m for m in models if m.unique_id == i][
                        0
                    ].config.materialized in materializations:
                        relevant_upstream_models.append(i)

        depth += 1
        return return_upstream_view_models(
            materializations=materializations,
            max_chained_views=max_chained_views,
            models=models,
            model_unique_ids_to_check=relevant_upstream_models,
            package_name=package_name,
            depth=depth,
        )

    assert (
        len(
            return_upstream_view_models(
                materializations=materializations_to_include,
                max_chained_views=max_chained_views,
                models=models,
                model_unique_ids_to_check=[model.unique_id],
                package_name=manifest_obj.manifest.metadata.project_name,
            )
        )
        == 0
    ), f"`{model.name}` has more than {max_chained_views} upstream dependents that are not tables."


class CheckModelMaxFanout(BaseCheck):
    max_downstream_models: int = Field(default=3)
    name: Literal["check_model_max_fanout"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_max_fanout(
    models: List[DbtBouncerModel],
    request: TopRequest,
    max_downstream_models: Union[int, None] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    Models cannot have more than the specified number of downstream models (default: 3).

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        max_downstream_models (Optional[int]): The maximum number of permitted downstream models. Default: 3
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_fanout
              max_downstream_models: 2
        ```
    """

    num_downstream_models = sum(model.unique_id in m.depends_on.nodes for m in models)

    assert (
        num_downstream_models <= max_downstream_models  # type: ignore[operator]
    ), f"`{model.name}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {max_downstream_models}."


class CheckModelMaxUpstreamDependencies(BaseCheck):
    max_upstream_macros: int = Field(
        default=5,
    )
    max_upstream_models: int = Field(
        default=5,
    )
    max_upstream_sources: int = Field(
        default=1,
    )
    name: Literal["check_model_max_upstream_dependencies"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_max_upstream_dependencies(
    request: TopRequest,
    max_upstream_macros: Union[int, None] = None,
    max_upstream_models: Union[int, None] = None,
    max_upstream_sources: Union[int, None] = None,
    model: Union[DbtBouncerModel, None] = None,
    **kwargs,
) -> None:
    """
    Limit the number of upstream dependencies a model has. Default values are 5 for models, 5 for macros, and 1 for sources.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        max_upstream_macros (Optional[int]): The maximum number of permitted upstream macros. Default: 5
        max_upstream_models (Optional[int]): The maximum number of permitted upstream models. Default: 5
        max_upstream_sources (Optional[int]): The maximum number of permitted upstream sources. Default: 1
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_upstream_dependencies
              max_upstream_models: 3
        ```
    """

    num_upstream_macros = len(list(model.depends_on.macros))
    num_upstream_models = len([m for m in model.depends_on.nodes if m.split(".")[0] == "model"])
    num_upstream_sources = len([m for m in model.depends_on.nodes if m.split(".")[0] == "source"])

    assert (
        num_upstream_macros <= max_upstream_macros  # type: ignore[operator]
    ), f"`{model.name}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {max_upstream_macros}."
    assert (
        num_upstream_models <= max_upstream_models  # type: ignore[operator]
    ), f"`{model.name}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {max_upstream_models}."
    assert (
        num_upstream_sources <= max_upstream_sources  # type: ignore[operator]
    ), f"`{model.name}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {max_upstream_sources}."


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_names(
    request: TopRequest,
    model: Union[DbtBouncerModel, None] = None,
    model_name_pattern: Union[None, str] = None,
    **kwargs,
) -> None:
    """
    Models must have a name that matches the supplied regex.

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model_name_pattern (str): Regexp the model name must match.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_names
              include: ^intermediate
              model_name_pattern: ^int_
            - name: check_model_names
              include: ^staging
              model_name_pattern: ^stg_
        ```
    """

    assert (
        re.compile(model_name_pattern.strip()).match(model.name) is not None
    ), f"`{model.name}` does not match the supplied regex `{model_name_pattern.strip()})`."


class CheckModelPropertyFileLocation(BaseCheck):
    name: Literal["check_model_property_file_location"]


@pytest.mark.iterate_over_models
@bouncer_check
def check_model_property_file_location(
    request: TopRequest, model: Union[DbtBouncerModel, None] = None, **kwargs
) -> None:
    """
    Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Receives:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_property_file_location
        ```
    """

    expected_substr = (
        "_".join(model.path.split("/")[:-1])
        .replace("staging", "stg")
        .replace("intermediate", "int")
        .replace("marts", "")
    )
    properties_yml_name = model.patch_path.split("/")[-1]

    assert properties_yml_name.startswith(
        "_"
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not start with an underscore."
    assert (
        expected_substr in properties_yml_name
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
    assert properties_yml_name.endswith(
        "__models.yml"
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not end with `__models.yml`."


class CheckModelsTestCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
    name: Literal["check_model_test_coverage"]
    min_model_test_coverage_pct: float = Field(
        default=100,
        ge=0,
        le=100,
    )


@bouncer_check
def check_model_test_coverage(
    models: List[DbtBouncerModel],
    request: TopRequest,
    tests: List[DbtBouncerModel],
    min_model_test_coverage_pct: Union[float, None] = None,
    **kwargs,
) -> None:
    """
    Set the minimum percentage of models that have at least one test.

    Receives:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test. Default: 100

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_test_coverage
              min_model_test_coverage_pct: 90
        ```
    """

    num_models = len(models)
    models_with_tests = []
    for model in models:
        for test in tests:
            if model.unique_id in test.depends_on.nodes:
                models_with_tests.append(model.unique_id)
    num_models_with_tests = len(set(models_with_tests))
    model_test_coverage_pct = (num_models_with_tests / num_models) * 100

    assert (
        model_test_coverage_pct >= min_model_test_coverage_pct  # type: ignore[operator]
    ), f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {min_model_test_coverage_pct}%."
