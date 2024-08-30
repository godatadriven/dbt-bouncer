# mypy: disable-error-code="union-attr"

import logging
import re
import warnings
from typing import TYPE_CHECKING, List, Literal, Optional

import semver
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
            UnitTests,
        )
    from dbt_bouncer.parsers import DbtBouncerManifest, DbtBouncerModel, DbtBouncerTest


class CheckModelAccess(BaseCheck):
    access: Literal["private", "protected", "public"]
    name: Literal["check_model_access"]


def check_model_access(
    access: str,
    model: "DbtBouncerModel",
    **kwargs,
) -> None:
    """Models must have the specified access attribute. Requires dbt 1.7+.

    Parameters:
        access (Literal["private", "protected", "public"]): The access level to check for.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Align with dbt best practices that marts should be `public`, everything else should be `protected`
            - name: check_model_access
              access: protected
              include: ^models/intermediate
            - name: check_model_access
              access: public
              include: ^models/marts
            - name: check_model_access
              access: protected
              include: ^models/staging
        ```

    """
    assert (
        model.access.value == access
    ), f"`{model.name}` has `{model.access.value}` access, it should have access `{access}`."


class CheckModelContractsEnforcedForPublicModel(BaseCheck):
    name: Literal["check_model_contract_enforced_for_public_model"]


def check_model_contract_enforced_for_public_model(
    model: "DbtBouncerModel",
    **kwargs,
) -> None:
    """Public models must have contracts enforced.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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


def check_model_description_populated(model: "DbtBouncerModel", **kwargs) -> None:
    """Models must have a populated description.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    name: Literal["check_model_documentation_coverage"]
    min_model_documentation_coverage_pct: int = Field(
        default=100,
        ge=0,
        le=100,
    )
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )


def check_model_documentation_coverage(
    models: List["DbtBouncerModel"],
    min_model_documentation_coverage_pct: float = 100,
    **kwargs,
) -> None:
    """Set the minimum percentage of models that have a populated description.

    Parameters:
        min_model_documentation_coverage_pct (float): The minimum percentage of models that must have a populated description.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        model_description_coverage_pct >= min_model_documentation_coverage_pct
    ), f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {min_model_documentation_coverage_pct}%."


class CheckModelDocumentedInSameDirectory(BaseCheck):
    name: Literal["check_model_documented_in_same_directory"]


def check_model_documented_in_same_directory(
    model: "DbtBouncerModel", **kwargs
) -> None:
    """Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_documented_in_same_directory
        ```

    """
    model_doc_dir = model.patch_path[model.patch_path.find("models") :].split("/")[:-1]
    model_sql_dir = model.original_file_path.split("/")[:-1]

    assert (
        model_doc_dir == model_sql_dir
    ), f"`{model.name}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str


def check_model_code_does_not_contain_regexp_pattern(
    model: "DbtBouncerModel",
    regexp_pattern: str,
    **kwargs,
) -> None:
    """The raw code for a model must not match the specified regexp pattern.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_model_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```

    """
    assert (
        re.compile(regexp_pattern.strip(), flags=re.DOTALL).match(model.raw_code)
        is None
    ), f"`{model.name}` contains a banned string: `{regexp_pattern.strip()}`."


class CheckModelDependsOnMultipleSources(BaseCheck):
    name: Literal["check_model_depends_on_multiple_sources"]


def check_model_depends_on_multiple_sources(model: "DbtBouncerModel", **kwargs) -> None:
    """Models cannot reference more than one source.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_depends_on_multiple_sources
        ```

    """
    num_reffed_sources = sum(
        x.split(".")[0] == "source" for x in model.depends_on.nodes
    )
    assert num_reffed_sources <= 1, f"`{model.name}` references more than one source."


class CheckModelDirectories(BaseCheck):
    name: Literal["check_model_directories"]
    permitted_sub_directories: List[str]


def check_model_directories(
    include: str,
    model: "DbtBouncerModel",
    permitted_sub_directories: List[str],
    **kwargs,
) -> None:
    """Only specified sub-directories are permitted.

    Parameters:
        include (str): Regex pattern to the directory to check.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        permitted_sub_directories (List[str]): List of permitted sub-directories.

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
    matched_path = re.compile(include.strip()).match(model.original_file_path)
    path_after_match = model.original_file_path[matched_path.end() + 1 :]  # type: ignore[union-attr]

    assert (
        path_after_match.split("/")[0] in permitted_sub_directories
    ), f"`{model.name}` is located in `{model.original_file_path.split('/')[1]}`, this is not a valid sub-directory. {path_after_match.split('/')[0]}"


class CheckModelHasContractsEnforced(BaseCheck):
    name: Literal["check_model_has_contracts_enforced"]


def check_model_has_contracts_enforced(model: "DbtBouncerModel", **kwargs) -> None:
    """Model must have contracts enforced.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_contracts_enforced
              include: ^models/marts
        ```

    """
    assert (
        model.contract.enforced is True
    ), f"`{model.name}` does not have contracts enforced."


class CheckModelHasMetaKeys(BaseCheck):
    keys: NestedDict
    name: Literal["check_model_has_meta_keys"]


def check_model_has_meta_keys(
    keys: NestedDict,
    model: "DbtBouncerModel",
    **kwargs,
) -> None:
    """The `meta` config for models must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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


def check_model_has_no_upstream_dependencies(
    model: "DbtBouncerModel", **kwargs
) -> None:
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
    tags: List[str]


def check_model_has_tags(model: "DbtBouncerModel", tags: List[str], **kwargs) -> None:
    """Models must have the specified tags.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        tags (List[str]): List of tags to check for.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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


def check_model_has_unique_test(
    model: "DbtBouncerModel",
    tests: "DbtBouncerTest",
    accepted_uniqueness_tests: List[str] = (
        [  # noqa: B006
            "expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ]
    ),
    **kwargs,
) -> None:
    """Models must have a test for uniqueness of a column.

    Parameters:
        accepted_uniqueness_tests (Optional[List[str]]): List of tests that are accepted as uniqueness tests.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        tests (List[DbtBouncerTest]): List of DbtBouncerTest objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
          - name: check_model_has_unique_test
            include: ^models/marts
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
        and test.test_metadata.name in accepted_uniqueness_tests
        for test in tests
    )
    assert (
        num_unique_tests >= 1
    ), f"`{model.name}` does not have a test for uniqueness of a column."


class CheckModelHasUnitTests(BaseCheck):
    min_number_of_unit_tests: int = Field(default=1)
    name: Literal["check_model_has_unit_tests"]


def check_model_has_unit_tests(
    manifest_obj: "DbtBouncerManifest",
    model: "DbtBouncerModel",
    unit_tests: List["UnitTests"],
    min_number_of_unit_tests: int = 1,
    **kwargs,
) -> None:
    """Models must have more than the specified number of unit tests.

    Parameters:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        min_number_of_unit_tests (Optional[int]): The minimum number of unit tests that a model must have.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        unit_tests (List[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              min_number_of_unit_tests: 2
        ```

    """
    if semver.Version.parse(manifest_obj.manifest.metadata.dbt_version) >= "1.8.0":
        num_unit_tests = len(
            [
                t.unique_id
                for t in unit_tests
                if t.depends_on.nodes[0] == model.unique_id
            ],
        )
        assert (
            num_unit_tests >= min_number_of_unit_tests
        ), f"`{model.name}` has {num_unit_tests} unit tests, this is less than the minimum of {min_number_of_unit_tests}."
    else:
        logging.warning(
            "The `check_model_has_unit_tests` check is only supported for dbt 1.8.0 and above.",
        )


class CheckModelMaxChainedViews(BaseCheck):
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    max_chained_views: int = Field(
        default=3,
    )
    name: Literal["check_model_max_chained_views"]


def check_model_max_chained_views(
    manifest_obj: "DbtBouncerManifest",
    model: "DbtBouncerModel",
    models: List["DbtBouncerModel"],
    materializations_to_include: List[str] = ["ephemeral", "view"],  # noqa: B006
    max_chained_views: int = 3,
    **kwargs,
) -> None:
    """Models cannot have more than the specified number of upstream dependents that are not tables.

    Parameters:
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check.
        max_chained_views (Optional[int]): The maximum number of upstream dependents that are not tables.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

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
        """Recursive function to return model unique_id's of upstream models that are views. Depth of recursion can be specified. If no models meet the criteria then an empty list is returned.

        Returns
        -
            List[str]: List of model unique_id's of upstream models that are views.

        """
        if depth == max_chained_views or model_unique_ids_to_check == []:
            return model_unique_ids_to_check

        relevant_upstream_models = []
        for model in model_unique_ids_to_check:
            upstream_nodes = list(
                next(m2 for m2 in models if m2.unique_id == model).depends_on.nodes,
            )
            if upstream_nodes != []:
                upstream_models = [
                    m
                    for m in upstream_nodes
                    if m.split(".")[0] == "model" and m.split(".")[1] == package_name
                ]
                for i in upstream_models:
                    if (
                        next(m for m in models if m.unique_id == i).config.materialized
                        in materializations
                    ):
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
            ),
        )
        == 0
    ), f"`{model.name}` has more than {max_chained_views} upstream dependents that are not tables."


class CheckModelMaxFanout(BaseCheck):
    max_downstream_models: int = Field(default=3)
    name: Literal["check_model_max_fanout"]


def check_model_max_fanout(
    model: "DbtBouncerModel",
    models: List["DbtBouncerModel"],
    max_downstream_models: int = 3,
    **kwargs,
) -> None:
    """Models cannot have more than the specified number of downstream models.

    Parameters:
        max_downstream_models (Optional[int]): The maximum number of permitted downstream models.
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_fanout
              max_downstream_models: 2
        ```

    """
    num_downstream_models = sum(model.unique_id in m.depends_on.nodes for m in models)

    assert (
        num_downstream_models <= max_downstream_models
    ), f"`{model.name}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {max_downstream_models}."


class CheckModelMaxNumberOfLines(BaseCheck):
    name: Literal["check_model_max_number_of_lines"]
    max_number_of_lines: int = Field(default=100)


def check_model_max_number_of_lines(
    model: "DbtBouncerModel",
    max_number_of_lines: int = 100,
    **kwargs,
) -> None:
    """Models may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
        ```
        ```yaml
        manifest_checks:
            - name: check_model_max_number_of_lines
              max_number_of_lines: 150
        ```

    """
    actual_number_of_lines = model.raw_code.count("\n") + 1

    assert (
        actual_number_of_lines <= max_number_of_lines
    ), f"`{model.name}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({max_number_of_lines})."


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


def check_model_max_upstream_dependencies(
    model: "DbtBouncerModel",
    max_upstream_macros: int = 5,
    max_upstream_models: int = 5,
    max_upstream_sources: int = 1,
    **kwargs,
) -> None:
    """Limit the number of upstream dependencies a model has.

    Parameters:
        max_upstream_macros (Optional[int]): The maximum number of permitted upstream macros.
        max_upstream_models (Optional[int]): The maximum number of permitted upstream models.
        max_upstream_sources (Optional[int]): The maximum number of permitted upstream sources.
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_max_upstream_dependencies
              max_upstream_models: 3
        ```

    """
    num_upstream_macros = len(list(model.depends_on.macros))
    num_upstream_models = len(
        [m for m in model.depends_on.nodes if m.split(".")[0] == "model"],
    )
    num_upstream_sources = len(
        [m for m in model.depends_on.nodes if m.split(".")[0] == "source"],
    )

    assert (
        num_upstream_macros <= max_upstream_macros
    ), f"`{model.name}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {max_upstream_macros}."
    assert (
        num_upstream_models <= max_upstream_models
    ), f"`{model.name}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {max_upstream_models}."
    assert (
        num_upstream_sources <= max_upstream_sources
    ), f"`{model.name}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {max_upstream_sources}."


class CheckModelNames(BaseCheck):
    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_model_names"]
    model_name_pattern: str


def check_model_names(
    model: "DbtBouncerModel",
    model_name_pattern: str,
    **kwargs,
) -> None:
    """Models must have a name that matches the supplied regex.

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.
        model_name_pattern (str): Regexp the model name must match.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_names
              include: ^models/intermediate
              model_name_pattern: ^int_
            - name: check_model_names
              include: ^models/staging
              model_name_pattern: ^stg_
        ```

    """
    assert (
        re.compile(model_name_pattern.strip()).match(model.name) is not None
    ), f"`{model.name}` does not match the supplied regex `{model_name_pattern.strip()})`."


class CheckModelPropertyFileLocation(BaseCheck):
    name: Literal["check_model_property_file_location"]


def check_model_property_file_location(model: "DbtBouncerModel", **kwargs) -> None:
    """Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Parameters:
        model (DbtBouncerModel): The DbtBouncerModel object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_property_file_location
        ```

    """
    expected_substr = (
        "_".join(model.original_file_path.split("/")[1:-1])
        .replace("staging", "stg")
        .replace("intermediate", "int")
        .replace("marts", "")
    )
    properties_yml_name = model.patch_path.split("/")[-1]

    assert properties_yml_name.startswith(
        "_",
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not start with an underscore."
    assert (
        expected_substr in properties_yml_name
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
    assert properties_yml_name.endswith(
        "__models.yml",
    ), f"The properties file for `{model.name}` (`{properties_yml_name}`) does not end with `__models.yml`."


class CheckModelsTestCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    name: Literal["check_model_test_coverage"]
    min_model_test_coverage_pct: float = Field(
        default=100,
        ge=0,
        le=100,
    )
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )


def check_model_test_coverage(
    models: List["DbtBouncerModel"],
    tests: List["DbtBouncerTest"],
    min_model_test_coverage_pct: float = 100,
    **kwargs,
) -> None:
    """Set the minimum percentage of models that have at least one test.

    Parameters:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test.
        models (List[DbtBouncerModel]): List of DbtBouncerModel objects parsed from `manifest.json`.
        tests (List[DbtBouncerTest]): List of DbtBouncerTest objects parsed from `manifest.json`.

    Other Parameters:
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.


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
        model_test_coverage_pct >= min_model_test_coverage_pct
    ), f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {min_model_test_coverage_pct}%."
