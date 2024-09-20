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
    from dbt_bouncer.parsers import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )
if TYPE_CHECKING:
    from dbt_bouncer.parsers import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )
from dbt_bouncer.utils import clean_path_str


class CheckModelAccess(BaseCheck):
    """Models must have the specified access attribute. Requires dbt 1.7+.

    Parameters:
        access (Literal["private", "protected", "public"]): The access level to check for.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    access: Literal["private", "protected", "public"]
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_access"]

    def execute(self) -> None:
        """Execute the check."""
        assert (
            self.model.access.value == self.access
        ), f"`{self.model.name}` has `{self.model.access.value}` access, it should have access `{self.access}`."


class CheckModelContractsEnforcedForPublicModel(BaseCheck):
    """Public models must have contracts enforced.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_contract_enforced_for_public_model"]

    def execute(self) -> None:
        """Execute the check."""
        if self.model.access.value == "public":
            assert (
                self.model.contract.enforced is True
            ), f"`{self.model.name}` is a public model but does not have contracts enforced."


class CheckModelDescriptionPopulated(BaseCheck):
    """Models must have a populated description.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_description_populated"]

    def execute(self) -> None:
        """Execute the check."""
        assert (
            len(self.model.description.strip()) > 4
        ), f"`{self.model.name}` does not have a populated description."


class CheckModelsDocumentationCoverage(BaseModel):
    """Set the minimum percentage of models that have a populated description.

    Parameters:
        min_model_documentation_coverage_pct (float): The minimum percentage of models that must have a populated description.

    Receives:
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

    Other Parameters:
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_documentation_coverage
              min_model_documentation_coverage_pct: 90
        ```

    """

    model_config = ConfigDict(extra="forbid")

    index: Optional[int] = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    min_model_documentation_coverage_pct: int = Field(
        default=100,
        ge=0,
        le=100,
    )
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_documentation_coverage"]
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    def execute(self) -> None:
        """Execute the check."""
        num_models = len(self.models)
        models_with_description = []
        for model in self.models:
            if len(model.description.strip()) > 4:
                models_with_description.append(model.unique_id)

        num_models_with_descriptions = len(models_with_description)
        model_description_coverage_pct = (
            num_models_with_descriptions / num_models
        ) * 100

        assert (
            model_description_coverage_pct >= self.min_model_documentation_coverage_pct
        ), f"Only {model_description_coverage_pct}% of models have a populated description, this is less than the permitted minimum of {self.min_model_documentation_coverage_pct}%."


class CheckModelDocumentedInSameDirectory(BaseCheck):
    """Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_documented_in_same_directory"]

    def execute(self) -> None:
        """Execute the check."""
        model_sql_dir = clean_path_str(self.model.original_file_path).split("/")[:-1]
        assert (  # noqa: PT018
            hasattr(self.model, "patch_path")
            and clean_path_str(self.model.patch_path) is not None
        ), f"`{self.model.name}` is not documented."

        model_doc_dir = clean_path_str(
            self.model.patch_path[
                clean_path_str(self.model.patch_path).find("models") :
            ]
        ).split("/")[:-1]

        assert (
            model_doc_dir == model_sql_dir
        ), f"`{self.model.name}` is documented in a different directory to the `.sql` file: `{'/'.join(model_doc_dir)}` vs `{'/'.join(model_sql_dir)}`."


class CheckModelCodeDoesNotContainRegexpPattern(BaseCheck):
    """The raw code for a model must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the model code.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.regexp_pattern.strip(), flags=re.DOTALL).match(
                self.model.raw_code
            )
            is None
        ), f"`{self.model.name}` contains a banned string: `{self.regexp_pattern.strip()}`."


class CheckModelDependsOnMultipleSources(BaseCheck):
    """Models cannot reference more than one source.

    Parameters:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_depends_on_multiple_sources"]

    def execute(self) -> None:
        """Execute the check."""
        num_reffed_sources = sum(
            x.split(".")[0] == "source" for x in self.model.depends_on.nodes
        )
        assert (
            num_reffed_sources <= 1
        ), f"`{self.model.name}` references more than one source."


class CheckModelDirectories(BaseCheck):
    """Only specified sub-directories are permitted.

    Parameters:
        include (str): Regex pattern to the directory to check.
        permitted_sub_directories (List[str]): List of permitted sub-directories.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    include: str
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_directories"]
    permitted_sub_directories: List[str]

    def execute(self) -> None:
        """Execute the check."""
        matched_path = re.compile(self.include.strip()).match(
            clean_path_str(self.model.original_file_path)
        )
        path_after_match = clean_path_str(self.model.original_file_path)[
            matched_path.end() + 1 :
        ]

        assert (
            path_after_match.split("/")[0] in self.permitted_sub_directories
        ), f"`{self.model.name}` is located in `{self.model.original_file_path.split('/')[1]}`, this is not a valid sub-directory."


class CheckModelHasContractsEnforced(BaseCheck):
    """Model must have contracts enforced.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_contracts_enforced"]

    def execute(self) -> None:
        """Execute the check."""
        assert (
            self.model.contract.enforced is True
        ), f"`{self.model.name}` does not have contracts enforced."


class CheckModelHasMetaKeys(BaseCheck):
    """The `meta` config for models must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    keys: NestedDict
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_meta_keys"]

    def execute(self) -> None:
        """Execute the check."""
        missing_keys = find_missing_meta_keys(
            meta_config=self.model.meta,
            required_keys=self.keys.model_dump(),
        )
        assert (
            missing_keys == []
        ), f"`{self.model.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"


class CheckModelHasNoUpstreamDependencies(BaseCheck):
    """Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_no_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check."""
        assert (
            len(self.model.depends_on.nodes) > 0
        ), f"`{self.model.name}` has no upstream dependencies, this likely indicates hard-coded tables references."


class CheckModelHasTags(BaseCheck):
    """Models must have the specified tags.

    Parameters:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_tags"]
    tags: List[str]

    def execute(self) -> None:
        """Execute the check."""
        missing_tags = [tag for tag in self.tags if tag not in self.model.tags]
        assert (
            not missing_tags
        ), f"`{self.model.name}` is missing required tags: {missing_tags}."


class CheckModelHasUniqueTest(BaseCheck):
    """Models must have a test for uniqueness of a column.

    Parameters:
        accepted_uniqueness_tests (Optional[List[str]]): List of tests that are accepted as uniqueness tests.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        tests (List[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

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

    accepted_uniqueness_tests: Optional[List[str]] = Field(
        default=[
            "expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ],
    )
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_unique_test"]
    tests: List["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check."""
        num_unique_tests = sum(
            test.attached_node == self.model.unique_id
            and test.test_metadata.name in self.accepted_uniqueness_tests  # type: ignore[operator]
            for test in self.tests
            if hasattr(test, "test_metadata")
        )
        assert (
            num_unique_tests >= 1
        ), f"`{self.model.name}` does not have a test for uniqueness of a column."


class CheckModelHasUnitTests(BaseCheck):
    """Models must have more than the specified number of unit tests.

    Parameters:
        min_number_of_unit_tests (Optional[int]): The minimum number of unit tests that a model must have.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
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

    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    min_number_of_unit_tests: int = Field(default=1)
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_has_unit_tests"]
    unit_tests: List["UnitTests"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check."""
        if (
            semver.Version.parse(self.manifest_obj.manifest.metadata.dbt_version)
            >= "1.8.0"
        ):
            num_unit_tests = len(
                [
                    t.unique_id
                    for t in self.unit_tests
                    if t.depends_on.nodes[0] == self.model.unique_id
                ],
            )
            assert (
                num_unit_tests >= self.min_number_of_unit_tests
            ), f"`{self.model.name}` has {num_unit_tests} unit tests, this is less than the minimum of {self.min_number_of_unit_tests}."
        else:
            logging.warning(
                "The `check_model_has_unit_tests` check is only supported for dbt 1.8.0 and above.",
            )


class CheckModelMaxChainedViews(BaseCheck):
    """Models cannot have more than the specified number of upstream dependents that are not tables.

    Parameters:
        materializations_to_include (Optional[List[str]]): List of materializations to include in the check.
        max_chained_views (Optional[int]): The maximum number of upstream dependents that are not tables.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    manifest_obj: "DbtBouncerManifest" = Field(default=None)
    materializations_to_include: List[str] = Field(
        default=["ephemeral", "view"],
    )
    max_chained_views: int = Field(
        default=3,
    )
    model: "DbtBouncerModelBase" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_chained_views"]

    def execute(self) -> None:
        """Execute the check."""

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
                        if m.split(".")[0] == "model"
                        and m.split(".")[1] == package_name
                    ]
                    for i in upstream_models:
                        if (
                            next(
                                m for m in models if m.unique_id == i
                            ).config.materialized
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
                    materializations=self.materializations_to_include,
                    max_chained_views=self.max_chained_views,
                    models=self.models,
                    model_unique_ids_to_check=[self.model.unique_id],
                    package_name=self.manifest_obj.manifest.metadata.project_name,
                ),
            )
            == 0
        ), f"`{self.model.name}` has more than {self.max_chained_views} upstream dependents that are not tables."


class CheckModelMaxFanout(BaseCheck):
    """Models cannot have more than the specified number of downstream models.

    Parameters:
        max_downstream_models (Optional[int]): The maximum number of permitted downstream models.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    max_downstream_models: int = Field(default=3)
    model: "DbtBouncerModelBase" = Field(default=None)
    models: List["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_model_max_fanout"]

    def execute(self) -> None:
        """Execute the check."""
        num_downstream_models = sum(
            self.model.unique_id in m.depends_on.nodes for m in self.models
        )

        assert (
            num_downstream_models <= self.max_downstream_models
        ), f"`{self.model.name}` has {num_downstream_models} downstream models, which is more than the permitted maximum of {self.max_downstream_models}."


class CheckModelMaxNumberOfLines(BaseCheck):
    """Models may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_max_number_of_lines"]
    max_number_of_lines: int = Field(default=100)

    def execute(self) -> None:
        """Execute the check."""
        actual_number_of_lines = self.model.raw_code.count("\n") + 1

        assert (
            actual_number_of_lines <= self.max_number_of_lines
        ), f"`{self.model.name}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."


class CheckModelMaxUpstreamDependencies(BaseCheck):
    """Limit the number of upstream dependencies a model has.

    Parameters:
        max_upstream_macros (Optional[int]): The maximum number of permitted upstream macros.
        max_upstream_models (Optional[int]): The maximum number of permitted upstream models.
        max_upstream_sources (Optional[int]): The maximum number of permitted upstream sources.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    max_upstream_macros: int = Field(
        default=5,
    )
    max_upstream_models: int = Field(
        default=5,
    )
    max_upstream_sources: int = Field(
        default=1,
    )
    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_max_upstream_dependencies"]

    def execute(self) -> None:
        """Execute the check."""
        num_upstream_macros = len(list(self.model.depends_on.macros))
        num_upstream_models = len(
            [m for m in self.model.depends_on.nodes if m.split(".")[0] == "model"],
        )
        num_upstream_sources = len(
            [m for m in self.model.depends_on.nodes if m.split(".")[0] == "source"],
        )

        assert (
            num_upstream_macros <= self.max_upstream_macros
        ), f"`{self.model.name}` has {num_upstream_macros} upstream macros, which is more than the permitted maximum of {self.max_upstream_macros}."
        assert (
            num_upstream_models <= self.max_upstream_models
        ), f"`{self.model.name}` has {num_upstream_models} upstream models, which is more than the permitted maximum of {self.max_upstream_models}."
        assert (
            num_upstream_sources <= self.max_upstream_sources
        ), f"`{self.model.name}` has {num_upstream_sources} upstream sources, which is more than the permitted maximum of {self.max_upstream_sources}."


class CheckModelNames(BaseCheck):
    """Models must have a name that matches the supplied regex.

    Parameters:
        model_name_pattern (str): Regexp the model name must match.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_names"]
    model_name_pattern: str

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.model_name_pattern.strip()).match(self.model.name)
            is not None
        ), f"`{self.model.name}` does not match the supplied regex `{self.model_name_pattern.strip()})`."


class CheckModelPropertyFileLocation(BaseCheck):
    """Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

    Parameters:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

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

    model: "DbtBouncerModelBase" = Field(default=None)
    name: Literal["check_model_property_file_location"]

    def execute(self) -> None:
        """Execute the check."""
        assert (  # noqa: PT018
            hasattr(self.model, "patch_path")
            and clean_path_str(self.model.patch_path) is not None
        ), f"`{self.model.name}` is not documented."

        expected_substr = (
            "_".join(clean_path_str(self.model.original_file_path).split("/")[1:-1])
            .replace("staging", "stg")
            .replace("intermediate", "int")
            .replace("marts", "")
        )
        properties_yml_name = clean_path_str(self.model.patch_path).split("/")[-1]

        assert properties_yml_name.startswith(
            "_",
        ), f"The properties file for `{self.model.name}` (`{properties_yml_name}`) does not start with an underscore."
        assert (
            expected_substr in properties_yml_name
        ), f"The properties file for `{self.model.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
        assert properties_yml_name.endswith(
            "__models.yml",
        ), f"The properties file for `{self.model.name}` (`{properties_yml_name}`) does not end with `__models.yml`."


class CheckModelsTestCoverage(BaseModel):
    """Set the minimum percentage of models that have at least one test.

    Parameters:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test.
        models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        tests (List[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

    Other Parameters:
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.


    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_test_coverage
              min_model_test_coverage_pct: 90
        ```

    """

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
    models: List["DbtBouncerModelBase"] = Field(default=[])
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )
    tests: List["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check."""
        num_models = len(self.models)
        models_with_tests = []
        for model in self.models:
            for test in self.tests:
                if model.unique_id in test.depends_on.nodes:
                    models_with_tests.append(model.unique_id)
        num_models_with_tests = len(set(models_with_tests))
        model_test_coverage_pct = (num_models_with_tests / num_models) * 100

        assert (
            model_test_coverage_pct >= self.min_model_test_coverage_pct
        ), f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {self.min_model_test_coverage_pct}%."
