# mypy: disable-error-code="union-attr"

import re
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import Field

from dbt_bouncer.utils import clean_path_str

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Macros


import jinja2
from jinja2_simple_tags import StandaloneTag

from dbt_bouncer.check_base import BaseCheck


class TagExtension(StandaloneTag):
    tags: ClassVar = {"do", "endmaterialization", "endtest", "materialization", "test"}


class CheckMacroArgumentsDescriptionPopulated(BaseCheck):
    """Macro arguments must have a populated description.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_arguments_description_populated
        ```
        ```yaml
        # Only "common" macros need to have their arguments populated
        manifest_checks:
            - name: check_macro_arguments_description_populated
              include: ^macros/common
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_arguments_description_populated"]

    def execute(self) -> None:
        """Execute the check."""
        environment = jinja2.Environment(autoescape=True, extensions=[TagExtension])
        ast = environment.parse(self.macro.macro_sql)

        if hasattr(ast.body[0], "args"):
            # Assume macro is a "true" macro
            macro_arguments = [a.name for a in ast.body[0].args]
        else:
            if "materialization" in [
                x.value.value
                for x in ast.body[0].nodes[0].kwargs  # type: ignore[attr-defined]
                if isinstance(x.value, jinja2.nodes.Const)
            ]:
                # Materializations don't have arguments
                macro_arguments = []
            else:
                # Macro is a test
                test_macro = next(
                    x
                    for x in ast.body
                    if not isinstance(x.nodes[0], jinja2.nodes.Call)  # type: ignore[attr-defined]
                )
                macro_arguments = [
                    x.name
                    for x in test_macro.nodes  # type: ignore[attr-defined]
                    if isinstance(x, jinja2.nodes.Name)
                ]

        # macro_arguments: List of args parsed from macro SQL
        # macro.arguments: List of args manually added to the properties file

        non_complying_args = []
        for arg in macro_arguments:
            macro_doc_raw = [x for x in self.macro.arguments if x.name == arg]
            if macro_doc_raw == [] or (
                arg not in [x.name for x in self.macro.arguments]
                or len(macro_doc_raw[0].description.strip()) <= 4
            ):
                non_complying_args.append(arg)

        assert (
            non_complying_args == []
        ), f"Macro `{self.macro.name}` does not have a populated description for the following argument(s): {non_complying_args}."


class CheckMacroCodeDoesNotContainRegexpPattern(BaseCheck):
    """The raw code for a macro must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the macro code.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_macro_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str

    def execute(self) -> None:
        """Execute the check."""
        assert (
            re.compile(self.regexp_pattern.strip(), flags=re.DOTALL).match(
                self.macro.macro_sql
            )
            is None
        ), f"Macro `{self.macro.name}` contains a banned string: `{self.regexp_pattern.strip()}`."


class CheckMacroDescriptionPopulated(BaseCheck):
    """Macros must have a populated description.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_description_populated
        ```
        ```yaml
        # Only "common" macros need to have a populated description
        manifest_checks:
            - name: check_macro_description_populated
              include: ^macros/common
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_description_populated"]

    def execute(self) -> None:
        """Execute the check."""
        assert (
            len(self.macro.description.strip()) > 4
        ), f"Macro `{self.macro.name}` does not have a populated description."


class CheckMacroMaxNumberOfLines(BaseCheck):
    """Macros may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_max_number_of_lines
        ```
        ```yaml
        manifest_checks:
            - name: check_macro_max_number_of_lines
              max_number_of_lines: 100
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_max_number_of_lines"]
    max_number_of_lines: int = Field(default=50)

    def execute(self) -> None:
        """Execute the check."""
        actual_number_of_lines = self.macro.macro_sql.count("\n") + 1

        assert (
            actual_number_of_lines <= self.max_number_of_lines
        ), f"Macro `{self.macro.name}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."


class CheckMacroNameMatchesFileName(BaseCheck):
    """Macros names must be the same as the file they are contained in.

    Generic tests are also macros, however to document these tests the "name" value must be preceded with "test_".

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_name_matches_file_name
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_name_matches_file_name"]

    def execute(self) -> None:
        """Execute the check."""
        if self.macro.name.startswith("test_"):
            assert (
                self.macro.name[5:]
                == clean_path_str(self.macro.original_file_path)
                .split("/")[-1]
                .split(".")[0]
            ), f"Macro `{self.macro.unique_id}` is not in a file named `{self.macro.name[5:]}.sql`."
        else:
            assert (
                self.macro.name
                == clean_path_str(self.macro.original_file_path)
                .split("/")[-1]
                .split(".")[0]
            ), f"Macro `{self.macro.name}` is not in a file of the same name."


class CheckMacroPropertyFileLocation(BaseCheck):
    """Macro properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project#how-we-use-the-other-folders).

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Optional[Literal["error", "warn"]]): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_property_file_location
        ```

    """

    macro: "Macros" = Field(default=None)
    name: Literal["check_macro_property_file_location"]

    def execute(self) -> None:
        """Execute the check."""
        expected_substr = "_".join(
            clean_path_str(self.macro.original_file_path)[6:].split("/")[:-1]
        )

        assert (
            clean_path_str(self.macro.patch_path) is not None
        ), f"Macro `{self.macro.name}` is not defined in a `.yml` properties file."
        properties_yml_name = clean_path_str(self.macro.patch_path).split("/")[-1]

        if clean_path_str(self.macro.original_file_path).startswith(
            "tests/",
        ):  # Do not check generic tests (which are also macros)
            pass
        elif expected_substr == "":  # i.e. macro in ./macros
            assert (
                properties_yml_name == "_macros.yml"
            ), f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) should be `_macros.yml`."
        else:
            assert properties_yml_name.startswith(
                "_",
            ), f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) does not start with an underscore."
            assert (
                expected_substr in properties_yml_name
            ), f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
            assert properties_yml_name.endswith(
                "__macros.yml",
            ), f"The properties file for `{self.macro.name.name}` (`{properties_yml_name}`) does not end with `__macros.yml`."
