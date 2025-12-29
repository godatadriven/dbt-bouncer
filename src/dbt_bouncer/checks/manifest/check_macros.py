import re
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import Field

from dbt_bouncer.utils import clean_path_str

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Macros


from jinja2 import Environment, nodes
from jinja2_simple_tags import StandaloneTag

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError


class TagExtension(StandaloneTag):
    tags: ClassVar = {"do", "endmaterialization", "endtest", "materialization", "test"}


class CheckMacroArgumentsDescriptionPopulated(BaseCheck):
    """Macro arguments must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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
        ```yaml
        manifest_checks:
            - name: check_macro_arguments_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    min_description_length: int | None = Field(default=None)
    macro: "Macros | None" = Field(default=None)
    name: Literal["check_macro_arguments_description_populated"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If macro arguments are not populated.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        environment = Environment(autoescape=True, extensions=[TagExtension])
        ast = environment.parse(self.macro.macro_sql)

        if hasattr(ast.body[0], "args"):
            # Assume macro is a "true" macro
            macro_arguments = [a.name for a in getattr(ast.body[0], "args", [])]
        else:
            if "materialization" in [
                x.value.value
                for x in ast.body[0].nodes[0].kwargs  # type: ignore[attr-defined]
                if isinstance(x.value, nodes.Const)
            ]:
                # Materializations don't have arguments
                macro_arguments = []
            else:
                # Macro is a test
                test_macro = next(
                    x
                    for x in ast.body
                    if not isinstance(x.nodes[0], nodes.Call)  # type: ignore[attr-defined]
                )
                macro_arguments = [
                    x.name
                    for x in test_macro.nodes  # type: ignore[attr-defined]
                    if isinstance(x, nodes.Name)
                ]

        # macro_arguments: List of args parsed from macro SQL
        # macro.arguments: List of args manually added to the properties file

        non_complying_args = []
        if self.macro.arguments:
            for arg in macro_arguments:
                macro_doc_raw = [x for x in self.macro.arguments if x.name == arg]
                if macro_doc_raw == [] or (
                    arg not in [x.name for x in self.macro.arguments]
                    or not self._is_description_populated(
                        str(macro_doc_raw[0].description or ""),
                        self.min_description_length,
                    )
                ):
                    non_complying_args.append(arg)

        if non_complying_args != []:
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` does not have a populated description for the following argument(s): {non_complying_args}."
            )


class CheckMacroCodeDoesNotContainRegexpPattern(BaseCheck):
    """The raw code for a macro must not match the specified regexp pattern.

    Parameters:
        regexp_pattern (str): The regexp pattern that should not be matched by the macro code.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: [https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02](https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02)
            - name: check_macro_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```

    """

    macro: "Macros | None" = Field(default=None)
    name: Literal["check_macro_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If macro code contains banned string.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        if (
            re.compile(self.regexp_pattern.strip(), flags=re.DOTALL).match(
                self.macro.macro_sql
            )
            is not None
        ):
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` contains a banned string: `{self.regexp_pattern.strip()}`."
            )


class CheckMacroDescriptionPopulated(BaseCheck):
    """Macros must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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

    macro: "Macros | None" = Field(default=None)
    min_description_length: int | None = Field(default=None)
    name: Literal["check_macro_description_populated"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If macro description is not populated.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        if not self._is_description_populated(
            str(self.macro.description or ""), self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` does not have a populated description."
            )


class CheckMacroMaxNumberOfLines(BaseCheck):
    """Macros may not have more than the specified number of lines.

    Parameters:
        max_number_of_lines (int): The maximum number of permitted lines.

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

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

    macro: "Macros | None" = Field(default=None)
    name: Literal["check_macro_max_number_of_lines"]
    max_number_of_lines: int = Field(default=50)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If max lines exceeded.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        actual_number_of_lines = self.macro.macro_sql.count("\n") + 1

        if actual_number_of_lines > self.max_number_of_lines:
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` has {actual_number_of_lines} lines, this is more than the maximum permitted number of lines ({self.max_number_of_lines})."
            )


class CheckMacroNameMatchesFileName(BaseCheck):
    """Macros names must be the same as the file they are contained in.

    Generic tests are also macros, however to document these tests the "name" value must be preceded with "test_".

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_name_matches_file_name
        ```

    """

    macro: "Macros | None" = Field(default=None)
    name: Literal["check_macro_name_matches_file_name"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If macro name does not match file name.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        file_path = Path(clean_path_str(self.macro.original_file_path))
        file_stem = file_path.stem

        if self.macro.name.startswith("test_"):
            if self.macro.name[5:] != file_stem:
                raise DbtBouncerFailedCheckError(
                    f"Macro `{self.macro.unique_id}` is not in a file named `{self.macro.name[5:]}.sql`."
                )
        else:
            if self.macro.name != file_stem:
                raise DbtBouncerFailedCheckError(
                    f"Macro `{self.macro.name}` is not in a file of the same name."
                )


class CheckMacroPropertyFileLocation(BaseCheck):
    """Macro properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project#how-we-use-the-other-folders).

    Receives:
        macro (Macros): The Macros object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_property_file_location
        ```

    """

    macro: "Macros | None" = Field(default=None)
    name: Literal["check_macro_property_file_location"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If property file location is incorrect.

        """
        if self.macro is None:
            raise DbtBouncerFailedCheckError("self.macro is None")
        original_path = Path(clean_path_str(self.macro.original_file_path))

        # Logic matches previous manual splitting:
        # If path is `macros/utils/file.sql`, we want `_utils`.
        # We assume the first part of the path is the root (e.g. 'macros' or 'tests').
        subdir_parts = original_path.parent.parts[1:]
        expected_substr = "_" + "_".join(subdir_parts) if subdir_parts else ""

        if self.macro.patch_path is None:
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` is not defined in a `.yml` properties file."
            )
        clean_patch_path = clean_path_str(self.macro.patch_path)
        if clean_patch_path is None:
            raise DbtBouncerFailedCheckError(
                f"Macro `{self.macro.name}` has an invalid patch path."
            )

        patch_path = Path(clean_patch_path)
        properties_yml_name = patch_path.name

        if original_path.parts[0] == "tests":
            # Do not check generic tests (which are also macros)
            pass
        elif expected_substr == "":  # i.e. macro in ./macros
            if properties_yml_name != "_macros.yml":
                raise DbtBouncerFailedCheckError(
                    f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) should be `_macros.yml`."
                )
        else:
            if not properties_yml_name.startswith("_"):
                raise DbtBouncerFailedCheckError(
                    f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) does not start with an underscore."
                )
            if expected_substr not in properties_yml_name:
                raise DbtBouncerFailedCheckError(
                    f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
                )
            if not properties_yml_name.endswith("__macros.yml"):
                raise DbtBouncerFailedCheckError(
                    f"The properties file for `{self.macro.name}` (`{properties_yml_name}`) does not end with `__macros.yml`."
                )
