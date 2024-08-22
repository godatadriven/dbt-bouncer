# mypy: disable-error-code="union-attr"

import re
import warnings
from typing import Literal, Union

import pytest
from _pytest.fixtures import TopRequest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Macros

import jinja2
from jinja2_simple_tags import StandaloneTag

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import bouncer_check


class TagExtension(StandaloneTag):
    tags = {"endtest", "test"}


class CheckMacroArgumentsDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_arguments_description_populated"]


@pytest.mark.iterate_over_macros
@bouncer_check
def check_macro_arguments_description_populated(
    request: TopRequest, macro: Union[Macros, None] = None, **kwargs
) -> None:
    """
    Macro arguments must have a populated description.

    Receives:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        macro (Macros): The Macro object to check.

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

    environment = jinja2.Environment(extensions=[TagExtension])
    ast = environment.parse(macro.macro_sql)

    # Assume macro is a "true" macro, if not see if it's a generic test
    try:
        macro_arguments = [a.name for a in ast.body[0].args]  # type: ignore[attr-defined]
    except AttributeError:
        test_macro = [x for x in ast.body if not isinstance(x.nodes[0], jinja2.nodes.Call)][0]  # type: ignore[attr-defined]
        macro_arguments = [x.name for x in test_macro.nodes if isinstance(x, jinja2.nodes.Name)]  # type: ignore[attr-defined]

    # macro_arguments: List of args parsed from macro SQL
    # macro.arguments: List of args manually added to the properties file

    non_complying_args = []
    for arg in macro_arguments:
        macro_doc_raw = [x for x in macro.arguments if x.name == arg]
        if macro_doc_raw == []:
            non_complying_args.append(arg)
        elif (
            arg not in [x.name for x in macro.arguments]
            or len(macro_doc_raw[0].description.strip()) <= 4
        ):
            non_complying_args.append(arg)

    assert (
        non_complying_args == []
    ), f"Macro `{macro.name}` does not have a populated description for the following argument(s): {non_complying_args}."


class CheckMacroCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_macro_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str


@pytest.mark.iterate_over_macros
@bouncer_check
def check_macro_code_does_not_contain_regexp_pattern(
    request: TopRequest,
    macro: Union[Macros, None] = None,
    regexp_pattern: Union[None, str] = None,
    **kwargs,
) -> None:
    """The raw code for a macro must not match the specified regexp pattern.

    Receives:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        macro (Macros): The Macro object to check.
        regexp_pattern (str): The regexp pattern that should not be matched by the macro code.

    Example(s):
        ```yaml
        manifest_checks:
            # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
            - name: check_macro_code_does_not_contain_regexp_pattern
              regexp_pattern: .*[i][f][n][u][l][l].*
        ```
    """

    assert (
        re.compile(regexp_pattern.strip(), flags=re.DOTALL).match(macro.macro_sql) is None
    ), f"Macro `{macro.name}` contains a banned string: `{regexp_pattern.strip()}`."


class CheckMacroDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_description_populated"]


@pytest.mark.iterate_over_macros
@bouncer_check
def check_macro_description_populated(
    request: TopRequest, macro: Union[Macros, None] = None, **kwargs
) -> None:
    """
    Macros must have a populated description.

    Receives:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        macro (Macros): The Macro object to check.

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

    assert (
        len(macro.description.strip()) > 4
    ), f"Macro `{macro.name}` does not have a populated description."


class CheckMacroNameMatchesFileName(BaseCheck):
    name: Literal["check_macro_name_matches_file_name"]


@pytest.mark.iterate_over_macros
@bouncer_check
def check_macro_name_matches_file_name(
    request: TopRequest, macro: Union[Macros, None] = None, **kwargs
) -> None:
    """
    Macros names must be the same as the file they are contained in.

    Generic tests are also macros, however to document these tests the "name" value must be precededed with "test_".

    Receives:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        macro (Macros): The Macro object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_name_matches_file_name
        ```
    """

    if macro.name.startswith("test_"):
        assert (
            macro.name[5:] == macro.path.split("/")[-1].split(".")[0]
        ), f"Macro `{macro.unique_id}` is not in a file named `{macro.name[5:]}.sql`."
    else:
        assert (
            macro.name == macro.path.split("/")[-1].split(".")[0]
        ), f"Macro `{macro.name}` is not in a file of the same name."


class CheckMacroPropertyFileLocation(BaseCheck):
    name: Literal["check_macro_property_file_location"]


@pytest.mark.iterate_over_macros
@bouncer_check
def check_macro_property_file_location(
    request: TopRequest, macro: Union[Macros, None] = None, **kwargs
) -> None:
    """
    Macro properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project#how-we-use-the-other-folders).

    Receives:
        exclude (Optional[str]): Regex pattern to match the macro path. Macro paths that match the pattern will not be checked.
        include (Optional[str]): Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
        macro (Macros): The Macro object to check.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_macro_property_file_location
        ```
    """

    expected_substr = "_".join(macro.path[6:].split("/")[:-1])
    properties_yml_name = macro.patch_path.split("/")[-1]

    if macro.path.startswith("tests/"):  # Do not check generic tests (which are also macros)
        pass
    elif expected_substr == "":  # i.e. macro in ./macros
        assert (
            properties_yml_name == "_macros.yml"
        ), f"The properties file for `{macro.name}` (`{properties_yml_name}`) should be `_macros.yml`."
    else:
        assert properties_yml_name.startswith(
            "_"
        ), f"The properties file for `{macro.name}` (`{properties_yml_name}`) does not start with an underscore."
        assert (
            expected_substr in properties_yml_name
        ), f"The properties file for `{macro.name}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
        assert properties_yml_name.endswith(
            "__macros.yml"
        ), f"The properties file for `{macro.name.name}` (`{properties_yml_name}`) does not end with `__macros.yml`."
