import re
from typing import Literal

import pytest
from pydantic import Field

from dbt_bouncer.conf_validator_base import BaseCheck
from dbt_bouncer.utils import get_check_inputs


class CheckMacroArgumentsDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_arguments_description_populated"]


@pytest.mark.iterate_over_macros
def check_macro_arguments_description_populated(request, check_config=None, macro=None) -> None:
    """
    Macro arguments must have a populated description.
    """

    macro = get_check_inputs(check_config=check_config, macro=macro, request=request)["macro"]
    for arg in macro["arguments"]:
        assert (
            len(arg["description"].strip()) > 4
        ), f"Argument `{arg['name']}` in {macro['unique_id'].split('.')[-1]} does not have a populated description."


class CheckMacroCodeDoesNotContainRegexpPattern(BaseCheck):
    name: Literal["check_macro_code_does_not_contain_regexp_pattern"]
    regexp_pattern: str = Field(
        description="The regexp pattern that should not be matched by the macro code."
    )


@pytest.mark.iterate_over_macros
def check_macro_code_does_not_contain_regexp_pattern(request, check_config=None, macro=None):
    """
    The raw code for a macro must not match the specified regexp pattern.
    """

    input_vars = get_check_inputs(check_config=check_config, macro=macro, request=request)
    check_config = input_vars["check_config"]
    macro = input_vars["macro"]

    assert (
        re.compile(check_config["regexp_pattern"].strip(), flags=re.DOTALL).match(
            macro["macro_sql"]
        )
        is None
    ), f"`{macro['unique_id'].split('.')[-1]}` contains a banned string: `{check_config['regexp_pattern'].strip()}`."


class CheckMacroDescriptionPopulated(BaseCheck):
    name: Literal["check_macro_description_populated"]


@pytest.mark.iterate_over_macros
def check_macro_description_populated(request, check_config=None, macro=None) -> None:
    """
    Macros must have a populated description.
    """

    macro = get_check_inputs(check_config=check_config, macro=macro, request=request)["macro"]
    assert (
        len(macro["description"].strip()) > 4
    ), f"`{macro['unique_id'].split('.')[-1]}` does not have a populated description."


class CheckMacroNameMatchesFileName(BaseCheck):
    name: Literal["check_macro_name_matches_file_name"]


@pytest.mark.iterate_over_macros
def check_macro_name_matches_file_name(request, check_config=None, macro=None) -> None:
    """
    Macros names must be the same as the file they are contained in.

    Generic tests are also macros, however to document these tests the "name" value must be precededed with "test_".
    """

    macro = get_check_inputs(macro=macro, request=request)["macro"]
    if macro["name"].startswith("test_"):
        assert (
            macro["name"][5:] == macro["path"].split("/")[-1].split(".")[0]
        ), f"{macro['unique_id']} is not in a file named `{macro['name'][5:]}.sql`."
    else:
        assert (
            macro["name"] == macro["path"].split("/")[-1].split(".")[0]
        ), f"`{macro['unique_id'].split('.')[-1]}` is not in a file of the same name."


class CheckMacroPropertyFileLocation(BaseCheck):
    name: Literal["check_macro_property_file_location"]


@pytest.mark.iterate_over_macros
def check_macro_property_file_location(request, macro=None):
    """
    Macro properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project#how-we-use-the-other-folders).
    """

    macro = get_check_inputs(macro=macro, request=request)["macro"]

    expected_substr = "_".join(macro["path"][6:].split("/")[:-1])
    properties_yml_name = macro["patch_path"].split("/")[-1]

    if macro["path"].startswith("tests/"):  # Do not check generic tests (which are also macros)
        pass
    elif expected_substr == "":  # i.e. macro in ./macros
        assert (
            properties_yml_name == "_macros.yml"
        ), f"The properties file for `{macro['name']}` (`{properties_yml_name}`) should be `_macros.yml`."
    else:
        assert properties_yml_name.startswith(
            "_"
        ), f"The properties file for `{macro['name']}` (`{properties_yml_name}`) does not start with an underscore."
        assert (
            expected_substr in properties_yml_name
        ), f"The properties file for `{macro['name']}` (`{properties_yml_name}`) does not contain the expected substring (`{expected_substr}`)."
        assert properties_yml_name.endswith(
            "__macros.yml"
        ), f"The properties file for `{macro['name'].split('.')[-1]}` (`{properties_yml_name}`) does not end with `__macros.yml`."
