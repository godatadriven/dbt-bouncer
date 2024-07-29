import pytest

from dbt_bouncer.utils import get_check_inputs


@pytest.mark.iterate_over_macros
def check_macro_arguments_description_populated(request, check_config=None, macro=None) -> None:
    """
    Macro arguments must have a populated description.
    """

    macro = get_check_inputs(check_config=check_config, macro=macro, request=request)["macro"]
    for arg in macro["arguments"]:
        assert (
            len(arg["description"].strip()) > 4
        ), f"Argument {arg['name']} in {macro['unique_id']} does not have a populated description."


@pytest.mark.iterate_over_macros
def check_macro_description_populated(request, check_config=None, macro=None) -> None:
    """
    Macros must have a populated description.
    """

    macro = get_check_inputs(check_config=check_config, macro=macro, request=request)["macro"]
    assert (
        len(macro["description"].strip()) > 4
    ), f"{macro['unique_id']} does not have a populated description."


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
        ), f"{macro['unique_id']} is not in a file of the same name."
