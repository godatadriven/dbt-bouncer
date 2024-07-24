import pytest

from dbt_bouncer.utils import get_check_inputs


@pytest.mark.iterate_over_macros
def check_macro_name_matches_file_name(request, check_config=None, macro=None) -> None:
    """
    Macros names must be the same as the file they are contained in.
    """

    check_config, macro, _, _ = get_check_inputs(
        check_config=check_config, macro=macro, request=request
    )
    assert (
        macro["name"] == macro["path"].split("/")[-1].split(".")[0]
    ), f"{macro['unique_id']} is not in a file of the same name."


@pytest.mark.iterate_over_macros
def check_populated_macro_arguments_description(request, check_config=None, macro=None) -> None:
    """
    Macro arguments must have a populated description.
    """

    check_config, macro, _, _ = get_check_inputs(
        check_config=check_config, macro=macro, request=request
    )
    for arg in macro["arguments"]:
        assert (
            len(arg["description"].strip()) > 4
        ), f"Argument {arg['name']} in {macro['unique_id']} does not have a populated description."


@pytest.mark.iterate_over_macros
def check_populated_macro_description(request, check_config=None, macro=None) -> None:
    """
    Macros must have a populated description.
    """

    check_config, macro, _, _ = get_check_inputs(
        check_config=check_config, macro=macro, request=request
    )
    assert (
        len(macro["description"].strip()) > 4
    ), f"{macro['unique_id']} does not have a populated description."
