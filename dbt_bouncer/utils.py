def flatten(structure, key="", path="", flattened=None):
    """
    Take a dict of arbitrary depth that may contain lists and return a non-nested dict of all pathways.
    """

    if flattened is None:
        flattened = {}
    if type(structure) not in (dict, list):
        flattened[((path + ">") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, path + ">" + key, flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, path + ">" + key, flattened)
    return flattened


from dbt_bouncer.logger import logger


def get_check_inputs(
    check_config=None, macro=None, mode="check", model=None, request=None, source=None
):
    """
    Helper function that is used to account for the difference in how arguments are passed to check functions
    when they are run by `dbt-bouncer` and when they are called by pytest.
    """

    if request is not None:
        check_config = request.node.check_config
        macro = request.node.macro
        model = request.node.model
        source = request.node.source
    else:
        check_config = check_config
        macro = macro
        model = model
        source = source

    return check_config, macro, model, source
