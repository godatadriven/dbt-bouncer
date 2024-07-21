import re


def check_project_name(manifest_obj, request, check_config=None):
    """
    Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.
    """

    check_config = request.node.check_config if check_config is None else check_config

    assert (
        re.compile(check_config["project_name_pattern"].strip()).match(
            manifest_obj.metadata.project_name
        )
        is not None
    ), f"Project name (`{manifest_obj.metadata.project_name}`) does not conform to the supplied regex `({check_config['project_name_pattern'].strip()})`."