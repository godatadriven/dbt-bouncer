from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check(
    "check_project_name",
    params={
        "package_name": (str | None, None),
        "project_name_pattern": str,
    },
)
def check_project_name(ctx, *, package_name: str | None, project_name_pattern: str):
    """Enforce that the name of the dbt project matches a supplied regex."""
    compiled_project_name_pattern = compile_pattern(project_name_pattern.strip())
    manifest_obj = ctx.manifest_obj

    resolved_package_name = package_name or manifest_obj.manifest.metadata.project_name
    if compiled_project_name_pattern.match(str(resolved_package_name)) is None:
        fail(
            f"Project name (`{resolved_package_name}`) does not conform to the supplied regex `({project_name_pattern.strip()})`."
        )
