from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check
def check_project_name(
    ctx, *, package_name: str | None = None, project_name_pattern: str
):
    """Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

    !!! info "Rationale"

        In organisations running multiple dbt projects, consistent project naming makes it easy to identify which team or domain owns a project, automate governance policies, and set up CI/CD pipelines that apply different rules to different project types. Without a naming convention, projects can accumulate with arbitrary names that make ownership and purpose ambiguous.

    Parameters:
        project_name_pattern (str): Regex pattern to match the project name.

    Receives:
        manifest_obj (ManifestObject): The manifest object.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_project_name
              project_name_pattern: ^awesome_company_
        ```

    """
    compiled_project_name_pattern = compile_pattern(project_name_pattern.strip())
    manifest_obj = ctx.manifest_obj

    resolved_package_name = package_name or manifest_obj.manifest.metadata.project_name
    if compiled_project_name_pattern.match(str(resolved_package_name)) is None:
        fail(
            f"Project name (`{resolved_package_name}`) does not conform to the supplied regex `({project_name_pattern.strip()})`."
        )
