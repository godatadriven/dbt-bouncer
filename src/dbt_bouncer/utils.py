# mypy: disable-error-code="union-attr"

import contextlib
import os
import re
import warnings
from functools import wraps
from pathlib import Path
from typing import Any, List, Mapping, Union

import toml
import yaml

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures

from dbt_bouncer.logger import logger
from dbt_bouncer.parsers import (
    DbtBouncerManifest,
    DbtBouncerModel,
    DbtBouncerSource,
    DbtBouncerTest,
)


def bouncer_check(func):
    @wraps(func)
    def wrapper(
        exposures: Union[List[Exposures], None] = None,
        manifest_obj: Union[DbtBouncerManifest, None] = None,
        models: Union[List[DbtBouncerModel], None] = None,
        sources: Union[List[DbtBouncerSource], None] = None,
        tests: Union[List[DbtBouncerTest], None] = None,
        **kwargs,
    ):
        request = kwargs.get("request")
        if request is not None:
            # From provided check_config
            accepted_uniqueness_tests = request.node.check_config.get("accepted_uniqueness_tests")
            access = request.node.check_config.get("access")
            column_name_pattern = request.node.check_config.get("column_name_pattern")
            exclude = request.node.check_config.get("exclude")
            include = request.node.check_config.get("include")
            keys = request.node.check_config.get("keys")
            materializations_to_include = request.node.check_config.get(
                "materializations_to_include"
            )
            max_chained_views = request.node.check_config.get("max_chained_views")
            max_downstream_models = request.node.check_config.get("max_downstream_models")
            max_execution_time_seconds = request.node.check_config.get(
                "max_execution_time_seconds"
            )
            max_gigabytes_billed = request.node.check_config.get("max_gigabytes_billed")
            max_upstream_macros = request.node.check_config.get("max_upstream_macros")
            max_upstream_models = request.node.check_config.get("max_upstream_models")
            max_upstream_sources = request.node.check_config.get("max_upstream_sources")
            min_model_documentation_coverage_pct = request.node.check_config.get(
                "min_model_documentation_coverage_pct"
            )
            min_model_test_coverage_pct = request.node.check_config.get(
                "min_model_test_coverage_pct"
            )
            model_name_pattern = request.node.check_config.get("model_name_pattern")
            permitted_sub_directories = request.node.check_config.get("permitted_sub_directories")
            project_name_pattern = request.node.check_config.get("project_name_pattern")
            regexp_pattern = request.node.check_config.get("regexp_pattern")
            source_name_pattern = request.node.check_config.get("source_name_pattern")
            tags = request.node.check_config.get("tags")
            test_name = request.node.check_config.get("test_name")
            types = request.node.check_config.get("types")
            upstream_path_pattern = request.node.check_config.get("upstream_path_pattern")

            # Variables from `iterate_over_*` markers
            catalog_node = getattr(request.node.catalog_node, "node", lambda: None)
            catalog_source = getattr(request.node.catalog_source, "node", lambda: None)
            exposure = request.node.exposure
            macro = request.node.macro
            model = getattr(request.node.model, "model", lambda: None)
            run_result = getattr(request.node.run_result, "result", lambda: None)
            source = getattr(request.node.source, "source", lambda: None)
        else:
            # From provided check_config
            accepted_uniqueness_tests = kwargs.get("accepted_uniqueness_tests")
            access = kwargs.get("access")
            column_name_pattern = kwargs.get("column_name_pattern")
            include = kwargs.get("include")
            exclude = kwargs.get("exclude")
            keys = kwargs.get("keys")
            materializations_to_include = kwargs.get("materializations_to_include")
            max_chained_views = kwargs.get("max_chained_views")
            max_downstream_models = kwargs.get("max_downstream_models")
            max_execution_time_seconds = kwargs.get("max_execution_time_seconds")
            max_gigabytes_billed = kwargs.get("max_gigabytes_billed")
            max_upstream_macros = kwargs.get("max_upstream_macros")
            max_upstream_models = kwargs.get("max_upstream_models")
            max_upstream_sources = kwargs.get("max_upstream_sources")
            min_model_documentation_coverage_pct = kwargs.get(
                "min_model_documentation_coverage_pct"
            )
            min_model_test_coverage_pct = kwargs.get("min_model_test_coverage_pct")
            model_name_pattern = kwargs.get("model_name_pattern")
            permitted_sub_directories = kwargs.get("permitted_sub_directories")
            project_name_pattern = kwargs.get("project_name_pattern")
            regexp_pattern = kwargs.get("regexp_pattern")
            source_name_pattern = kwargs.get("source_name_pattern")
            tags = kwargs.get("tags")
            test_name = kwargs.get("test_name")
            types = kwargs.get("types")
            upstream_path_pattern = kwargs.get("upstream_path_pattern")

            # Variables from `iterate_over_*` markers
            catalog_node = kwargs.get("catalog_node")
            catalog_source = kwargs.get("catalog_source")
            exposure = kwargs.get("exposure")
            macro = kwargs.get("macro")
            model = kwargs.get("model")
            run_result = kwargs.get("run_result")
            source = kwargs.get("source")

        func(
            # From provided check_config
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            access=access,
            column_name_pattern=column_name_pattern,
            exclude=exclude,
            include=include,
            keys=keys,
            materializations_to_include=materializations_to_include,
            max_chained_views=max_chained_views,
            max_downstream_models=max_downstream_models,
            max_execution_time_seconds=max_execution_time_seconds,
            max_gigabytes_billed=max_gigabytes_billed,
            max_upstream_macros=max_upstream_macros,
            max_upstream_models=max_upstream_models,
            max_upstream_sources=max_upstream_sources,
            min_model_documentation_coverage_pct=min_model_documentation_coverage_pct,
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            model_name_pattern=model_name_pattern,
            permitted_sub_directories=permitted_sub_directories,
            project_name_pattern=project_name_pattern,
            regexp_pattern=regexp_pattern,
            request=request,
            source_name_pattern=source_name_pattern,
            tags=tags,
            test_name=test_name,
            types=types,
            upstream_path_pattern=upstream_path_pattern,
            # Variables from `iterate_over_*` markers
            catalog_node=catalog_node,
            catalog_source=catalog_source,
            exposure=exposure,
            model=model,
            macro=macro,
            run_result=run_result,
            source=source,
            # Fixtures from `FixturePlugin`
            exposures=exposures,
            manifest_obj=manifest_obj,
            models=models,
            sources=sources,
            tests=tests,
        )

    return wrapper


def create_github_comment_file(failed_checks: List[List[str]]) -> None:
    """
    Create a markdown file containing a comment for GitHub.
    """

    md_formatted_comment = make_markdown_table(
        [["Check name", "Failure message"]] + list(sorted(failed_checks))
    )

    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ.get('GITHUB_REPOSITORY',None)}/actions/runs/{os.environ.get('GITHUB_RUN_ID', None)})."  # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324

    logger.debug(f"{md_formatted_comment=}")

    if os.environ.get("GITHUB_REPOSITORY", None) is not None:
        comment_file = "/app/github-comment.md"
    else:
        comment_file = "github-comment.md"

    logger.info(f"Writing comment for GitHub to {comment_file}...")
    with open(comment_file, "w") as f:
        f.write(md_formatted_comment)


def find_missing_meta_keys(meta_config, required_keys) -> List[str]:
    """
    Find missing keys in a meta config.
    """

    keys_in_meta = list(flatten(meta_config).keys())

    # Get required keys and convert to a list
    required_keys = [
        re.sub(r"(\>{1}\d{1,10})", "", f"{k}>{v}") for k, v in flatten(required_keys).items()
    ]

    return [x for x in required_keys if x not in keys_in_meta]


def flatten(structure: Any, key: str = "", path: str = "", flattened=None):
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


def get_dbt_bouncer_config(config_file: str, config_file_source: str) -> Mapping[str, Any]:
    """
    Get the config for dbt-bouncer. This is fetched from (in order):
        1. The file passed via the `--config=file` CLI flag.
        2. A file named `dbt_bouncer.yml` in the current working directory.
        3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).
    """

    logger.debug(f"{config_file=}")
    logger.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logger.debug(f"Config file passed via command line: {config_file}")
        return load_config_from_yaml(Path(config_file))

    if config_file_source == "DEFAULT":
        logger.debug(f"Using default value for config file: {config_file}")
        with contextlib.suppress(FileNotFoundError):
            return load_config_from_yaml(Path.cwd() / config_file)

    # Read config from pyproject.toml
    logger.info("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / "pyproject.toml").exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (parent for parent in Path().cwd().parents if (parent / "pyproject.toml").exists()),
            None,  # type: ignore[arg-type]
        )  # i.e. look in parent directories for a pyproject.toml file

    if pyproject_toml_dir is None:
        logger.debug("No pyproject.toml found.")
        raise RuntimeError(
            "No pyproject.toml found. Please ensure you have a pyproject.toml file in the root of your project correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
        )
    else:
        logger.debug(f"{pyproject_toml_dir / 'pyproject.toml'=}")

        toml_cfg = toml.load(pyproject_toml_dir / "pyproject.toml")
        if "dbt-bouncer" in toml_cfg["tool"].keys():
            conf = [v for k, v in toml_cfg["tool"].items() if k == "dbt-bouncer"][0]
        else:
            raise RuntimeError(
                "Please ensure your pyproject.toml file is correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
            )
    return conf


def load_config_from_yaml(config_file: Path) -> Mapping[str, Any]:

    config_path = Path().cwd() / config_file
    logger.debug(f"Loading config from {config_path}...")
    logger.debug(f"Loading config from {config_file}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_file}...")

    return conf


def make_markdown_table(array: List[List[str]]) -> str:
    """
    Transforms a list of lists into a markdown table. The first element is the header row.
    """

    nl = "\n"

    markdown = nl
    markdown += f"| {' | '.join(array[0])} |"

    markdown += nl
    markdown += f"| {' | '.join([':---']*len(array[0]))} |"

    markdown += nl
    for entry in array[1:]:
        markdown += f"| {' | '.join(entry)} |{nl}"

    return markdown


def object_in_path(include_pattern: str, path: str) -> bool:
    """
    Determine if an object is included in the specified path pattern.
    If no pattern is specified then all objects are included
    """

    if include_pattern is not None:
        return re.compile(include_pattern.strip()).match(path) is not None
    else:
        return True
