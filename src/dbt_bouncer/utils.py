# mypy: disable-error-code="union-attr"

import importlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._model_construction import ModelMetaclass
from typing_extensions import Annotated

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
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures, UnitTests

import logging

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
        unit_tests: Union[List[UnitTests], None] = None,
        **kwargs,
    ):
        request = kwargs.get("request")
        if request is not None:
            # From provided check_config
            accepted_uniqueness_tests = getattr(
                request.node.check_config, "accepted_uniqueness_tests", lambda: None
            )
            access = getattr(request.node.check_config, "access", lambda: None)
            column_name_pattern = getattr(
                request.node.check_config, "column_name_pattern", lambda: None
            )
            exclude = getattr(request.node.check_config, "exclude", lambda: None)
            include = getattr(request.node.check_config, "include", lambda: None)
            keys = getattr(request.node.check_config, "keys", lambda: None)
            materializations_to_include = getattr(
                request.node.check_config, "materializations_to_include", lambda: None
            )
            max_gigabytes_billed = getattr(
                request.node.check_config, "max_gigabytes_billed", lambda: None
            )
            max_number_of_lines = getattr(
                request.node.check_config, "max_number_of_lines", lambda: None
            )
            max_upstream_macros = getattr(
                request.node.check_config, "max_upstream_macros", lambda: None
            )
            max_upstream_models = getattr(
                request.node.check_config, "max_upstream_models", lambda: None
            )
            max_upstream_sources = getattr(
                request.node.check_config, "max_upstream_sources", lambda: None
            )
            min_model_documentation_coverage_pct = getattr(
                request.node.check_config, "min_model_documentation_coverage_pct", lambda: None
            )
            max_chained_views = getattr(
                request.node.check_config, "max_chained_views", lambda: None
            )
            max_downstream_models = getattr(
                request.node.check_config, "max_downstream_models", lambda: None
            )
            max_execution_time_seconds = getattr(
                request.node.check_config, "max_execution_time_seconds", lambda: None
            )
            max_gigabytes_billed = getattr(
                request.node.check_config, "max_gigabytes_billed", lambda: None
            )
            max_upstream_macros = getattr(
                request.node.check_config, "max_upstream_macros", lambda: None
            )
            max_upstream_models = getattr(
                request.node.check_config, "max_upstream_models", lambda: None
            )
            max_upstream_sources = getattr(
                request.node.check_config, "max_upstream_sources", lambda: None
            )
            min_model_documentation_coverage_pct = getattr(
                request.node.check_config, "min_model_documentation_coverage_pct", lambda: None
            )
            min_model_test_coverage_pct = getattr(
                request.node.check_config, "min_model_test_coverage_pct", lambda: None
            )
            min_number_of_unit_tests = getattr(
                request.node.check_config, "min_number_of_unit_tests", lambda: None
            )
            model_name_pattern = getattr(
                request.node.check_config, "model_name_pattern", lambda: None
            )
            permitted_formats = getattr(
                request.node.check_config, "permitted_formats", lambda: None
            )
            permitted_sub_directories = getattr(
                request.node.check_config, "permitted_sub_directories", lambda: None
            )
            project_name_pattern = getattr(
                request.node.check_config, "project_name_pattern", lambda: None
            )
            regexp_pattern = getattr(request.node.check_config, "regexp_pattern", lambda: None)
            source_name_pattern = getattr(
                request.node.check_config, "source_name_pattern", lambda: None
            )
            tags = getattr(request.node.check_config, "tags", lambda: None)
            test_name = getattr(request.node.check_config, "test_name", lambda: None)
            types = getattr(request.node.check_config, "types", lambda: None)
            upstream_path_pattern = getattr(
                request.node.check_config, "upstream_path_pattern", lambda: None
            )

            # Variables from `iterate_over_*` markers
            catalog_node = getattr(request.node.catalog_node, "node", lambda: None)
            catalog_source = getattr(request.node.catalog_source, "node", lambda: None)
            exposure = request.node.exposure
            macro = request.node.macro
            model = getattr(request.node.model, "model", lambda: None)
            run_result = getattr(request.node.run_result, "result", lambda: None)
            source = getattr(request.node.source, "source", lambda: None)
            unit_test = request.node.unit_test
        else:
            # From provided check_config
            accepted_uniqueness_tests = kwargs.get("accepted_uniqueness_tests")
            access = kwargs.get("access")
            column_name_pattern = kwargs.get("column_name_pattern")
            exclude = kwargs.get("exclude")
            include = kwargs.get("include")
            keys = kwargs.get("keys")
            materializations_to_include = kwargs.get("materializations_to_include")
            max_chained_views = kwargs.get("max_chained_views")
            max_downstream_models = kwargs.get("max_downstream_models")
            max_execution_time_seconds = kwargs.get("max_execution_time_seconds")
            max_gigabytes_billed = kwargs.get("max_gigabytes_billed")
            max_number_of_lines = kwargs.get("max_number_of_lines")
            max_upstream_macros = kwargs.get("max_upstream_macros")
            max_upstream_models = kwargs.get("max_upstream_models")
            max_upstream_sources = kwargs.get("max_upstream_sources")
            min_model_documentation_coverage_pct = kwargs.get(
                "min_model_documentation_coverage_pct"
            )
            min_model_test_coverage_pct = kwargs.get("min_model_test_coverage_pct")
            min_number_of_unit_tests = kwargs.get("min_number_of_unit_tests")
            model_name_pattern = kwargs.get("model_name_pattern")
            permitted_formats = kwargs.get("permitted_formats")
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
            unit_test = kwargs.get("unit_test")

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
            max_number_of_lines=max_number_of_lines,
            max_upstream_macros=max_upstream_macros,
            max_upstream_models=max_upstream_models,
            max_upstream_sources=max_upstream_sources,
            min_model_documentation_coverage_pct=min_model_documentation_coverage_pct,
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            min_number_of_unit_tests=min_number_of_unit_tests,
            model_name_pattern=model_name_pattern,
            permitted_formats=permitted_formats,
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
            unit_test=unit_test,
            # Fixtures from `FixturePlugin`
            exposures=exposures,
            manifest_obj=manifest_obj,
            models=models,
            sources=sources,
            tests=tests,
            unit_tests=unit_tests,
        )

    return wrapper

















def bouncer_check_v2(iterate_over=None):
    def decorator(func):
        @wraps(func)
        def wrapper(
            *args,
            **kwargs,
        ):
            # model = None
            # logging.error(iterate_over)
            # return func(access="public", iterate_over=iterate_over, model=model)
            return func(*args, **kwargs)
        # logging.error(iterate_over)
        return wrapper
    return decorator
    



















    
def create_github_comment_file(failed_checks: List[List[str]]) -> None:
    """
    Create a markdown file containing a comment for GitHub.
    """

    md_formatted_comment = make_markdown_table(
        [["Check name", "Failure message"]] + list(sorted(failed_checks))
    )

    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ.get('GITHUB_REPOSITORY',None)}/actions/runs/{os.environ.get('GITHUB_RUN_ID', None)})."  # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324

    logging.debug(f"{md_formatted_comment=}")

    if os.environ.get("GITHUB_REPOSITORY", None) is not None:
        comment_file = "/app/github-comment.md"
    else:
        comment_file = "github-comment.md"

    logging.info(f"Writing comment for GitHub to {comment_file}...")
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
        1. The file passed via the `--config-file` CLI flag.
        2. A file named `dbt-bouncer.yml` in the current working directory.
        3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).
    """

    logging.debug(f"{config_file=}")
    logging.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logging.debug(f"Config file passed via command line: {config_file}")
        return load_config_from_yaml(Path(config_file))

    if config_file_source == "DEFAULT":
        logging.debug(f"Using default value for config file: {config_file}")
        with contextlib.suppress(FileNotFoundError):
            return load_config_from_yaml(Path.cwd() / config_file)

    # Read config from pyproject.toml
    logging.info("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / "pyproject.toml").exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (parent for parent in Path().cwd().parents if (parent / "pyproject.toml").exists()),
            None,  # type: ignore[arg-type]
        )  # i.e. look in parent directories for a pyproject.toml file

    if pyproject_toml_dir is None:
        logging.debug("No pyproject.toml found.")
        raise RuntimeError(
            "No pyproject.toml found. Please ensure you have a pyproject.toml file in the root of your project correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
        )
    else:
        logging.debug(f"{pyproject_toml_dir / 'pyproject.toml'=}")

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
    logging.debug(f"Loading config from {config_path}...")
    logging.debug(f"Loading config from {config_file}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logging.info(f"Loaded config from {config_file}...")

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
