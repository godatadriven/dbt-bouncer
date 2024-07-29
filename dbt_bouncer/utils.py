import contextlib
import re
from pathlib import Path

import toml
import yaml

from dbt_bouncer.logger import logger


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


def get_check_inputs(check_config=None, macro=None, model=None, request=None, source=None):
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

    return {"check_config": check_config, "macro": macro, "model": model, "source": source}


def get_dbt_bouncer_config(config_file: str, config_file_source: str):
    """
    Get the config for dbt-bouncer. This is fetched from (in order):
        1. The file passed via the `--config=file` CLI flag.
        2. A file named `dbt_bouncer.yml` in the current working directory.
        3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).
    """

    if config_file_source == "COMMANDLINE":
        logger.debug(f"Config file passed via command line: {config_file}")
        return load_config_from_yaml(config_file)

    if config_file_source == "DEFAULT":
        logger.debug(f"Using default value for config file: {config_file}")
        with contextlib.suppress(FileNotFoundError):
            return load_config_from_yaml(config_file)

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
        try:
            conf = [v for k, v in toml_cfg["tool"].items() if k == "dbt-bouncer"][0]
        except IndexError:
            raise RuntimeError(
                "Please ensure your pyproject.toml file is correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
            )
    return conf


def load_config_from_yaml(config_file):

    config_path = Path(__file__).parent.parent / config_file
    logger.debug(f"Loading config from {config_path}...")
    logger.debug(f"Loading config from {config_file}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_file}...")

    return conf


def make_markdown_table(array):
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
