from pathlib import Path

import click

from dbt_bouncer.logger import logger
from dbt_bouncer.version import version


@click.command()
@click.option(
    "--dbt-project-dir",
    help="Directory where the dbt project exists.",
    required=True,
    type=click.Path(exists=True),
)
@click.version_option()
def cli(dbt_project_dir):
    logger.info(f"Running dbt_bouncer ({version()})...")
    logger.debug(f"{dbt_project_dir=}")

    manifest_json_path = Path(dbt_project_dir) / "target/manifest.json"
    if not manifest_json_path.exists():
        raise RuntimeError(f"No manifest.json found at {manifest_json_path}.")
