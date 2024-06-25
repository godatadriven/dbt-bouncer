import click

from dbt_bouncer.logger import logger
from dbt_bouncer.version import version


@click.command()
def cli():
    logger.info(f"Running dbt_bouncer ({version()})...")
