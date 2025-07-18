import logging
from pathlib import Path

import sh

from dbt_bouncer.logger import configure_console_logging


def build_pex_file(dbt_version, pex_file_name):
    """Build a pex file of `dbt-bouncer` if it does not already exist."""
    if not Path(pex_file_name).exists():
        logging.info(f"Building pex file for dbt version {dbt_version}")
        command = [
            "run",
            "pex",
            "--interpreter-constraint",
            ">=3.9,<3.14",
            "--jobs",
            "128",
            "--max-install-jobs",
            "0",
            "--output-file",
            pex_file_name,
            "--pip-version",
            "24.1",
            "--python-shebang",
            "/usr/bin/env python",
            "--script",
            "dbt",
            f"dbt-core=={dbt_version}",
            "dbt-duckdb",
            "pytz",
            "setuptools",
        ]
        if dbt_version in ["1.7", "1.8"]:
            command.extend(["protobuf<5"])
        else:
            command.extend(["protobuf>=5"])

        sh.poetry(
            command,
            _fg=True,
        )


def generate_artifacts(
    dbt_version,
    artifact_path,
    pex_file_name,
):
    """Generate dbt artifacts for all specified versions of dbt. These artifacts are used in testing."""
    logging.info(f"Generating dbt artifacts for dbt version {dbt_version}")
    Path(artifact_path).mkdir(exist_ok=True, parents=True)
    sh.python(
        [
            pex_file_name,
            "deps",
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "dbt_project",
        ],
        _fg=True,
    )
    sh.python(
        [
            pex_file_name,
            "parse",
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "dbt_project",
            "--target-path",
            (Path().cwd() / artifact_path).__str__(),
        ],
        _fg=True,
    )
    sh.python(
        [
            pex_file_name,
            "docs",
            "generate",
            "--project-dir",
            "dbt_project",
            "--profiles-dir",
            "dbt_project",
            "--target-path",
            (Path().cwd() / artifact_path).__str__(),
        ],
        _fg=True,
    )


def main():
    """For the specified dbt versions, build a pex file and generate dbt artifacts in the `./tests` directory."""
    dbt_versions = ["1.7", "1.8", "1.9", "1.10"]

    for dbt_version in dbt_versions:
        pex_file_name = f"./dist/dbt-{dbt_version.replace('.', '')}.pex"
        artifact_path = f"./tests/fixtures/dbt_{dbt_version.replace('.', '')}/target"

        build_pex_file(dbt_version=dbt_version, pex_file_name=pex_file_name)
        generate_artifacts(
            dbt_version=dbt_version,
            artifact_path=artifact_path,
            pex_file_name=pex_file_name,
        )


if __name__ == "__main__":
    configure_console_logging(1)
    main()
