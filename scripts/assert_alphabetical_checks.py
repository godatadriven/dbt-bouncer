import ast
import logging
from pathlib import Path

from dbt_bouncer.reporting.logger import configure_console_logging


def main():
    """Assert that all checks are alphabetically sorted."""
    for f in sorted(Path("src/dbt_bouncer/checks").glob("**/*.py")):
        logging.info(f"Checking {f}...")
        with Path.open(f, encoding="utf-8") as file:
            node = ast.parse(file.read())

        check_names = [
            defn.name
            for defn in node.body
            if (isinstance(defn, ast.FunctionDef) and defn.name.startswith("check_"))
            or (isinstance(defn, ast.ClassDef) and defn.name.startswith("Check"))
        ]
        logging.debug(f"{check_names=}")
        logging.debug(f"{sorted(check_names)=}")
        assert check_names == sorted(check_names), (
            f"Check definitions are not sorted alphabetically in {f}: {check_names}"
        )


if __name__ == "__main__":
    configure_console_logging(1)
    main()
