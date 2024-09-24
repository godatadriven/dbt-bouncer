import ast
import logging
from pathlib import Path

from dbt_bouncer.logger import configure_console_logging


def main():
    """Assert that all checks are alphabetically sorted."""
    for f in Path("src/dbt_bouncer/checks").glob("*/*.py"):
        logging.info(f"Checking {f.name}...")
        with Path.open(f) as file:
            node = ast.parse(file.read())

        class_names = [
            class_.name
            for class_ in [n for n in node.body if isinstance(n, ast.ClassDef)]
            if class_.name.startswith("Check")
        ]
        logging.debug(f"{class_names=}")
        logging.debug(f"{sorted(class_names)=}")
        logging.debug(class_names == sorted(class_names))
        assert class_names == sorted(
            class_names
        ), f"Class names are not sorted alphabetically in {f.name}"


if __name__ == "__main__":
    configure_console_logging(1)
    main()
