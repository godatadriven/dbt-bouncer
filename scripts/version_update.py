import argparse
import re
from pathlib import Path


def update_version(new_version: str, file_path: Path):
    """Update the version string in the given file."""
    old_version_pattern = "0.0.0"
    content = file_path.read_text()
    updated_content = re.sub(old_version_pattern, new_version, content)
    file_path.write_text(updated_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update version in files.")
    parser.add_argument(
        "--new-version", type=str, help="The new version string to set."
    )
    args = parser.parse_args()

    for file in ["src/dbt_bouncer/version.py", "./action.yml"]:
        file_path = Path(file)
        update_version(args.new_version, file_path)
