# ruff: file-ignore[suspicious-subprocess-import, subprocess-without-shell-equals-true]
"""Build standalone zero-Python single-binary executable using PyInstaller.

Packages dbt-bouncer and its dependencies into a standalone single-file executable
for the current operating system and architecture.
"""

from __future__ import annotations

import argparse
import logging
import platform
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def get_target_platform_tag() -> str:
    """Return a standard platform tag string (e.g. linux-x86_64, darwin-arm64).

    Returns:
        str: Platform tag indicating OS and architecture.

    """
    system = platform.system().lower()
    machine = platform.machine().lower()

    match machine:
        case "x86_64" | "amd64":
            arch = "x86_64"
        case "aarch64" | "arm64":
            arch = "arm64"
        case _:
            arch = machine

    return f"{system}-{arch}"


def build_pyinstaller_command(
    output_dir: Path,
    binary_name: str | None = None,
) -> list[str]:
    """Construct the PyInstaller CLI argument list.

    Args:
        output_dir: Directory where the output binary should be placed.
        binary_name: Optional custom binary name.

    Returns:
        list[str]: Command line arguments for PyInstaller.

    """
    tag = get_target_platform_tag()
    target_name = (
        binary_name
        if binary_name
        else (
            f"dbt-bouncer-{tag}" if "windows" not in tag else f"dbt-bouncer-{tag}.exe"
        )
    )

    entrypoint = Path("src/dbt_bouncer/main.py").resolve()
    schema_file = Path("schema.json").resolve()

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        target_name,
        "--onefile",
        "--clean",
        "--noconfirm",
        "--distpath",
        str(output_dir),
        "--collect-all",
        "dbt_bouncer",
        "--collect-all",
        "sqlglot",
        "--hidden-import",
        "pydantic",
        "--hidden-import",
        "pydantic_core",
        "--hidden-import",
        "rich",
        "--hidden-import",
        "typer",
        "--hidden-import",
        "yaml",
        "--hidden-import",
        "orjson",
    ]

    if schema_file.exists():
        sep = ";" if platform.system().lower() == "windows" else ":"
        cmd.extend(["--add-data", f"{schema_file}{sep}."])

    cmd.append(str(entrypoint))
    return cmd


def main() -> int:
    """Execute standalone binary build.

    Returns:
        int: Exit status code (0 for success, non-zero for failure).

    """
    parser = argparse.ArgumentParser(
        description="Build standalone dbt-bouncer binary executable."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("dist"),
        help="Directory to save the built standalone binary.",
    )
    parser.add_argument(
        "--binary-name",
        type=str,
        default=None,
        help="Custom name for the output executable.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Run smoke test against the built binary after compilation.",
    )

    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = build_pyinstaller_command(output_dir, args.binary_name)
    logger.info("Building standalone binary with command:\n%s", " ".join(cmd))

    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        logger.error("PyInstaller build failed.")
        return result.returncode

    if args.verify:
        tag = get_target_platform_tag()
        target_name = (
            args.binary_name
            if args.binary_name
            else (
                f"dbt-bouncer-{tag}"
                if "windows" not in tag
                else f"dbt-bouncer-{tag}.exe"
            )
        )
        binary_path = output_dir / target_name
        if not binary_path.exists():
            logger.error("Built binary not found at %s", binary_path)
            return 1

        logger.info("Verifying built binary at %s...", binary_path)
        test_version = subprocess.run(
            [str(binary_path), "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        if test_version.returncode != 0:
            logger.error(
                "Binary verification --version failed:\n%s", test_version.stderr
            )
            return test_version.returncode

        logger.info(
            "Binary verification succeeded! Output: %s", test_version.stdout.strip()
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
