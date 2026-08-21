# ruff: file-ignore[suspicious-subprocess-import, subprocess-without-shell-equals-true]
"""Build standalone zero-Python single-binary executable using PyInstaller.

Packages dbt-bouncer and its dependencies into a standalone single-file executable
for the current operating system and architecture.
"""

from __future__ import annotations

import argparse
import logging
import platform
import stat
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


def resolve_target_names(tag: str, binary_name: str | None = None) -> tuple[str, str]:
    """Resolve the final binary filename and the base name without extension.

    Args:
        tag: Platform tag (e.g. 'linux-x86_64', 'windows-x86_64').
        binary_name: Optional explicit custom binary name.

    Returns:
        tuple[str, str]: A tuple of `(target_name, target_base)` where `target_name`
            includes `.exe` on Windows, and `target_base` has `.exe` stripped.

    """
    if binary_name:
        target_name = binary_name
    elif "windows" in tag:
        target_name = f"dbt-bouncer-{tag}.exe"
    else:
        target_name = f"dbt-bouncer-{tag}"

    target_base = target_name.removesuffix(".exe")
    return target_name, target_base


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
    _, target_base = resolve_target_names(tag, binary_name)

    entrypoint = Path("src/dbt_bouncer/main.py").resolve()
    schema_file = Path("schema.json").resolve()

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        target_base,
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


def main(argv: list[str] | None = None) -> int:
    """Execute standalone binary build.

    Args:
        argv: Optional command line argument list (defaults to sys.argv[1:]).

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

    args = parser.parse_args(argv)
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
        target_name, target_base = resolve_target_names(tag, args.binary_name)
        binary_path = output_dir / target_name
        if not binary_path.exists() and (output_dir / f"{target_base}.exe").exists():
            binary_path = output_dir / f"{target_base}.exe"

        if not binary_path.exists():
            logger.error("Built binary not found at %s", binary_path)
            return 1

        # Ensure executable permissions on POSIX
        if platform.system().lower() != "windows":
            binary_path.chmod(binary_path.stat().st_mode | stat.S_IEXEC)

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

        # Also run --help to confirm the Typer command tree assembled correctly
        # (subcommands registered), which --version alone does not exercise.
        test_help = subprocess.run(
            [str(binary_path), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        if test_help.returncode != 0:
            logger.error("Binary verification --help failed:\n%s", test_help.stderr)
            return test_help.returncode

        logger.info(
            "Binary verification succeeded! Output: %s", test_version.stdout.strip()
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
