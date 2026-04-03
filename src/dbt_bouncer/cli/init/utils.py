"""Utility functions for the init CLI subcommand."""

from pathlib import Path

from dbt_bouncer.enums import ConfigFileName


def build_initial_config(
    artifacts_dir: str,
    check_descriptions: bool,
    check_unique_tests: bool,
    check_naming: bool,
) -> tuple[dict, int]:
    """Build the configuration dictionary based on user's answers.

    Args:
        artifacts_dir: The directory where dbt artifacts are located.
        check_descriptions: Whether to check for model descriptions.
        check_unique_tests: Whether to check for unique tests on models.
        check_naming: Whether to check naming conventions for staging models.

    Returns:
        tuple[dict, int]: The configuration dictionary and the number of checks added.

    """
    manifest_checks = []

    if check_descriptions:
        manifest_checks.append(
            {
                "name": "check_model_description_populated",
                "description": "All models must have a description.",
            }
        )

    if check_unique_tests:
        manifest_checks.append(
            {
                "name": "check_model_has_unique_test",
                "description": "All models must have a unique test defined.",
            }
        )

    if check_naming:
        manifest_checks.append(
            {
                "name": "check_model_names",
                "description": "Models in the staging layer should always start with 'stg_'.",
                "include": "^models/staging",
                "model_name_pattern": "^stg_",
            }
        )

    config_dict = {
        "dbt_artifacts_dir": artifacts_dir,
        "manifest_checks": manifest_checks,
    }

    return config_dict, len(manifest_checks)


def write_config_file(config_dict: dict) -> Path:
    """Write the configuration dictionary to a file.

    Args:
        config_dict: The configuration dictionary to write.

    Returns:
        Path: The path to the created config file.

    """
    import yaml

    config_path = Path(ConfigFileName.DBT_BOUNCER_YML)

    with Path(config_path).open("w") as f:
        yaml.dump(
            config_dict,
            f,
            default_flow_style=False,
            sort_keys=False,
            Dumper=yaml.CSafeDumper,  # type: ignore[possibly-missing-attribute]
        )
    return config_path
