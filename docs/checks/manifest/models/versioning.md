# Versioning

!!! note

    The below checks require `manifest.json` to be present.

These checks validate version management for your dbt models. Use them to ensure that versioned models specify a latest version, that version identifiers follow a required pattern, and that `ref()` calls to versioned models always pin a specific version. Proper versioning prevents breaking changes from silently propagating to downstream consumers and keeps your data contracts stable.

::: manifest.models.versioning
