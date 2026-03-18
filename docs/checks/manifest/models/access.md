# Access

!!! note

    The below checks require `manifest.json` to be present.

These checks enforce access controls and governance on your dbt models. Use them to verify that models have the correct access level (e.g. `private`, `protected`, `public`), that public models have contracts enforced, and that appropriate grants are configured. Consistent access policies prevent accidental exposure of intermediate models and ensure downstream consumers interact only with stable, well-defined interfaces.

::: manifest.models.access
