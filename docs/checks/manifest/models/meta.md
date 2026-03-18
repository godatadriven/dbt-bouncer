# Meta

!!! note

    The below checks require `manifest.json` to be present.

These checks validate the `meta` configuration on your dbt models. Use them to ensure every model includes a set of required meta keys, such as `owner`, `domain`, or any custom keys your organization relies on for governance and classification. Enforcing meta keys keeps your metadata consistent and enables downstream tooling to reliably discover and classify models.

::: manifest.models.meta
