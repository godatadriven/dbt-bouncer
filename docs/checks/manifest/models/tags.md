# Tags

!!! note

    The below checks require `manifest.json` to be present.

These checks validate the tags assigned to your dbt models. Use them to ensure every model carries a required set of tags, with the option to require all specified tags or any one of them. Tags drive selective execution (`dbt build --select tag:...`) and filtering in documentation, so enforcing them keeps your project organized and your CI pipelines efficient.

::: manifest.models.tags
