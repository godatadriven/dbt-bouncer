# Code

!!! note

    The below checks require `manifest.json` to be present.

These checks validate the SQL code within your dbt models. Use them to detect hard-coded database or schema references, enforce regex-based code patterns, flag missing or extraneous semicolons, and limit model length. Catching these issues early keeps your SQL portable across environments and easier to maintain as your project grows.

::: manifest.models.code
