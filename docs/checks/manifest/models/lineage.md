# Lineage

!!! note

    The below checks require `manifest.json` to be present.

These checks validate the DAG lineage and dependency structure of your dbt models. Use them to control which macros a model may depend on, ensure models do not pull from multiple sources, verify that key models feed into at least one exposure, limit chained views and fanout, and cap the number of upstream dependencies. Keeping your DAG well-structured prevents long-running query chains, reduces blast radius during failures, and makes the overall data pipeline easier to reason about.

::: manifest.models.lineage
