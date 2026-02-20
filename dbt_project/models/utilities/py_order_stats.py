"""Python model example for testing dbt-bouncer with Python models."""


def model(dbt, _session):
    """Return staging orders data as a table.

    This is a minimal Python model that demonstrates:
    - Reading from an upstream dbt model
    - Returning a relation to dbt

    Args:
        dbt: dbt context object
        _session: Database session (unused)

    Returns:
        DuckDB relation

    """
    dbt.config(materialized="table")

    # Reference the upstream staging model and return it
    # This is the simplest possible Python model
    return dbt.ref("stg_orders")
