def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "iterate_over_catalog_nodes: Tests that should run once per node in `catalog.json`",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_catalog_sources: Tests that should run once per source in `catalog.json`",
    )
    config.addinivalue_line("markers", "iterate_over_macros: Tests that should run once per macro")
    config.addinivalue_line(
        "markers", "iterate_over_exposures: Tests that should run once per exposure"
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_models: Tests that should run once per model",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_run_results: Tests that should run once per run result in `run_results.json`",
    )
    config.addinivalue_line(
        "markers",
        "iterate_over_sources: Tests that should run once per source",
    )

    ini_values = {
        "python_classes": ["check_", "Check"],
        "python_files": ["check_*.py"],
        "python_functions": ["check_*"],
    }

    for name, values in ini_values.items():
        current = config.getini(name)
        current[:] = values
