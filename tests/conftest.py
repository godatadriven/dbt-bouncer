def pytest_configure(config):
    config.addinivalue_line("markers", "iterate_over_models: Tests that should run once per model")
