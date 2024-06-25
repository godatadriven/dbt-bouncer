test:
	poetry run pytest --junitxml=coverage.xml --cov-report=term-missing:skip-covered --cov=dbt_bouncer/ ./tests --numprocesses 5
