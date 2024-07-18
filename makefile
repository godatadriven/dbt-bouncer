build-pex:
	poetry run pex . -c dbt-bouncer -o ./dist/dbt-bouncer.pex --python-shebang='/usr/bin/env python'

test:
	poetry run pytest --junitxml=coverage.xml --cov-report=term-missing:skip-covered --cov=dbt_bouncer/ ./tests --numprocesses 5
