# On GitHub the `dbt build` command often returns "leaked semaphores" errors.
build-and-run-dbt-bouncer:
	poetry run dbt deps
	poetry run dbt build
	poetry run dbt docs generate
	poetry run dbt-bouncer --config-file ./dbt-bouncer-example.yml

build-artifacts:
	poetry run python ./scripts/generate_artifacts.py

build-pex:
	poetry run pex . \
		--interpreter-constraint ">=3.9,<3.14" \
		--jobs 128 \
		--max-install-jobs 0 \
		--output-file ./dist/dbt-bouncer.pex \
		--pip-version 24.1 \
		--platform macosx_11_0_x86_64-cp-3.9.21-cp39 \
		--platform macosx_11_0_x86_64-cp-3.10.16-cp310 \
		--platform macosx_11_0_x86_64-cp-3.11.11-cp311 \
		--platform macosx_11_0_x86_64-cp-3.12.8-cp312 \
		--platform macosx_10_13_x86_64-cp-3.13.0-cp313 \
		--platform manylinux2014_x86_64-cp-3.9.21-cp39 \
		--platform manylinux2014_x86_64-cp-3.10.16-cp310 \
		--platform manylinux2014_x86_64-cp-3.11.11-cp311 \
		--platform manylinux2014_x86_64-cp-3.12.8-cp312 \
		--platform manylinux2014_x86_64-cp-3.13.0-cp313 \
		--python-shebang='/usr/bin/env python' \
		--script dbt-bouncer

test:
	$(MAKE) test-unit
	$(MAKE) test-integration

test-integration:
	poetry run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/integration \
		$(MAKE_ARGS)

test-unit:
	poetry run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/unit \
		-m 'not not_in_parallel and not not_in_parallel2' && \
	poetry run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--cov-append \
		-m not_in_parallel && \
	poetry run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--cov-append \
		-m not_in_parallel2

test-windows:
	poetry run pytest -c ./tests --numprocesses 5 ./tests/unit -m 'not not_in_parallel and not not_in_parallel2' && \
	poetry run pytest -c ./tests -m not_in_parallel && \
	poetry run pytest -c ./tests -m not_in_parallel2 && \
	poetry run pytest -c ./tests --numprocesses 5 ./tests/integration
