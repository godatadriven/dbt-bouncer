# On GitHub the `dbt build` command often returns "leaked semaphores" errors.
build-and-run-dbt-bouncer:
	uv run dbt deps
	uv run dbt build
	uv run dbt docs generate
	uv run dbt-bouncer --config-file ./dbt-bouncer-example.yml

build-artifacts:
	uv run python ./scripts/generate_artifacts.py

install:
	uv sync --extra=dev --extra=docs

test:
	$(MAKE) test-unit
	$(MAKE) test-integration

test-integration:
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/integration \
		$(MAKE_ARGS)

test-unit:
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/unit \
		-m 'not not_in_parallel and not not_in_parallel2' && \
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--cov-append \
		-m not_in_parallel && \
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--cov-append \
		-m not_in_parallel2

test-windows:
	uv run pytest -c ./tests --numprocesses 5 ./tests/unit -m 'not not_in_parallel and not not_in_parallel2' && \
	uv run pytest -c ./tests -m not_in_parallel && \
	uv run pytest -c ./tests -m not_in_parallel2 && \
	uv run pytest -c ./tests --numprocesses 5 ./tests/integration
