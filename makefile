PYTHON_VERSION_FILE := .python-version
PYTHON_INTERPRETER_CONSTRAINT := $(shell cat $(PYTHON_VERSION_FILE))

# On GitHub the `dbt build` command often returns "leaked semaphores" errors.
build-and-run-dbt-bouncer:
	uv run dbt deps
	uv run dbt build
	uv run dbt docs generate
	uv run dbt-bouncer --config-file ./dbt-bouncer-example.yml

build-artifacts: # 1.7 and 1.8 are no longer compatible with the latest dbt features so those fixtures are considered frozen
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.9.0' --from 'dbt-core~=1.9.0' dbt parse --profiles-dir ./dbt_project --project-dir ./dbt_project
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.9.0' --from 'dbt-core~=1.9.0' dbt docs generate --profiles-dir ./dbt_project --project-dir ./dbt_project
	rm -r ./tests/fixtures/dbt_19/target || true
	mkdir -p ./tests/fixtures/dbt_19/target
	mv ./dbt_project/target/*.json ./tests/fixtures/dbt_19/target
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.10.0' --from 'dbt-core~=1.10.0' dbt parse --profiles-dir ./dbt_project --project-dir ./dbt_project
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.10.0' --from 'dbt-core~=1.10.0' dbt docs generate --profiles-dir ./dbt_project --project-dir ./dbt_project
	rm -r ./tests/fixtures/dbt_110/target || true
	mkdir -p ./tests/fixtures/dbt_110/target
	mv ./dbt_project/target ./tests/fixtures/dbt_110
	# No dbt-duckdb==1.11 yet so sticking with dbt-duckdb==1.10
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.10.0' --from 'dbt-core~=1.11.0' dbt parse --profiles-dir ./dbt_project --project-dir ./dbt_project
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --with 'dbt-duckdb~=1.10.0' --from 'dbt-core~=1.11.0' dbt docs generate --profiles-dir ./dbt_project --project-dir ./dbt_project
	rm -r ./tests/fixtures/dbt_111/target || true
	mkdir -p ./tests/fixtures/dbt_111/target
	mv ./dbt_project/target ./tests/fixtures/dbt_111
	make install
	uv run dbt parse --profiles-dir ./dbt_project --project-dir ./dbt_project
	uv run dbt docs generate --profiles-dir ./dbt_project --project-dir ./dbt_project

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
		./tests/unit

test-windows:
	uv run pytest -c ./tests --numprocesses 5 ./tests/unit && \
	uv run pytest -c ./tests --numprocesses 5 ./tests/integration
