PYTHON_VERSION_FILE := .python-version
PYTHON_INTERPRETER_CONSTRAINT := $(shell cat $(PYTHON_VERSION_FILE))

.PHONY: help
help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# On GitHub the `dbt build` command often returns "leaked semaphores" errors.
build-and-run-dbt-bouncer: ## Run dbt deps, build, docs generate and run dbt-bouncer
	uv run dbt deps
	uv run dbt build
	uv run dbt docs generate
	uv run dbt-bouncer --config-file ./dbt-bouncer-example.yml

build-artifacts: ## Build dbt artifacts for testing
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

install: ## Install dependencies
	uv sync --extra=dev --extra=docs

test: ## Run all tests
	$(MAKE) test-unit
	$(MAKE) test-integration

# Necessary due to https://github.com/coveragepy/coveragepy/issues/1514
test-dev-container: ## Run tests in the dev container
	uv run --extra dev pytest \
		--numprocesses 5 \
		./tests/unit && \
	uv run --extra dev pytest \
		--numprocesses 5 \
		./tests/integration

test-integration: ## Run integration tests
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/integration \
		$(MAKE_ARGS)

test-perf: ## Run performance tests
	bencher run \
		--adapter shell_hyperfine \
		--dry-run \
		--file results.json \
		--format json \
		"hyperfine --export-json results.json --runs 10 'dbt-bouncer --config-file dbt-bouncer-example.yml'"

test-unit: ## Run unit tests
	uv run pytest \
		-c ./tests \
		--junitxml=coverage.xml \
		--cov-report=term-missing:skip-covered \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/unit

test-windows: ## Run tests on Windows
	uv run pytest -c ./tests --numprocesses 5 ./tests/unit && \
	uv run pytest -c ./tests --numprocesses 5 ./tests/integration
