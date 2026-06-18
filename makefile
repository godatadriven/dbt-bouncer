PYTHON_VERSION_FILE := .python-version
PYTHON_INTERPRETER_CONSTRAINT := $(shell cat $(PYTHON_VERSION_FILE))

.PHONY: help
help: ## Display help
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# On GitHub the `dbt build` command often returns "leaked semaphores" errors.
# dbt 2.0 drops `dbt docs generate`; use compile --write-catalog first (produces
# catalog.json), then build (produces manifest.json + run_results.json without
# clobbering the catalog). Explicit --profiles-dir/--project-dir for CI correctness.
build-and-run-dbt-bouncer: ## Run dbt deps, compile --write-catalog, build and run dbt-bouncer
	uv run dbt deps --profiles-dir ./dbt_project --project-dir ./dbt_project
	uv run dbt compile --write-catalog --profiles-dir ./dbt_project --project-dir ./dbt_project
	uv run dbt build --profiles-dir ./dbt_project --project-dir ./dbt_project
	uv run dbt-bouncer --config-file ./dbt-bouncer-example.yml

# Each version-specific target uses --target-path to write to an isolated directory.
# dbt 1.7–1.11 fixtures are frozen: their committed fixtures stay for backward-compat
# parsing coverage but are no longer rebuilt (matching the pattern for dbt 1.7/1.8/1.9).
# Only dbt 1.12 and 2.0 are actively rebuilt.
build-artifacts: build-artifacts-112 build-artifacts-20 build-artifacts-local ## Build dbt artifacts for testing

build-artifacts-112: ## Build dbt 1.12 test artifacts
	# No dbt-duckdb==1.12 yet so sticking with dbt-duckdb==1.10
	# dbt-core 1.12 is currently pre-release; --prerelease=allow lets uvx resolve the beta.
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --prerelease=allow --with 'dbt-duckdb~=1.10.0' --from 'dbt-core>=1.12.0b1,<1.13' dbt parse --profiles-dir ./dbt_project --project-dir ./dbt_project --target-path ./target_112
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --prerelease=allow --with 'dbt-duckdb~=1.10.0' --from 'dbt-core>=1.12.0b1,<1.13' dbt docs generate --profiles-dir ./dbt_project --project-dir ./dbt_project --target-path ./target_112
	rm -r ./tests/fixtures/dbt_112/target || true
	mv ./dbt_project/target_112 ./tests/fixtures/dbt_112/target

build-artifacts-20: ## Build dbt 2.0 (Fusion) test artifacts
	# dbt 2.0 deprecates `dbt docs generate`; use `dbt compile --write-catalog` instead.
	uvx --python "==$(PYTHON_INTERPRETER_CONSTRAINT)" --prerelease=allow --with 'dbt-duckdb~=1.10.0' --from 'dbt-core>=2.0.0a1,<3' dbt compile --write-catalog --profiles-dir ./dbt_project --project-dir ./dbt_project --target-path ./target_20
	rm -r ./tests/fixtures/dbt_20/target || true
	mkdir -p ./tests/fixtures/dbt_20
	mv ./dbt_project/target_20 ./tests/fixtures/dbt_20/target

build-artifacts-local: install ## Build local dbt test artifacts
	uv run dbt compile --write-catalog --profiles-dir ./dbt_project --project-dir ./dbt_project

generate-schema: ## Regenerate schema.json from Pydantic models
	uv run python scripts/generate_schema.py

install: ## Install dependencies
	uv sync --extra=dev --group=docs

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
		--junitxml=junit.xml \
		--cov-report=term-missing:skip-covered \
		--cov-report=xml \
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
		--junitxml=junit.xml \
		--cov-report=term-missing:skip-covered \
		--cov-report=xml \
		--cov=src/dbt_bouncer/ \
		--numprocesses 5 \
		./tests/unit

test-windows: ## Run tests on Windows
	uv run pytest -c ./tests --numprocesses 5 ./tests/unit && \
	uv run pytest -c ./tests --numprocesses 5 ./tests/integration
