# Keep default in sync with .python-version
ARG PYTHON_VERSION=3.14.8

# Build stage: install the locked dependencies and dbt-bouncer into a virtual
# environment. The image then starts without a network call, and every start
# of one image tag runs the same dependency versions.
FROM python:${PYTHON_VERSION}-slim AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_HTTP_TIMEOUT=100 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/dbt-bouncer \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /src

# Dependencies first: this layer is rebuilt only when the lock file changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-editable --no-install-project

COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:${PYTHON_VERSION}-slim
COPY --from=build /opt/dbt-bouncer /opt/dbt-bouncer
ENV PATH="/opt/dbt-bouncer/bin:${PATH}"

ENTRYPOINT ["dbt-bouncer"]
