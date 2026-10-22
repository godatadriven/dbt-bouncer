# Keep default in sync with .python-version
ARG PYTHON_VERSION=3.14.8
FROM python:${PYTHON_VERSION}-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_HTTP_TIMEOUT=100 \
    UV_NO_CACHE=1

ENTRYPOINT ["uvx", "dbt-bouncer==0.0.0"]
