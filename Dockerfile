# Keep in sync with .python-version
FROM python:3.11.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_HTTP_TIMEOUT=100 \
    UV_NO_CACHE=1

ENTRYPOINT ["uvx", "dbt-bouncer==2.0.0rc1"]
