# Keep in sync with .python-version
FROM python:3.11.5-alpine as base

# https://python-poetry.org/docs#ci-recommendations
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VENV=/opt/poetry-venv \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_VIRTUALENVS_IN_PROJECT=true


FROM base as builder

# # Install OS dependencies
RUN apk add --update curl && \
    rm -rf /var/cache/apk/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --version ${POETRY_VERSION}

# Add Poetry to PATH
ENV PATH="${PATH}:$POETRY_HOME/bin"

# Copy Dependencies
COPY poetry.lock pyproject.toml README.md ./
COPY dbt_bouncer ./dbt_bouncer

# Install Dependencies
RUN poetry install --no-cache --no-interaction --without dev \
    && rm -rf ~/.cache/pypoetry/artifacts


FROM base as dbt_bouncer

# Copy in pre-built .venv
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/lib /usr/local/lib

# Add dbt_bouncer to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

# Copy all remaining files
COPY dbt_bouncer ./dbt_bouncer

CMD ["/bin/bash", "-c", "echo 'Expecting commands to be passed in.' && exit 1"]
