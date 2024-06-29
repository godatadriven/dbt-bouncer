ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}-slim

# https://python-poetry.org/docs#ci-recommendations
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME=/opt/poetry \
    POETRY_VENV=/opt/poetry-venv

# Creating a virtual environment just for poetry and install it with pip
RUN python3 -m venv $POETRY_VENV \
    && $POETRY_VENV/bin/pip install --no-cache-dir -U pip setuptools \
    && $POETRY_VENV/bin/pip install --no-cache-dir poetry==${POETRY_VERSION}

# Add Poetry to PATH
ENV PATH="${PATH}:${POETRY_VENV}/bin"

WORKDIR /app

# Copy Dependencies
COPY poetry.lock pyproject.toml README.md ./
COPY dbt_bouncer ./dbt_bouncer

# Install Dependencies
RUN poetry install --no-cache --no-interaction --without dev \
    && rm -rf ~/.cache/pypoetry/artifacts

CMD ["/bin/bash", "-c", "echo 'Expecting commands to be passed in.' && exit 1"]
