FROM rust:slim-bookworm AS rust-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --version 1.20.0 hyperfine

FROM python:3.11.14-slim AS base

ARG USER_ID=1001
ARG GROUP_ID=1001
ENV HOME=/home/appuser

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        adduser \
        build-essential \
        ca-certificates \
        curl \
        git \
        make \
        wget \
        zsh \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g $GROUP_ID appgroup && \
    useradd -u $USER_ID -g appgroup -s /bin/bash -m -d $HOME appuser

RUN mkdir -p "${HOME}" && \
    chown -R appuser:appgroup "${HOME}" && \
    chmod 755 "${HOME}" # rwxr-xr-x for home directory

COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

RUN curl --proto '=https' --tlsv1.2 -sSfL https://bencher.dev/download/install-cli.sh | sh

COPY --from=rust-builder /usr/local/cargo/bin/hyperfine /usr/local/bin/hyperfine

# Set initial PATH, virtual env settings, and disable compfix for ZSH setup
ENV BENCHER_VERSION=0.5.9 \
    DBT_PROFILES_DIR="./dbt_project" \
    DBT_PROJECT_DIR="./dbt_project" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_VENV_CLEAR=1 \
    UV_LINK_MODE=copy \
    VIRTUAL_ENV="/workspaces/dbt-bouncer/.venv" \
    PATH="${HOME}/.cargo/bin:${HOME}/.local/bin:/app/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    ZSH_DISABLE_COMPFIX="true"

FROM base AS final

WORKDIR /app
COPY --chown=appuser:appgroup . .
RUN chown -R appuser:appgroup /app
RUN mkdir -p /app/.git/hooks && chmod -R u+rwX /app/.git/hooks

USER appuser

# Configure git for the non-root user
RUN git config --global --add safe.directory /workspaces/dbt-bouncer

# Install Oh My Zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.1/zsh-in-docker.sh)" \
    -p git \
    -p ssh-agent \
    -p https://github.com/zsh-users/zsh-autosuggestions \
    -p https://github.com/zsh-users/zsh-completions \
    -p https://github.com/zsh-users/zsh-syntax-highlighting.git \
    -p git-auto-fetch

# Install Atuin
RUN curl --proto '=https' --tlsv1.2 -LsSf https://setup.atuin.sh | sh && \
    echo 'eval "$(atuin init zsh)"' >> "${HOME}/.zshrc"

# Install Python dependencies as the non-root user
RUN UV_VENV_OVERWRITE=1 uv venv --python /usr/local/bin/python && \
    uv sync --extra=dev --extra=docs && \
    uv run prek install && \
    uv run prek install --install-hooks

USER root
RUN sed -i "s|appuser:x:${USER_ID}:${GROUP_ID}:${HOME}:/bin/bash|appuser:x:${USER_ID}:${GROUP_ID}:${HOME}:$(which zsh)|" /etc/passwd
USER appuser

CMD ["zsh"]
