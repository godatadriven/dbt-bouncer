FROM python:3.11.14-slim

ARG USER_ID=1001
ARG GROUP_ID=1001
ENV HOME=/home/appuser

# Install OS dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install adduser build-essential ca-certificates curl git make wget -y && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

RUN groupadd -g $GROUP_ID appgroup && \
    useradd -u $USER_ID -g appgroup -s /bin/bash -m -d $HOME appuser

WORKDIR /app
# RUN chown -R appuser:appgroup /app

ENV BENCHER_VERSION=0.5.9 \
    DBT_PROFILES_DIR="./dbt_project" \
    DBT_PROJECT_DIR="./dbt_project" \
    PATH="/root/.local/bin/:/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_VENV_CLEAR=1 \
    UV_LINK_MODE=copy

USER appuser

# Configure git
RUN git config --global --add safe.directory /workspaces/dbt-bouncer

# Install Bencher
RUN curl --proto '=https' --tlsv1.2 -sSfL https://bencher.dev/download/install-cli.sh | sh

# Install Hyperfine via Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install --version 1.20.0 hyperfine

# Install Oh My Zsh
RUN sh -c "$(wget -O- https://github.com/deluan/zsh-in-docker/releases/download/v1.2.1/zsh-in-docker.sh)" \
    -p git \
    -p ssh-agent \
    -p https://github.com/zsh-users/zsh-autosuggestions \
    -p https://github.com/zsh-users/zsh-completions \
    -p https://github.com/zsh-users/zsh-syntax-highlighting.git \
    -p git-auto-fetch && \
    chsh -s $(which zsh)

# Install Atuin
RUN curl --proto '=https' --tlsv1.2 -LsSf https://setup.atuin.sh | sh && \
    echo 'eval "$(atuin init zsh)"' >> ~/.zshrc

COPY .pre-commit-config.yaml .python-version pyproject.toml README.md uv.lock ./

# Install Python dependencies
RUN uv venv && \
    uv sync --extra=dev --extra=docs && \
    echo 'source /workspace/dbt-bouncer/.venv/bin/activate' >> ~/.zshrc && \
    uv run prek install && \
    uv run prek install --install-hooks

COPY . .
