FROM debian:bookworm-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.18 /uv /uvx /bin/

ENV BENCHER_VERSION=0.5.9 \
    DBT_PROFILES_DIR="./dbt_project" \
    DBT_PROJECT_DIR="./dbt_project" \
    PATH="/root/.local/bin/:/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# Install OS dependencies
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install build-essential ca-certificates curl git make wget -y && \
    rm -rf /var/lib/apt/lists/*

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

COPY . .

# Install Python dependencies
RUN uv sync --extra=dev --extra=docs && \
    uv run prek install && \
    uv run prek install --install-hooks
