FROM python:3.11

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# needed for pymssql to install in 3.11
RUN apt update && apt install -y freetds-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app/

# Copy dependency files (uv.lock* in case it doesn't exist yet)
COPY ./pyproject.toml ./uv.lock* /app/

# Install dependencies without the project itself (layer caching)
RUN uv sync --frozen --no-dev --no-install-project

COPY ./src /app/

RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}"
