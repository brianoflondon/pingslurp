FROM python:3.11

# Install UV
COPY --from=ghcr.io/astral-sh/uv:0.13.8 /uv /uvx /bin/

# needed for pymssql to install in 3.11
RUN apt update && apt install -y freetds-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app/

# Copy dependency files (uv.lock* in case it doesn't exist yet)
COPY ./pyproject.toml ./uv.lock* /app/

# Install dependencies without the project itself (layer caching)
RUN uv sync --frozen --no-dev --no-install-project

# Keep the src layout. Copying src/ onto /app makes uv look for
# /app/src/pingslurp, so the pingslurper script is installed but
# `import pingslurp` fails. README.md is required to build the wheel.
# --no-editable installs the package into site-packages.
COPY ./README.md ./LICENSE /app/
COPY ./src /app/src

RUN uv sync --frozen --no-dev --no-editable

ENV PATH="/app/.venv/bin:${PATH}"

RUN pingslurper --help
