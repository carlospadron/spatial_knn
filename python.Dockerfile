FROM python:3.12-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libgdal-dev libgeos-dev libproj-dev \
 && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Build the venv at /opt/venv so the /workspace bind-mount can't overwrite it
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /build
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

WORKDIR /workspace
