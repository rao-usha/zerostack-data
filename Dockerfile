# syntax=docker/dockerfile:1.4
# Nexdata Dockerfile - Optimized for fast rebuilds
#
# Build commands:
#   DOCKER_BUILDKIT=1 docker build -t nexdata .                          # Core (no Playwright browsers)
#   DOCKER_BUILDKIT=1 docker build --build-arg INSTALL_BROWSERS=1 -t nexdata .  # Full (with browsers)
#
# The INSTALL_BROWSERS=1 option adds ~500MB for Chromium (needed for JS-rendered pages)

FROM python:3.11-slim AS base

WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/.playwright

# Install system dependencies and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

# -------------------------------------------------------------------
# Dependencies stage - cached separately from application code
# -------------------------------------------------------------------
FROM base AS dependencies

# Build argument for Playwright browsers
ARG INSTALL_BROWSERS=0

# Copy requirements (cache layer)
COPY requirements.txt .

# Install Python dependencies with caching
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=cache,target=/root/.cache/pip \
    uv pip install --system -r requirements.txt

# Install Playwright browsers if requested (large download, cached separately)
RUN --mount=type=cache,target=/app/.playwright-cache \
    if [ "$INSTALL_BROWSERS" = "1" ]; then \
        playwright install chromium --with-deps; \
    fi

# -------------------------------------------------------------------
# Final stage - application code
# -------------------------------------------------------------------
FROM dependencies AS final

# Copy application code
COPY app/ ./app/
COPY scripts/ ./scripts/

# Create non-root user and set ownership
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

