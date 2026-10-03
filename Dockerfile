# syntax=docker/dockerfile:1.4
# Nexdata Dockerfile -- multi-target (SPEC_161)
#
# Targets:
#   runtime (default, last stage)  api + worker: requirements-runtime.txt only,
#                                  no compiler, no torch, non-root, HEALTHCHECK
#   train                          runtime deps + requirements-train.txt (torch,
#                                  einops, wandb) for the offline PLAN_062 trainers
#
# Build commands:
#   DOCKER_BUILDKIT=1 docker build --target runtime -t nexdata .
#   DOCKER_BUILDKIT=1 docker build --target train -t nexdata-train .
#   DOCKER_BUILDKIT=1 docker build --target runtime --build-arg INSTALL_BROWSERS=1 -t nexdata .
#
# INSTALL_BROWSERS=1 adds ~500MB for Chromium (needed for JS-rendered pages).
# Every stage uses the same python:3.11-slim base, so the virtualenv built in
# `builder` is valid when copied into `runtime` / `train`.

# -------------------------------------------------------------------
# builder: compiler + uv, installs the runtime deps into /opt/venv
# -------------------------------------------------------------------
FROM python:3.11-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv \
    && python -m venv /opt/venv

COPY requirements-runtime.txt /tmp/req/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python /opt/venv/bin/python -r /tmp/req/requirements-runtime.txt

# -------------------------------------------------------------------
# builder-train: adds the training-only deps on top of the runtime venv
# -------------------------------------------------------------------
FROM builder AS builder-train

COPY requirements-train.txt /tmp/req/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python /opt/venv/bin/python -r /tmp/req/requirements-train.txt

# -------------------------------------------------------------------
# os-base: OS libraries the app needs at run time (no compiler), app user
# -------------------------------------------------------------------
FROM python:3.11-slim AS os-base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PLAYWRIGHT_BROWSERS_PATH=/app/.playwright

WORKDIR /app

# postgresql-client: pg_dump/psql in ops scripts; pango/harfbuzz: weasyprint;
# tesseract/poppler: pytesseract + pdf2image (pension CAFR OCR)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpango-1.0-0 \
    libpangoft2-1.0-0 \
    libharfbuzz-subset0 \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 appuser

# -------------------------------------------------------------------
# train: offline trainers (torch). Not pushed by CI.
# -------------------------------------------------------------------
FROM os-base AS train

ARG INSTALL_BROWSERS=0

COPY --from=builder-train /opt/venv /opt/venv
RUN --mount=type=cache,target=/app/.playwright-cache \
    if [ "$INSTALL_BROWSERS" = "1" ]; then \
        playwright install chromium --with-deps; \
    fi
COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser alembic/ ./alembic/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser data/reference/ ./data/reference/
COPY --chown=appuser:appuser app/ ./app/
# directories only: COPY --chown did the files, and a recursive chown would
# duplicate every file into another layer
RUN mkdir -p data/raw data/reports data/seeds data/kaggle \
    && chown appuser:appuser /app /app/data /app/data/raw /app/data/reports /app/data/seeds /app/data/kaggle
USER appuser
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# -------------------------------------------------------------------
# runtime (default target): api + worker image pushed to Artifact Registry
# -------------------------------------------------------------------
FROM os-base AS runtime

ARG INSTALL_BROWSERS=0

COPY --from=builder /opt/venv /opt/venv

# Playwright browsers if requested (large download, cached separately)
RUN --mount=type=cache,target=/app/.playwright-cache \
    if [ "$INSTALL_BROWSERS" = "1" ]; then \
        playwright install chromium --with-deps; \
    fi

COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser alembic/ ./alembic/
COPY --chown=appuser:appuser scripts/ ./scripts/
COPY --chown=appuser:appuser data/reference/ ./data/reference/
# The VM's boot script extracts these from the image it is about to run, so the
# compose file always matches the image tag (deploy/vm-startup.sh). Root-owned:
# the app user has no reason to change them.
COPY docker-compose.gcp.yml ./deploy/docker-compose.gcp.yml
COPY deploy/ ./deploy/
COPY --chown=appuser:appuser app/ ./app/

# directories only: COPY --chown did the files, and a recursive chown would
# duplicate every file into another layer
RUN mkdir -p data/raw data/reports data/seeds data/kaggle \
    && chown appuser:appuser /app /app/data /app/data/raw /app/data/reports /app/data/seeds /app/data/kaggle

USER appuser

EXPOSE 8000

# Liveness of the api process (/livez does not touch the database, so a Cloud
# SQL blip does not mark the container unhealthy). The worker service disables
# this in docker-compose.gcp.yml: it serves no HTTP.
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/livez', timeout=4)"]

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
