#!/bin/bash
# Switch Nexdata environment
# Usage: source switch-env.sh [local|cloud|test]

ENV=${1:-local}

case $ENV in
  local)
    cp .env.local .env.active
    echo "Switched to LOCAL (Docker Postgres on :5434)"
    echo "Run: docker-compose up -d"
    ;;
  cloud)
    cp .env.cloud .env.active
    echo "Switched to CLOUD (GCP Cloud SQL)"
    echo "Prerequisite: gcloud auth application-default login"
    echo "Run: docker-compose --profile cloud --env-file .env.active up -d"
    ;;
  test)
    cp .env.test .env.active
    echo "Switched to TEST (isolated Postgres on :5436)"
    echo "Run: docker-compose --env-file .env.active up -d"
    ;;
  *)
    echo "Usage: source switch-env.sh [local|cloud|test]"
    echo "  local  - Docker Postgres container (default)"
    echo "  cloud  - GCP Cloud SQL (requires proxy)"
    echo "  test   - Isolated test database"
    exit 1
    ;;
esac
