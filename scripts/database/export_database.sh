#!/bin/bash
# Export Database Script
# This script exports the entire PostgreSQL database to a backup file

set -e

# Parse arguments
OUTPUT_PATH="./backup/nexdata_backup_$(date +%Y%m%d_%H%M%S).sql"
COMPRESS_GZIP=false
DATA_ONLY=false
SCHEMA_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_PATH="$2"
            shift 2
            ;;
        --compress)
            COMPRESS_GZIP=true
            shift
            ;;
        --data-only)
            DATA_ONLY=true
            shift
            ;;
        --schema-only)
            SCHEMA_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== Nexdata Database Export Script ==="
echo ""

# Database connection details from docker-compose.yml
DB_HOST="localhost"
DB_PORT="5433"
DB_NAME="nexdata"
DB_USER="nexdata"
DB_PASSWORD="nexdata_dev_password"

# Create backup directory if it doesn't exist
BACKUP_DIR=$(dirname "$OUTPUT_PATH")
mkdir -p "$BACKUP_DIR"
echo "Backup directory: $BACKUP_DIR"

# Check if PostgreSQL container is running
echo "Checking if PostgreSQL container is running..."
if ! docker ps --filter "name=postgres" --format "{{.Names}}" | grep -q .; then
    echo "ERROR: PostgreSQL container is not running!"
    echo "Please start it with: docker-compose up -d postgres"
    exit 1
fi
echo "PostgreSQL container is running"
echo ""

# Build pg_dump command options
DUMP_OPTIONS="--verbose --no-owner --no-acl"

if [ "$DATA_ONLY" = true ]; then
    DUMP_OPTIONS="$DUMP_OPTIONS --data-only"
    echo "Mode: Data only (no schema)"
elif [ "$SCHEMA_ONLY" = true ]; then
    DUMP_OPTIONS="$DUMP_OPTIONS --schema-only"
    echo "Mode: Schema only (no data)"
else
    echo "Mode: Full backup (schema + data)"
fi

echo "Output file: $OUTPUT_PATH"
echo "Starting export..."
echo ""

# Export database
export PGPASSWORD="$DB_PASSWORD"

if [ "$COMPRESS_GZIP" = true ]; then
    OUTPUT_PATH="${OUTPUT_PATH%.sql}.sql.gz"
    docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U "$DB_USER" -d "$DB_NAME" $DUMP_OPTIONS | gzip > "$OUTPUT_PATH"
else
    docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U "$DB_USER" -d "$DB_NAME" $DUMP_OPTIONS > "$OUTPUT_PATH"
fi

unset PGPASSWORD

echo ""
echo "=== Export Successful ==="
FILE_SIZE=$(du -h "$OUTPUT_PATH" | cut -f1)
echo "File: $OUTPUT_PATH"
echo "Size: $FILE_SIZE"
echo ""
echo "To restore this backup, use:"
echo "  ./scripts/database/restore_database.sh --backup '$OUTPUT_PATH'"

