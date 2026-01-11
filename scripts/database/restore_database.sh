#!/bin/bash
# Restore Database Script
# This script restores a PostgreSQL database from a backup file

set -e

# Parse arguments
BACKUP_PATH=""
DROP_EXISTING=false
TARGET_DATABASE="nexdata"

while [[ $# -gt 0 ]]; do
    case $1 in
        --backup)
            BACKUP_PATH="$2"
            shift 2
            ;;
        --drop-existing)
            DROP_EXISTING=true
            shift
            ;;
        --target-db)
            TARGET_DATABASE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$BACKUP_PATH" ]; then
    echo "ERROR: --backup parameter is required"
    echo "Usage: $0 --backup <path-to-backup-file> [--drop-existing] [--target-db <dbname>]"
    exit 1
fi

echo "=== Nexdata Database Restore Script ==="
echo ""

# Database connection details from docker-compose.yml
DB_HOST="localhost"
DB_PORT="5433"
DB_USER="nexdata"
DB_PASSWORD="nexdata_dev_password"

# Check if backup file exists
if [ ! -f "$BACKUP_PATH" ]; then
    echo "ERROR: Backup file not found: $BACKUP_PATH"
    exit 1
fi

FILE_SIZE=$(du -h "$BACKUP_PATH" | cut -f1)
echo "Backup file: $BACKUP_PATH"
echo "Size: $FILE_SIZE"
echo ""

# Check if PostgreSQL container is running
echo "Checking if PostgreSQL container is running..."
if ! docker ps --filter "name=postgres" --format "{{.Names}}" | grep -q .; then
    echo "ERROR: PostgreSQL container is not running!"
    echo "Please start it with: docker-compose up -d postgres"
    exit 1
fi
echo "PostgreSQL container is running"
echo ""

export PGPASSWORD="$DB_PASSWORD"

if [ "$DROP_EXISTING" = true ]; then
    echo "WARNING: Dropping existing database '$TARGET_DATABASE'..."
    echo "Press Ctrl+C within 5 seconds to cancel..."
    sleep 5
    
    docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS $TARGET_DATABASE;"
    docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U "$DB_USER" -d postgres -c "CREATE DATABASE $TARGET_DATABASE OWNER $DB_USER;"
    echo "Database recreated."
    echo ""
fi

echo "Starting restore to database '$TARGET_DATABASE'..."
echo ""

# Restore database
if [[ "$BACKUP_PATH" =~ \.gz$ ]]; then
    echo "Detected gzip-compressed backup file"
    gunzip < "$BACKUP_PATH" | docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U "$DB_USER" -d "$TARGET_DATABASE"
else
    docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U "$DB_USER" -d "$TARGET_DATABASE" < "$BACKUP_PATH"
fi

unset PGPASSWORD

echo ""
echo "=== Restore Successful ==="
echo "Database '$TARGET_DATABASE' has been restored from backup."
echo ""
echo "You can now start the API with:"
echo "  docker-compose up -d"

