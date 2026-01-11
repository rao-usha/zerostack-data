# Database Export and Restore Scripts

This directory contains scripts for exporting and restoring your Nexdata PostgreSQL database.

## Overview

These scripts allow you to:
- **Export** your database to a backup file (SQL dump)
- **Restore** your database from a backup file
- Create portable snapshots of your data
- Stand up a new database instance with existing data

## Prerequisites

- Docker must be running
- PostgreSQL container must be running (`docker-compose up -d postgres`)

## Quick Start

### Export Your Current Database

**Windows (PowerShell):**
```powershell
.\scripts\database\export_database.ps1
```

**Linux/Mac (Bash):**
```bash
chmod +x scripts/database/export_database.sh
./scripts/database/export_database.sh
```

This will create a backup file in `./backup/nexdata_backup_YYYYMMDD_HHMMSS.sql`

### Restore a Database

**Windows (PowerShell):**
```powershell
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_120000.sql"
```

**Linux/Mac (Bash):**
```bash
./scripts/database/restore_database.sh --backup "./backup/nexdata_backup_20241201_120000.sql"
```

## Export Options

### PowerShell Parameters

```powershell
# Custom output path
.\scripts\database\export_database.ps1 -OutputPath "C:\backups\my_backup.sql"

# Export with gzip compression (smaller file size)
.\scripts\database\export_database.ps1 -CompressGzip

# Export only data (no schema)
.\scripts\database\export_database.ps1 -DataOnly

# Export only schema (no data)
.\scripts\database\export_database.ps1 -SchemaOnly
```

### Bash Options

```bash
# Custom output path
./scripts/database/export_database.sh --output "/path/to/my_backup.sql"

# Export with gzip compression
./scripts/database/export_database.sh --compress

# Export only data
./scripts/database/export_database.sh --data-only

# Export only schema
./scripts/database/export_database.sh --schema-only
```

## Restore Options

### PowerShell Parameters

```powershell
# Restore to a different database name
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\my_backup.sql" -TargetDatabase "nexdata_test"

# Drop existing database before restore (WARNING: Destructive!)
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\my_backup.sql" -DropExisting
```

### Bash Options

```bash
# Restore to a different database name
./scripts/database/restore_database.sh --backup "./backup/my_backup.sql" --target-db "nexdata_test"

# Drop existing database before restore (WARNING: Destructive!)
./scripts/database/restore_database.sh --backup "./backup/my_backup.sql" --drop-existing
```

## Common Use Cases

### 1. Create a Snapshot Before Major Changes

```powershell
# Windows
.\scripts\database\export_database.ps1 -OutputPath ".\backup\before_changes_$(Get-Date -Format 'yyyyMMdd').sql"

# Linux/Mac
./scripts/database/export_database.sh --output "./backup/before_changes_$(date +%Y%m%d).sql"
```

### 2. Share Data with Team Members

```powershell
# Export with compression for smaller file size
.\scripts\database\export_database.ps1 -CompressGzip -OutputPath ".\backup\nexdata_team_share.sql.gz"
```

Then share the `.sql.gz` file (can be restored without modification)

### 3. Stand Up a Fresh Database from Backup

```bash
# Stop containers
docker-compose down

# Remove old data volume
docker volume rm nexdata_postgres_data

# Start containers (creates fresh database)
docker-compose up -d postgres

# Wait for database to be ready (check with docker-compose logs postgres)

# Restore from backup
./scripts/database/restore_database.sh --backup "./backup/nexdata_backup_20241201_120000.sql"

# Start API
docker-compose up -d
```

### 4. Clone Database to Test Environment

```powershell
# Export production data
.\scripts\database\export_database.ps1 -OutputPath ".\backup\prod_clone.sql"

# Restore to test database
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\prod_clone.sql" -TargetDatabase "nexdata_test" -DropExisting
```

### 5. Create Daily Automated Backups

**Windows Task Scheduler:**
Create a scheduled task to run:
```powershell
.\scripts\database\export_database.ps1 -CompressGzip -OutputPath "C:\backups\daily\nexdata_$(Get-Date -Format 'yyyyMMdd').sql.gz"
```

**Linux Cron Job:**
```bash
# Add to crontab (crontab -e)
0 2 * * * cd /path/to/Nexdata && ./scripts/database/export_database.sh --compress --output "./backup/daily/nexdata_$(date +\%Y\%m\%d).sql.gz"
```

## Manual Export/Restore (Without Scripts)

If you prefer to run commands directly:

### Manual Export
```bash
docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U nexdata -d nexdata --no-owner --no-acl > backup.sql
```

### Manual Restore
```bash
docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U nexdata -d nexdata < backup.sql
```

## Database Connection Details

From `docker-compose.yml`:
- **Host (from Docker):** postgres
- **Host (from local machine):** localhost
- **Port (on local machine):** 5433
- **Port (inside Docker):** 5432
- **Database:** nexdata
- **User:** nexdata
- **Password:** nexdata_dev_password

## File Size Considerations

- **Full backup:** Includes all schemas, tables, data, indexes
- **Compressed (gzip):** Typically 70-90% smaller than uncompressed
- **Data only:** Smaller if you have large indexes
- **Schema only:** Very small, just table definitions

## Troubleshooting

### "PostgreSQL container is not running"
```bash
docker-compose up -d postgres
# Wait 10 seconds for container to be ready
docker-compose logs postgres
```

### "Connection refused" errors
Make sure the database is fully started:
```bash
docker exec nexdata-postgres-1 pg_isready -U nexdata
```

### Restore has many "already exists" warnings
This is normal if restoring to a database that already has tables. Use `-DropExisting` (PowerShell) or `--drop-existing` (Bash) to start fresh.

### Out of disk space
Use compression: `-CompressGzip` (PowerShell) or `--compress` (Bash)

## Best Practices

1. **Regular backups:** Export your database before major changes
2. **Version control:** Don't commit backup files to git (they're in `.gitignore`)
3. **Test restores:** Periodically test that your backups can be restored
4. **Secure storage:** Keep backups in a secure location, separate from source code
5. **Retention policy:** Keep at least 7 daily backups, 4 weekly backups
6. **Document changes:** Note which ingestion jobs were run before creating backup

## What Gets Backed Up

The export includes:
- All tables and data (including source-specific tables like `acs5_2023_b01001`)
- `ingestion_jobs` table (job history)
- `dataset_registry` table (dataset metadata)
- All indexes and constraints
- All sequences (auto-increment counters)

The export does NOT include:
- PostgreSQL users/roles (uses `--no-owner`)
- Permissions (uses `--no-acl`)
- The `postgres` system database

## Next Steps

After exporting/restoring:
1. Verify data integrity: `docker exec -i nexdata-postgres-1 psql -U nexdata -d nexdata -c "\dt"`
2. Check row counts: `SELECT COUNT(*) FROM ingestion_jobs;`
3. Start the API: `docker-compose up -d`
4. Test API endpoints: http://localhost:8001/docs

