# Database Backup & Restore Guide

Quick reference for exporting and restoring your Nexdata database.

## TL;DR - Quick Commands

### Export Database (Windows)
```powershell
.\scripts\database\export_database.ps1
```
Creates: `backup/nexdata_backup_YYYYMMDD_HHMMSS.sql`

### Restore Database (Windows)
```powershell
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_120000.sql"
```

## Why Export Your Database?

- **Backup before changes:** Save your data before running new ingestion jobs
- **Share with team:** Give teammates a snapshot of your data
- **Deploy to production:** Stand up a database with pre-loaded data
- **Testing:** Create test environments with real data
- **Recovery:** Restore if something goes wrong

## What Gets Exported?

Everything in your `nexdata` database:
- ✅ All source-specific tables (e.g., `acs5_2023_b01001`, `sec_companies`)
- ✅ Job history (`ingestion_jobs`)
- ✅ Dataset metadata (`dataset_registry`)
- ✅ All indexes and constraints
- ✅ All family office data, SEC filings, FRED data, etc.

## Step-by-Step: Export Your Data

### 1. Make sure your database is running
```powershell
docker-compose up -d postgres
```

### 2. Run the export script
```powershell
.\scripts\database\export_database.ps1
```

### 3. Find your backup file
It will be saved in the `backup/` directory:
```
backup/
  nexdata_backup_20241201_143022.sql
```

### 4. Store it safely
- Copy to a backup drive
- Upload to cloud storage
- Share with your team
- Keep in a secure location (contains all your ingested data)

## Step-by-Step: Stand Up a Fresh Database

Let's say you want to set up the database on a new machine or restore from backup:

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd Nexdata
```

### 2. Start the PostgreSQL container
```powershell
docker-compose up -d postgres
```

Wait about 10 seconds for PostgreSQL to initialize.

### 3. Copy your backup file
Place your backup file (e.g., `nexdata_backup_20241201_143022.sql`) in the `backup/` directory:
```
Nexdata/
  backup/
    nexdata_backup_20241201_143022.sql
```

### 4. Restore the database
```powershell
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_143022.sql"
```

### 5. Start the API
```powershell
docker-compose up -d
```

### 6. Verify everything works
Open http://localhost:8001/docs in your browser.

Test an endpoint:
```powershell
curl http://localhost:8001/api/v1/jobs
```

You should see all your previous ingestion jobs!

## Advanced Options

### Compressed Backups (Smaller File Size)
```powershell
# Export with gzip compression
.\scripts\database\export_database.ps1 -CompressGzip

# Creates: backup/nexdata_backup_YYYYMMDD_HHMMSS.sql.gz
# Restore works the same way (automatically detects compression)
```

### Export Only Schema (No Data)
```powershell
# Useful for creating empty databases with the same structure
.\scripts\database\export_database.ps1 -SchemaOnly
```

### Export Only Data (No Schema)
```powershell
# Useful if schema already exists
.\scripts\database\export_database.ps1 -DataOnly
```

### Drop and Recreate Database
```powershell
# WARNING: This will DELETE all existing data!
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\my_backup.sql" -DropExisting
```

### Custom Output Location
```powershell
# Save to a specific location
.\scripts\database\export_database.ps1 -OutputPath "C:\MyBackups\nexdata_prod.sql"
```

## Common Scenarios

### Scenario 1: "I want to backup before ingesting new data"
```powershell
# Create a snapshot
.\scripts\database\export_database.ps1 -OutputPath ".\backup\before_new_ingestion.sql"

# Run your ingestion
curl -X POST "http://localhost:8001/api/v1/census/ingest/acs5/2023/B01001?geography=state:*"

# If something goes wrong, restore:
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\before_new_ingestion.sql" -DropExisting
```

### Scenario 2: "I want to share my database with a coworker"
```powershell
# Export with compression (smaller file)
.\scripts\database\export_database.ps1 -CompressGzip -OutputPath ".\nexdata_share.sql.gz"

# Share nexdata_share.sql.gz via email/drive/etc.

# Coworker restores:
.\scripts\database\restore_database.ps1 -BackupPath ".\nexdata_share.sql.gz"
```

### Scenario 3: "I want to migrate to a new server"
On old server:
```powershell
.\scripts\database\export_database.ps1 -OutputPath ".\migration.sql"
```

On new server:
```powershell
# Install Docker, clone repo
docker-compose up -d postgres
.\scripts\database\restore_database.ps1 -BackupPath ".\migration.sql"
docker-compose up -d
```

### Scenario 4: "I messed up and need to start over"
```powershell
# Stop everything
docker-compose down

# Remove data volume
docker volume rm nexdata_postgres_data

# Start fresh
docker-compose up -d postgres

# Restore from last good backup
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_120000.sql"

# Start API
docker-compose up -d
```

## Backup Best Practices

1. **Regular backups:** Export before any major changes
2. **Test restores:** Periodically verify backups can be restored
3. **Multiple copies:** Keep backups in 2-3 different locations
4. **Name clearly:** Use descriptive names like `before_2024_census_ingest.sql`
5. **Document state:** Note which data sources and years are in each backup
6. **Retention policy:** Keep at least 7 daily backups

## File Size Guidelines

Typical backup sizes (depends on how much data you've ingested):
- **Empty database:** ~10-20 KB (just schema)
- **After census ingestion:** 1-10 MB (depends on tables/geography)
- **After SEC + Census + FRED:** 10-100 MB
- **Compressed (gzip):** 70-90% smaller

## Troubleshooting

### "PostgreSQL container is not running"
```powershell
docker-compose up -d postgres
Start-Sleep -Seconds 10
docker-compose logs postgres
```

### "Backup file not found"
Check the path:
```powershell
Get-ChildItem .\backup\
```

### Restore shows many warnings
This is normal if the database already has tables. Use `-DropExisting` to start completely fresh.

### Out of disk space
Use compression:
```powershell
.\scripts\database\export_database.ps1 -CompressGzip
```

## Manual Commands (Alternative)

If you prefer to run PostgreSQL commands directly:

### Manual Export
```bash
docker exec -i nexdata-postgres-1 pg_dump -h localhost -p 5432 -U nexdata -d nexdata > backup.sql
```

### Manual Restore
```bash
docker exec -i nexdata-postgres-1 psql -h localhost -p 5432 -U nexdata -d nexdata < backup.sql
```

### Check What's in the Database
```bash
# List all tables
docker exec -i nexdata-postgres-1 psql -U nexdata -d nexdata -c "\dt"

# Count rows in a table
docker exec -i nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT COUNT(*) FROM ingestion_jobs;"
```

## Security Notes

- Backup files contain all your ingested data
- Don't commit backup files to Git (they're in `.gitignore`)
- Store backups securely (encrypt if sharing)
- The default password is for development only (change for production)

## Next Steps

1. **Create your first backup:** `.\scripts\database\export_database.ps1`
2. **Test a restore:** Create a test database and restore to it
3. **Set up automated backups:** Schedule daily exports
4. **Document your backups:** Keep notes on what's in each backup

## Related Documentation

- [Full Scripts Documentation](../scripts/database/README.md) - Detailed parameter reference
- [Docker Compose](../docker-compose.yml) - Database configuration
- [Getting Started](GETTING_STARTED.md) - Initial setup guide

## Questions?

Common questions:
- **Can I restore to a different database name?** Yes, use `-TargetDatabase` parameter
- **Will this work on Linux/Mac?** Yes, use the `.sh` scripts instead of `.ps1`
- **Can I automate this?** Yes, schedule the export script to run daily
- **What if I only want certain tables?** Use `pg_dump` with `--table` flag manually

