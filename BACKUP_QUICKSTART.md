# 🚀 Database Backup Quick Start

## Export Your Database (Right Now!)

```powershell
.\scripts\database\export_database.ps1
```

✅ Your backup will be saved to: `backup/nexdata_backup_YYYYMMDD_HHMMSS.sql`

---

## Restore a Database

```powershell
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_120000.sql"
```

✅ Your database is now restored with all your ingested data!

---

## That's It!

For more details, see: [docs/DATABASE_BACKUP_GUIDE.md](docs/DATABASE_BACKUP_GUIDE.md)

---

## Common Commands

### Before making changes:
```powershell
.\scripts\database\export_database.ps1 -OutputPath ".\backup\before_changes.sql"
```

### Compressed backup (smaller file):
```powershell
.\scripts\database\export_database.ps1 -CompressGzip
```

### Fresh start from backup:
```powershell
.\scripts\database\restore_database.ps1 -BackupPath ".\backup\my_backup.sql" -DropExisting
```

---

## Stand Up Database on New Machine

1. Clone repo & start database:
   ```powershell
   git clone <your-repo>
   cd Nexdata
   docker-compose up -d postgres
   ```

2. Copy your `.sql` backup file to the `backup/` folder

3. Restore:
   ```powershell
   .\scripts\database\restore_database.ps1 -BackupPath ".\backup\nexdata_backup_20241201_120000.sql"
   ```

4. Start everything:
   ```powershell
   docker-compose up -d
   ```

5. Test: http://localhost:8001/docs

Done! 🎉

