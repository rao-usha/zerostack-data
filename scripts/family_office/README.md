# Family Office Form ADV Scripts

Scripts for ingesting SEC Form ADV data for family offices and investment advisers.

## 📁 Files in This Directory

### Ingestion Scripts

| File | Description | Usage |
|------|-------------|-------|
| `ingest_family_offices.ps1` | **PowerShell batch ingestion script** | `powershell -ExecutionPolicy Bypass -File ingest_family_offices.ps1` |
| `ingest_family_offices.sh` | **Bash batch ingestion script** | `bash ingest_family_offices.sh` |
| `ingest_all_family_offices.py` | **Python comprehensive ingestion** | `python ingest_all_family_offices.py` |
| `monitor_ingestion.ps1` | **Monitor ongoing ingestion progress** | `powershell -ExecutionPolicy Bypass -File monitor_ingestion.ps1` |

## 🚀 Quick Start

### Option 1: PowerShell (Recommended for Windows)

```powershell
cd scripts/family_office
powershell -ExecutionPolicy Bypass -File .\ingest_family_offices.ps1
```

### Option 2: Python (Cross-platform)

```bash
cd scripts/family_office
pip install requests
python ingest_all_family_offices.py
```

### Option 3: Bash (Linux/Mac)

```bash
cd scripts/family_office
bash ingest_family_offices.sh
```

## 📊 What These Scripts Do

1. **Search SEC IAPD** for investment advisers by name
2. **Fetch Form ADV data** including:
   - Business contact information
   - Key personnel
   - Assets under management
   - Registration status
3. **Store in database** (`sec_form_adv` and `sec_form_adv_personnel` tables)
4. **Track progress** via job tracking system

## ⚙️ Configuration

### Default Settings

All scripts use conservative defaults:
- **Rate limit:** 2 requests/second (SEC IAPD friendly)
- **Concurrency:** 1 (sequential processing)
- **Timeout:** 600 seconds per batch
- **API URL:** `http://localhost:8001`

### Customizing

Edit the scripts to change:
- `BASE_URL` - API endpoint
- `max_concurrency` - Parallel requests
- `max_requests_per_second` - Rate limit
- `FAMILY_OFFICES` - Target firms list

## 📝 Monitoring Progress

### While Running

Use the monitor script:

```powershell
powershell -ExecutionPolicy Bypass -File .\monitor_ingestion.ps1
```

### Via API

Check job status directly:

```bash
curl http://localhost:8001/api/v1/jobs/{job_id}
```

### Via Swagger UI

Open http://localhost:8001/docs and use the `/jobs` endpoints

## 📈 Querying Results

### Via API

```bash
# Get statistics
curl http://localhost:8001/api/v1/sec/form-adv/stats

# Query all firms
curl http://localhost:8001/api/v1/sec/form-adv/firms

# Query family offices only
curl "http://localhost:8001/api/v1/sec/form-adv/firms?family_office_only=true"
```

### Via Database

```bash
docker-compose exec postgres psql -U nexdata -d nexdata
```

```sql
-- Count firms
SELECT COUNT(*) FROM sec_form_adv;

-- Get family offices
SELECT firm_name, business_phone, business_email 
FROM sec_form_adv 
WHERE is_family_office = TRUE;

-- Get key personnel
SELECT f.firm_name, p.full_name, p.title, p.email
FROM sec_form_adv f
JOIN sec_form_adv_personnel p ON f.crd_number = p.crd_number;
```

## 🔍 Expected Results

### Important Note

Most large family offices qualify for SEC registration exemptions and will NOT be found in the IAPD database. This is normal and expected behavior.

**Typical Results:**
- Large family offices: Usually 0 found (exemptions)
- Registered RIA firms: Found successfully
- Non-U.S. entities: Not in SEC database

### Testing with Real Data

To verify the system works, try registered RIA firms:

```powershell
# Edit ingest_family_offices.ps1 and change the list to:
$USOffices = @(
    "Fisher Investments",
    "Vanguard Group",
    "Fidelity Investments"
)
```

## 📚 Documentation

For complete documentation, see:

- **API Reference:** `../../docs/FORM_ADV_API_REFERENCE.md`
- **User Guide:** `../../docs/FORM_ADV_GUIDE.md`
- **Quick Start:** `../../docs/FORM_ADV_QUICKSTART.md`
- **Swagger UI:** http://localhost:8001/docs
- **Ingestion Report:** `../../docs/FAMILY_OFFICE_INGESTION_REPORT.md`

## 🛠️ Troubleshooting

### Service Not Running

```bash
# Start the service
cd ../..
docker-compose up -d

# Check status
docker-compose ps
```

### Port Issues

If port 8001 is unavailable, update `BASE_URL` in scripts:

```python
BASE_URL = "http://localhost:8001"  # Change port here
```

### Python Dependencies

```bash
pip install requests
```

### Connection Errors

1. Verify service is running: `docker-compose ps`
2. Check API is accessible: `curl http://localhost:8001/health`
3. View logs: `docker-compose logs -f api`

## 📊 Sample Output

```
================================================================================
FAMILY OFFICE FORM ADV COMPREHENSIVE INGESTION
================================================================================

Target: 32 family offices
Regions: 4

⚠️  IMPORTANT NOTES:
   - Many family offices have SEC registration exemptions
   - Only registered investment advisers will be found
   - This is normal and expected behavior
   - We retrieve BUSINESS contact info only (not personal PII)

================================================================================
BATCH: US Family Offices
================================================================================
Ingesting 16 family offices...
✅ Job created: ID=60

⏳ Waiting for job to complete...
   [10 s] Status: success

Job completed successfully!
   Searched: 16
   Matches found: 0
   Successfully ingested: 0
```

## 🎯 Best Practices

1. **Start small** - Test with 1-2 firms first
2. **Use monitoring** - Watch progress with monitor script
3. **Check logs** - `docker-compose logs -f api` for details
4. **Query via API** - Use Swagger UI for interactive testing
5. **Respect rate limits** - Default 2 req/sec is safe

## 🔗 Related Scripts

In the parent `/scripts/` directory:

- `check_jobs.py` - Check all recent jobs
- `check_progress.py` - Monitor any ingestion
- `start_service.py` - Start the Docker service

## ⚡ Quick Commands

```bash
# Run full ingestion (PowerShell)
cd scripts/family_office && powershell -ExecutionPolicy Bypass -File .\ingest_family_offices.ps1

# Monitor progress
cd scripts/family_office && powershell -ExecutionPolicy Bypass -File .\monitor_ingestion.ps1

# Check results
curl http://localhost:8001/api/v1/sec/form-adv/stats

# View in browser
start http://localhost:8001/docs
```

## 📞 Support

For issues or questions:
1. Check service logs: `docker-compose logs -f api`
2. Review job status: `curl http://localhost:8001/api/v1/jobs/{job_id}`
3. Check database: `docker-compose exec postgres psql -U nexdata -d nexdata`
4. Consult documentation in `/docs/`

