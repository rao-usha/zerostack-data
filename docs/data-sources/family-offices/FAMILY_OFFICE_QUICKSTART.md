# Family Office System - Quick Reference

**Status:** ✅ Fully Operational | **Records:** 12 US Family Offices | **Last Updated:** 2025-11-30

## 🚀 Quick Access

- **Swagger UI:** http://localhost:8001/docs (Interactive API docs)
- **Documentation:** [docs/FAMILY_OFFICE_TRACKING.md](docs/FAMILY_OFFICE_TRACKING.md)
- **Implementation Summary:** [docs/FAMILY_OFFICE_IMPLEMENTATION_SUMMARY.md](docs/FAMILY_OFFICE_IMPLEMENTATION_SUMMARY.md)

## 📊 What's In The System

**12 Major US Family Offices:**
- Soros Fund Management ($8B+)
- Pritzker Group ($15B+)
- Cascade Investment ($100B+ - Gates)
- MSD Capital ($50B+ - Dell)
- Emerson Collective ($20B+ - Powell Jobs)
- Bezos Expeditions ($150B+)
- Walton Family Office ($200B+)
- Ballmer Group ($100B+)
- Arnold Ventures ($5B+)
- Raine Group
- Hewlett Foundation ($10B+)
- Packard Foundation ($8B+)

## 🔌 Most Common Operations

### View All Family Offices
```powershell
$offices = Invoke-RestMethod "http://localhost:8001/api/v1/family-offices"
$offices.offices | Format-Table name, estimated_wealth
```

### Get Statistics
```powershell
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/stats/overview" | ConvertTo-Json
```

### Add New Family Office
```powershell
$body = @{
    name = "Smith Family Office"
    region = "US"
    country = "United States"
    city = "San Francisco"
    state_province = "CA"
    main_phone = "+1-415-555-0100"
    main_email = "contact@example.com"
    website = "https://example.com"
    investment_focus = @("Venture Capital", "Private Equity")
    sectors_of_interest = @("AI/ML", "Healthcare")
    check_size_range = '$10M-$50M'
    status = "Active"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/family-offices/" `
    -Method Post -Body $body -ContentType "application/json"
```

### Query By Region
```powershell
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?region=US&limit=50"
```

### Get Detailed Profile
```powershell
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/1" | ConvertTo-Json -Depth 5
```

## 🗄️ Database Access

### Connect to Database
```bash
docker-compose exec postgres psql -U nexdata -d nexdata
```

### Common Queries
```sql
-- All family offices
SELECT name, city, state_province, estimated_wealth FROM family_offices;

-- With contact info
SELECT name, main_phone, main_email, website 
FROM family_offices 
WHERE main_email IS NOT NULL;

-- By investment focus
SELECT name, investment_focus 
FROM family_offices 
WHERE 'Venture Capital' = ANY(investment_focus);
```

## 📥 Bulk Load Data

```powershell
# Load from JSON file
powershell -ExecutionPolicy Bypass -File scripts/family_office/load_data.ps1

# Or load custom file
$offices = Get-Content "my_offices.json" -Raw | ConvertFrom-Json
foreach ($office in $offices) {
    $body = $office | ConvertTo-Json -Depth 5
    Invoke-RestMethod -Uri "http://localhost:8001/api/v1/family-offices/" `
        -Method Post -Body $body -ContentType "application/json"
}
```

## 🎯 Key Fields

**Required:**
- `name` (unique identifier)

**Most Useful:**
- `region` (US, Europe, Asia, etc.)
- `city`, `state_province`
- `principal_family` (family name)
- `main_phone`, `main_email`, `website`
- `investment_focus` (array: PE, VC, Real Estate, etc.)
- `sectors_of_interest` (array: AI/ML, Healthcare, etc.)
- `check_size_range` (e.g., "$10M-$100M")
- `status` (Active, Inactive)

## 🔍 Search & Filter

```powershell
# By region
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?region=US"

# By country
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?country=United%20States"

# By status
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?status=Active"

# Pagination
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?limit=50&offset=0"
```

## 🐍 Python Examples

```python
import requests

# Get all family offices
response = requests.get("http://localhost:8001/api/v1/family-offices")
offices = response.json()["offices"]

for office in offices:
    print(f"{office['name']} - {office['location']['city']}")

# Add new office
data = {
    "name": "Example Family Office",
    "region": "US",
    "city": "Boston",
    "state_province": "MA",
    "investment_focus": ["Venture Capital"],
    "sectors_of_interest": ["Healthcare", "Biotech"]
}

response = requests.post(
    "http://localhost:8001/api/v1/family-offices/",
    json=data
)
print(response.json())
```

## 📊 Export Data

```powershell
# Export to JSON
$offices = Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?limit=1000"
$offices.offices | ConvertTo-Json -Depth 10 | Out-File "export_$(Get-Date -Format 'yyyyMMdd').json"

# Export to CSV (via SQL)
docker-compose exec postgres psql -U nexdata -d nexdata -c "\COPY (SELECT name, city, state_province, main_phone, main_email, website FROM family_offices) TO STDOUT WITH CSV HEADER" > family_offices.csv
```

## 🔧 Database Tables

1. **`family_offices`** - Main tracking table (12 records)
2. **`family_office_contacts`** - Personnel details (ready for use)
3. **`family_office_interactions`** - Engagement tracking (ready for use)

## ✅ System Health Check

```powershell
# Quick verification
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/stats/overview"
```

Expected output:
```json
{
  "total_family_offices": 12,
  "by_region": { "us": 12, "europe": 0, "asia": 0 },
  "active_count": 12,
  "contact_info_completeness": {
    "with_email": 0,
    "with_phone": 0,
    "with_website": 9
  },
  "sec_registered": 0
}
```

## 🆘 Troubleshooting

### API not responding?
```bash
docker-compose restart api
docker-compose logs -f api
```

### Need to reset data?
```sql
-- Connect to database
docker-compose exec postgres psql -U nexdata -d nexdata

-- Delete all records
TRUNCATE family_offices CASCADE;

-- Reload sample data
-- (then run load_data.ps1 again)
```

### Can't see new endpoints in Swagger?
```bash
# Hard refresh browser (Ctrl+Shift+R)
# Or restart API
docker-compose restart api
```

## 📚 Full Documentation

- **Complete Guide:** [docs/FAMILY_OFFICE_TRACKING.md](docs/FAMILY_OFFICE_TRACKING.md)
- **Implementation Details:** [docs/FAMILY_OFFICE_IMPLEMENTATION_SUMMARY.md](docs/FAMILY_OFFICE_IMPLEMENTATION_SUMMARY.md)
- **Data Sources Checklist:** [docs/EXTERNAL_DATA_SOURCES.md](docs/EXTERNAL_DATA_SOURCES.md) (Section 15)

## 🎉 You're Ready!

The system is **fully operational** with:
- ✅ 12 sample family offices loaded
- ✅ Full CRUD API available
- ✅ Swagger UI documentation
- ✅ Database tables created and indexed
- ✅ Bulk loading scripts ready

**Start adding your family offices now!** 🚀

