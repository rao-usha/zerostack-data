# Census Geographic API Guide

## ✅ What's Been Built

You now have **4 separate API endpoints** for different geographic levels of Census data:

1. **STATE** - All 50 states + DC + territories
2. **COUNTY** - All 3,000+ US counties (or filtered by state)
3. **TRACT** - Census tracts (~4,000 people each, filtered by state/county)
4. **ZIP** - ZIP Code Tabulation Areas (ZCTAs)

## 🎯 Key Features

✅ **Fixed Metadata Parser** - Now only fetches main table (B01001), not race subtables  
✅ **Separate APIs** - Each geographic level has its own endpoint  
✅ **Data Persistence** - All data stored in Docker volume `nexdata_postgres_data`  
✅ **No Null Values** - Only real Census variables included  
✅ **GeoJSON Ready** - Infrastructure in place (temporarily disabled for stability)  

## 📍 API Endpoints

### Base URL
```
http://localhost:8001/api/v1/census/
```

### 1. State-Level Data
```bash
POST /api/v1/census/state
```

**Example Request:**
```bash
curl -X POST http://localhost:8001/api/v1/census/state \
  -H "Content-Type: application/json" \
  -d '{
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "include_geojson": false
  }'
```

**PowerShell Example:**
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B01001"
    include_geojson = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/state" `
  -Method POST -Body $body -ContentType "application/json"
```

**Returns:** 52 records (50 states + DC + Puerto Rico)

---

### 2. County-Level Data
```bash
POST /api/v1/census/county
```

**Example Request (All California counties):**
```bash
curl -X POST http://localhost:8001/api/v1/census/county \
  -H "Content-Type: application/json" \
  -d '{
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "state_fips": "06",
    "include_geojson": false
  }'
```

**PowerShell Example:**
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B01001"
    state_fips = "06"  # California
    include_geojson = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/county" `
  -Method POST -Body $body -ContentType "application/json"
```

**Returns:** 58 records (all California counties)

---

### 3. Tract-Level Data
```bash
POST /api/v1/census/tract
```

**Example Request (Los Angeles County tracts):**
```bash
curl -X POST http://localhost:8001/api/v1/census/tract \
  -H "Content-Type: application/json" \
  -d '{
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "state_fips": "06",
    "county_fips": "037",
    "include_geojson": false
  }'
```

**PowerShell Example:**
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B01001"
    state_fips = "06"     # California
    county_fips = "037"   # Los Angeles County
    include_geojson = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/tract" `
  -Method POST -Body $body -ContentType "application/json"
```

**Returns:** ~2,000+ records (all tracts in LA County)

---

### 4. ZIP Code Level Data
```bash
POST /api/v1/census/zip
```

**Example Request (California ZCTAs):**
```bash
curl -X POST http://localhost:8001/api/v1/census/zip \
  -H "Content-Type: application/json" \
  -d '{
    "survey": "acs5",
    "year": 2021,
    "table_id": "B01001",
    "state_fips": "06",
    "include_geojson": false
  }'
```

**PowerShell Example:**
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B01001"
    state_fips = "06"  # California
    include_geojson = $false
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/zip" `
  -Method POST -Body $body -ContentType "application/json"
```

---

## 📊 Querying Your Data

### Check Job Status
```bash
GET /api/v1/jobs/{job_id}
```

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/jobs/4"
```

### View Data in PostgreSQL

**Connect to database:**
```bash
docker exec -it nexdata-postgres-1 psql -U nexdata -d nexdata
```

**Query examples:**
```sql
-- View all tables
\dt

-- Top 10 California counties by population
SELECT geo_name, b01001_001e as total_population 
FROM acs5_2021_b01001 
WHERE state_fips='06' 
ORDER BY b01001_001e DESC 
LIMIT 10;

-- Male vs Female population
SELECT geo_name, 
       b01001_002e as male, 
       b01001_026e as female,
       b01001_001e as total
FROM acs5_2021_b01001 
WHERE state_fips='06'
ORDER BY total DESC
LIMIT 5;

-- Count records by type
SELECT 
  CASE 
    WHEN geo_name LIKE '%County%' THEN 'County'
    WHEN geo_name NOT LIKE '%County%' AND state_fips IS NOT NULL THEN 'State'
    ELSE 'Other'
  END as geo_type,
  COUNT(*) as count
FROM acs5_2021_b01001
GROUP BY geo_type;
```

---

## 🗺️ FIPS Codes Reference

### Common State FIPS Codes:
- `06` - California
- `36` - New York
- `48` - Texas
- `12` - Florida
- `17` - Illinois

[Full list](https://www.census.gov/library/reference/code-lists/ansi.html)

### County FIPS Examples (California):
- `037` - Los Angeles County
- `073` - San Diego County
- `085` - Santa Clara County
- `001` - Alameda County

---

## 💾 Data Persistence

All data is stored in Docker volume:
```bash
docker volume ls | Select-String "nexdata"
# Output: nexdata_postgres_data
```

**Data survives container restarts!**

To backup:
```bash
docker exec nexdata-postgres-1 pg_dump -U nexdata nexdata > backup.sql
```

To restore:
```bash
docker exec -i nexdata-postgres-1 psql -U nexdata nexdata < backup.sql
```

---

## 🎨 Common Census Tables

| Table ID | Description | Key Variables |
|----------|-------------|---------------|
| B01001 | Sex by Age | Total pop, male/female by age groups |
| B19013 | Median Household Income | Median income by geography |
| B25001 | Housing Units | Total housing units |
| B01003 | Total Population | Simple population count |
| B02001 | Race | Population by race |
| B25077 | Median Home Value | Median value of owner-occupied units |
| B23025 | Employment Status | Labor force, employed, unemployed |
| B15003 | Educational Attainment | Population by education level |

[Full table list](https://api.census.gov/data/2021/acs/acs5/groups.html)

---

## 🔥 Example Workflow

### 1. Ingest State-Level Data
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B19013"  # Median household income
    include_geojson = $false
} | ConvertTo-Json

$job1 = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/state" `
  -Method POST -Body $body -ContentType "application/json"

Write-Host "Job ID: $($job1.id)"
```

### 2. Ingest County-Level Data for California
```powershell
$body = @{
    survey = "acs5"
    year = 2021
    table_id = "B19013"
    state_fips = "06"
    include_geojson = $false
} | ConvertTo-Json

$job2 = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/county" `
  -Method POST -Body $body -ContentType "application/json"

Write-Host "Job ID: $($job2.id)"
```

### 3. Check Status
```powershell
Start-Sleep -Seconds 5
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/jobs/$($job2.id)"
```

### 4. Query Data
```bash
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
SELECT geo_name, b19013_001e as median_income 
FROM acs5_2021_b19013 
WHERE state_fips='06' 
ORDER BY median_income DESC 
LIMIT 10;
"
```

---

## 🚀 What's Next

### GeoJSON Support (Coming Soon)
Set `include_geojson: true` to fetch geographic boundaries with your data.

**Planned Features:**
- Automatic boundary fetching from Census TIGER/Line
- Simplified GeoJSON for performance
- Spatial queries (point-in-polygon, distance, etc.)
- Map visualization helpers

### Additional Geographic Levels:
- **Block Groups** - Even smaller than tracts
- **Places** - Cities and towns
- **Congressional Districts**
- **School Districts**

---

## ⚙️ Configuration

All endpoints support these parameters:

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `survey` | No | Survey type | `"acs5"` (default), `"acs1"` |
| `year` | Yes | Survey year | `2021`, `2022`, `2023` |
| `table_id` | Yes | Census table | `"B01001"`, `"B19013"` |
| `state_fips` | Varies | State filter | `"06"` for California |
| `county_fips` | No | County filter (tracts only) | `"037"` for LA County |
| `include_geojson` | No | Fetch boundaries | `true` or `false` (default) |

---

## 📈 Performance Tips

1. **Filter by state** for county/tract data to reduce API calls
2. **Disable GeoJSON** for faster ingestion (enable only when needed)
3. **Use indexed columns** (`geo_id`, `state_fips`) in WHERE clauses
4. **Batch operations** - ingest multiple tables in parallel

---

## 🐛 Troubleshooting

### Job Failed?
```powershell
$job = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/jobs/X"
Write-Host "Error: $($job.error_message)"
```

### View Logs:
```bash
docker-compose logs api --tail 100
```

### Reset Database:
```bash
docker-compose down -v  # WARNING: Deletes all data!
docker-compose up -d
```

---

## 📝 Summary

You now have a **production-ready, multi-level Census data ingestion system** with:

✅ 4 geographic API endpoints (state, county, tract, zip)  
✅ Fixed metadata parser (no more null columns)  
✅ Persistent data storage in Docker volumes  
✅ Clean separation of geographic levels  
✅ Full demographic data (49 variables from B01001 table)  
✅ Ready for GeoJSON integration  

**All data persists** across Docker restarts in the `nexdata_postgres_data` volume!



