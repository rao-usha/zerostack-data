# Multi-Year Census Data & Column Mappings Guide

## ✅ What You Now Have

### 📊 **Data Coverage: 2020-2023**
You now have **4 years of Census data** (2020, 2021, 2022, 2023) for California counties:

- **2020:** 58 CA counties = 58 records
- **2021:** 58 CA counties = 58 records  
- **2022:** 58 CA counties = 58 records
- **2023:** 58 CA counties = 58 records

**Total: 232 county records across 4 years** ✅

### 📖 **Column Mappings Dataset**
A complete CSV file with **every column definition**: `census_variable_metadata.csv`

**Contains:**
- 196 total variable definitions (49 variables × 4 years)
- Column names (e.g., `b01001_001e`)
- Human-readable labels (e.g., "Estimate!!Total:")
- Data types and concepts

---

## 🚀 New Features

### 1. Batch Ingestion API

Ingest multiple years in one request!

**Endpoints:**
- `POST /api/v1/census/batch/state` - Batch state-level
- `POST /api/v1/census/batch/county` - Batch county-level

**Example: Get 5 years of data in one call**
```powershell
$body = @{
    survey = "acs5"
    years = @(2019, 2020, 2021, 2022, 2023)
    table_id = "B01001"
    state_fips = "06"  # California
} | ConvertTo-Json

$batch = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $body -ContentType "application/json"

Write-Host "Created $($batch.total_jobs) jobs: $($batch.job_ids -join ', ')"
```

**Check batch status:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/status?job_ids=5,6,7,8,9"
```

---

### 2. Metadata/Column Mapping API

Understand what each column means!

**Endpoints:**
- `GET /api/v1/metadata/datasets` - List datasets with metadata
- `GET /api/v1/metadata/variables/{dataset_id}` - Get all column definitions
- `GET /api/v1/metadata/search` - Search for specific variables
- `GET /api/v1/metadata/column/{dataset_id}/{column_name}` - Get single column info

**Example: Get all column definitions**
```powershell
$metadata = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/metadata/variables/acs5_2021_b01001"

Write-Host "Dataset: $($metadata.dataset_id)"
Write-Host "Total Variables: $($metadata.total_variables)`n"

# Show first 10 variables
$metadata.variables | Select-Object -First 10 | Format-Table
```

**Example: Search for specific variables**
```powershell
# Find all variables related to "male"
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/metadata/search?dataset_id=acs5_2021_b01001&query=male"

# Find all variables about "age"
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/metadata/search?dataset_id=acs5_2021_b01001&query=age"
```

**Example: Lookup a specific column**
```powershell
# What does b01001_026e mean?
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/metadata/column/acs5_2021_b01001/b01001_026e"

# Response:
# {
#   "variable_name": "B01001_026E",
#   "column_name": "b01001_026e",
#   "label": "Estimate!!Total:!!Female:",
#   "concept": "SEX BY AGE",
#   "postgres_type": "INTEGER"
# }
```

---

## 📋 Column Metadata Reference

### Key Columns in B01001 (Sex by Age)

| Column Name | Variable | Description |
|-------------|----------|-------------|
| `b01001_001e` | B01001_001E | **Total Population** |
| `b01001_002e` | B01001_002E | **Total Male** |
| `b01001_003e` | B01001_003E | Male: Under 5 years |
| `b01001_004e` | B01001_004E | Male: 5 to 9 years |
| `b01001_005e` | B01001_005E | Male: 10 to 14 years |
| `b01001_006e` | B01001_006E | Male: 15 to 17 years |
| `b01001_007e` | B01001_007E | Male: 18 and 19 years |
| ... | ... | (continues through all male age groups) |
| `b01001_026e` | B01001_026E | **Total Female** |
| `b01001_027e` | B01001_027E | Female: Under 5 years |
| `b01001_028e` | B01001_028E | Female: 5 to 9 years |
| ... | ... | (continues through all female age groups) |
| `b01001_049e` | B01001_049E | Female: 85 years and over |

**Full list:** See `census_variable_metadata.csv` (196 rows)

---

## 📊 Querying Multi-Year Data

### Example: Population Trends Over Time

```sql
-- Los Angeles County population trend 2020-2023
SELECT 
  '2020' as year,
  b01001_001e as total_pop,
  b01001_002e as male,
  b01001_026e as female
FROM acs5_2020_b01001 
WHERE geo_name = 'Los Angeles County, California'

UNION ALL

SELECT 
  '2021',
  b01001_001e,
  b01001_002e,
  b01001_026e
FROM acs5_2021_b01001 
WHERE geo_name = 'Los Angeles County, California'

UNION ALL

SELECT 
  '2022',
  b01001_001e,
  b01001_002e,
  b01001_026e
FROM acs5_2022_b01001 
WHERE geo_name = 'Los Angeles County, California'

UNION ALL

SELECT 
  '2023',
  b01001_001e,
  b01001_002e,
  b01001_026e
FROM acs5_2023_b01001 
WHERE geo_name = 'Los Angeles County, California'

ORDER BY year;
```

**Result:**
```
 year | total_pop |   male   |  female  
------+-----------+----------+----------
 2020 |  10040682 | 5006207  | 5034475
 2021 |  10019635 | 4995997  | 5023638
 2022 |   9936690 | 4952947  | 4983743
 2023 |   9848406 | 4906711  | 4941695
```

### Example: Year-over-Year Growth Rate

```sql
-- Calculate population change from 2022 to 2023
SELECT 
  t2023.geo_name,
  t2022.b01001_001e as pop_2022,
  t2023.b01001_001e as pop_2023,
  (t2023.b01001_001e - t2022.b01001_001e) as change,
  ROUND(
    ((t2023.b01001_001e::numeric - t2022.b01001_001e::numeric) / 
     t2022.b01001_001e::numeric * 100), 2
  ) as percent_change
FROM acs5_2022_b01001 t2022
JOIN acs5_2023_b01001 t2023 
  ON t2022.geo_name = t2023.geo_name
WHERE t2022.state_fips = '06'
ORDER BY percent_change DESC
LIMIT 10;
```

---

## 🔍 Understanding the Data

### Why "Estimate!!Total:!!Male:!!Under 5 years"?

Census uses a hierarchical labeling system:
- **Estimate** = This is an estimated value (vs. margin of error)
- **Total** = From the total population
- **Male** = Male population
- **Under 5 years** = Age group

The `!!` separates hierarchy levels.

### Variable Naming Convention

Census variables follow this pattern:
```
[TABLE]_[NUMBER][SUFFIX]

Examples:
- B01001_001E  = Table B01001, Variable 001, Estimate
- B01001_001M  = Table B01001, Variable 001, Margin of Error
- B01001_002E  = Table B01001, Variable 002, Estimate
```

**Suffixes:**
- `E` = Estimate (the actual value)
- `M` = Margin of Error (statistical uncertainty)
- `PE` = Percent Estimate
- `PM` = Percent Margin of Error

**We only ingest `E` (Estimate) variables** to keep data clean and usable.

---

## 📈 Data Quality

### Why is the data less sparse now?

**Before:** 300+ columns with mostly NULL values (included race subtables)
**Now:** 49 columns with actual data (only main table B01001)

**Fixed by:**
1. ✅ Better metadata filtering (no race subtables)
2. ✅ Only fetching main table variables
3. ✅ Proper Census API URL construction

---

## 🎯 Use Cases

### 1. Time Series Analysis
Track population changes over time for counties, states, or tracts.

### 2. Demographic Profiling
Understand age distribution, gender balance by geography.

### 3. Comparative Analysis
Compare counties within a state or states across the nation.

### 4. Data Joining
Join with other datasets using FIPS codes or geography names.

---

## 📦 Your Data Files

### 1. census_variable_metadata.csv
**Location:** Project root  
**Size:** 196 rows  
**Contains:** All variable definitions for 2020-2023

**Columns:**
- `dataset_id` - Which dataset (e.g., "acs5_2021_b01001")
- `variable_name` - Census variable (e.g., "B01001_001E")
- `column_name` - Database column (e.g., "b01001_001e")
- `label` - Human description (e.g., "Estimate!!Total:")
- `concept` - Overall concept (e.g., "SEX BY AGE")
- `postgres_type` - Data type (e.g., "INTEGER")

### 2. PostgreSQL Database Tables
**Location:** Docker volume `nexdata_postgres_data`

**Tables:**
- `acs5_2020_b01001` - 58 CA counties (2020)
- `acs5_2021_b01001` - 58 CA counties (2021)
- `acs5_2022_b01001` - 58 CA counties (2022)
- `acs5_2023_b01001` - 58 CA counties (2023)
- `variable_metadata` - 196 variable definitions
- `dataset_registry` - Dataset catalog
- `ingestion_jobs` - Job history

---

## 🚀 Next Steps

### Add More Years (2024, 2019, etc.)
```powershell
$body = @{
    survey = "acs5"
    years = @(2019, 2024)  # Note: 2024 may not be available yet
    table_id = "B01001"
    state_fips = "06"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $body -ContentType "application/json"
```

### Add More States
```powershell
# Texas
$body = @{
    survey = "acs5"
    years = @(2020, 2021, 2022, 2023)
    table_id = "B01001"
    state_fips = "48"  # Texas
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $body -ContentType "application/json"
```

### Add More Tables
```powershell
# Median Household Income (B19013)
$body = @{
    survey = "acs5"
    years = @(2020, 2021, 2022, 2023)
    table_id = "B19013"
    state_fips = "06"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $body -ContentType "application/json"
```

---

## 📊 Popular Census Tables

| Table ID | Description | Key Use |
|----------|-------------|---------|
| **B01001** | Sex by Age | Demographics, population pyramids |
| **B19013** | Median Household Income | Economic analysis |
| **B25077** | Median Home Value | Real estate, housing market |
| **B23025** | Employment Status | Labor force analysis |
| **B02001** | Race | Diversity metrics |
| **B15003** | Educational Attainment | Education analysis |
| **B01003** | Total Population | Simple population counts |

[Full list](https://api.census.gov/data/2021/acs/acs5/groups.html)

---

## 💡 Tips

### Export Metadata to Excel
```powershell
# Already exported to CSV, open in Excel:
Start-Process census_variable_metadata.csv
```

### Query Via PowerShell
```powershell
# Get data directly from database
$query = "SELECT * FROM acs5_2023_b01001 LIMIT 5"
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "$query"
```

### Backup Your Data
```bash
# Export all data
docker exec nexdata-postgres-1 pg_dump -U nexdata nexdata > census_backup_$(date +%Y%m%d).sql

# Restore later
docker exec -i nexdata-postgres-1 psql -U nexdata nexdata < census_backup_20251126.sql
```

---

## 📊 Summary

✅ **4 years of data** (2020-2023) for California counties  
✅ **232 total records** (58 counties × 4 years)  
✅ **49 real variables** per record (no sparse/null columns)  
✅ **196 variable definitions** with human-readable labels  
✅ **Metadata CSV exported** for easy reference  
✅ **Batch API** for multi-year ingestion  
✅ **Search API** to find specific variables  

**All data persists** in Docker volume across restarts!

Your Census data is now **comprehensive, well-documented, and ready for analysis**! 🎉



