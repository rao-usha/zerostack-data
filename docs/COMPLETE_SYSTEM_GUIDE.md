# Complete Census Data + GeoJSON System Guide

## 🎉 What You Have Now

### 📊 **Census Demographic Data**
- **4 years:** 2020, 2021, 2022, 2023
- **232 records:** 58 California counties × 4 years
- **49 variables:** Clean demographic data (no sparse columns)
- **4 separate tables:** One per year

### 🗺️ **GeoJSON Boundaries**
- **52 US states** + DC + territories
- **3,221 US counties** (all counties nationwide)
- **58 California counties** (dedicated dataset)
- All with bounding boxes and complete geometry

### 📖 **Column Metadata**
- **196 variable definitions** across all years
- Human-readable labels for every column
- Searchable via API
- Exported to CSV

---

## 🚀 Complete API Reference

### Census Data Ingestion

#### Single Geographic Level
```
POST /api/v1/census/state      - State level data
POST /api/v1/census/county     - County level data
POST /api/v1/census/tract      - Tract level data
POST /api/v1/census/zip        - ZIP code level data
```

#### Batch Multi-Year
```
POST /api/v1/census/batch/state   - Multiple years, state level
POST /api/v1/census/batch/county  - Multiple years, county level
GET  /api/v1/census/batch/status  - Check multiple job statuses
```

### Census Metadata
```
GET /api/v1/census/metadata/datasets              - List datasets
GET /api/v1/census/metadata/variables/{dataset}   - Get all variables
GET /api/v1/census/metadata/search                - Search variables
GET /api/v1/census/metadata/column/{dataset}/{col} - Get column info
```

### GeoJSON Boundaries
```
GET /api/v1/geojson/datasets                      - List boundary datasets
GET /api/v1/geojson/boundaries/{dataset}          - List boundaries
GET /api/v1/geojson/boundary/{dataset}/{geo_id}   - Get single boundary
GET /api/v1/geojson/featurecollection/{dataset}   - Get FeatureCollection
GET /api/v1/geojson/search                        - Search by name
```

### Job Management
```
GET /api/v1/jobs           - List all jobs
GET /api/v1/jobs/{id}      - Get job status
```

---

## 💡 Example Use Cases

### 1. Get California Boundary for Mapping

```powershell
# Get California state boundary (full GeoJSON)
$ca = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_states_2021/06"

# Save to file for use in mapping tools
$ca.geojson | ConvertTo-Json -Depth 100 | Out-File california.geojson
```

### 2. Get All California Counties as FeatureCollection

```powershell
# Get all CA counties in one GeoJSON FeatureCollection
$caCounties = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021"

# Save for mapping
$caCounties | ConvertTo-Json -Depth 100 | Out-File ca_counties.geojson

# Open in mapping tool (QGIS, Mapbox, etc.)
```

### 3. Get Population Data with Boundaries

```sql
-- Join Census data with GeoJSON boundaries
SELECT 
  d.geo_name,
  d.b01001_001e as population_2023,
  g.geojson as boundary
FROM acs5_2023_b01001 d
LEFT JOIN geojson_boundaries g 
  ON g.geo_id = d.state_fips || LPAD(SPLIT_PART(d.geo_id, 'US', 2), 3, '0')
  AND g.dataset_id = 'census_counties_us_2021'
WHERE d.state_fips = '06'
ORDER BY d.b01001_001e DESC
LIMIT 10;
```

### 4. Multi-Year Population Trends with Geography

```powershell
# PowerShell script to get trend data
$counties = @('06037', '06073', '06085')  # LA, San Diego, Santa Clara

foreach ($fips in $counties) {
    $boundary = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_counties_us_2021/$fips"
    
    Write-Host "`nCounty: $($boundary.geo_name)"
    Write-Host "FIPS: $fips"
    Write-Host "Bounding Box: [$($boundary.bbox_minx), $($boundary.bbox_miny)] to [$($boundary.bbox_maxx), $($boundary.bbox_maxy)]"
    
    # Query population for this county across years
    # (would need SQL query or additional API endpoint)
}
```

### 5. Search for Specific Counties

```powershell
# Find all counties with "San" in the name
$search = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/search?query=San&dataset_id=census_counties_ca_2021"

$search | Select-Object geo_name, geo_id | Format-Table

# Output:
# San Bernardino  06071
# San Diego       06073
# San Francisco   06075
# San Joaquin     06077
# San Luis Obispo 06079
# San Mateo       06081
```

---

## 📊 Database Structure

### Data Tables (by year)
```
acs5_2020_b01001  →  58 CA counties (2020)
acs5_2021_b01001  →  58 CA counties (2021)
acs5_2022_b01001  →  58 CA counties (2022)
acs5_2023_b01001  →  58 CA counties (2023)
```

### Metadata Tables
```
census_variable_metadata  →  196 variable definitions
dataset_registry         →  4 datasets registered
ingestion_jobs          →  8 completed jobs
```

### GeoJSON Tables
```
geojson_boundaries:
  - census_states_2021       →  52 states
  - census_counties_ca_2021  →  58 CA counties
  - census_counties_us_2021  →  3,221 US counties
```

---

## 🗺️ GeoJSON File Exports

### Export California Counties
```powershell
$caCounties = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021"
$caCounties | ConvertTo-Json -Depth 100 | Out-File ca_counties.geojson
```

### Export Specific States (CA, NY, TX)
```powershell
$states = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_states_2021?geo_ids=06,36,48"
$states | ConvertTo-Json -Depth 100 | Out-File major_states.geojson
```

### Export Specific Counties
```powershell
# Los Angeles, San Diego, Orange
$counties = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_us_2021?geo_ids=06037,06073,06059"
$counties | ConvertTo-Json -Depth 100 | Out-File socal_counties.geojson
```

---

## 🎨 Visualization Examples

### Using Leaflet.js
```javascript
// Fetch California counties with population data
fetch('http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021')
  .then(response => response.json())
  .then(geojson => {
    // Add to Leaflet map
    L.geoJSON(geojson, {
      style: feature => ({
        fillColor: getPopulationColor(feature.id),
        weight: 1,
        opacity: 1,
        color: 'white',
        fillOpacity: 0.7
      }),
      onEachFeature: (feature, layer) => {
        layer.bindPopup(`<b>${feature.properties.NAME}</b>`);
      }
    }).addTo(map);
  });
```

### Using Mapbox
```javascript
// Load GeoJSON from API
map.addSource('counties', {
  type: 'geojson',
  data: 'http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021'
});

map.addLayer({
  'id': 'counties-layer',
  'type': 'fill',
  'source': 'counties',
  'paint': {
    'fill-color': '#627BC1',
    'fill-opacity': 0.5
  }
});
```

### Using Python (Geopandas)
```python
import geopandas as gpd
import requests

# Fetch GeoJSON from API
response = requests.get('http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021')
geojson = response.json()

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame.from_features(geojson['features'])

# Plot
gdf.plot(figsize=(15, 10), edgecolor='black')
```

---

## 📈 Advanced Query Examples

### Population Density with Geography

```sql
-- Get population density for each county with boundary data
WITH pop_data AS (
  SELECT 
    SUBSTRING(geo_id FROM 'US(.*)') as fips,
    geo_name,
    b01001_001e as population
  FROM acs5_2023_b01001
  WHERE state_fips = '06'
),
geo_data AS (
  SELECT 
    geo_id,
    geo_name,
    (bbox_maxx::numeric - bbox_minx::numeric) * 
    (bbox_maxy::numeric - bbox_miny::numeric) * 69.0 * 69.0 as approx_area_sq_mi,
    geojson
  FROM geojson_boundaries
  WHERE dataset_id = 'census_counties_ca_2021'
)
SELECT 
  p.geo_name,
  p.population,
  ROUND(g.approx_area_sq_mi::numeric, 2) as area_sq_mi,
  ROUND((p.population / g.approx_area_sq_mi)::numeric, 2) as pop_density
FROM pop_data p
JOIN geo_data g ON p.fips = g.geo_id
ORDER BY pop_density DESC
LIMIT 10;
```

### Time Series with Spatial Context

```sql
-- Compare 2020 vs 2023 with county boundaries
SELECT 
  t2020.geo_name,
  t2020.b01001_001e as pop_2020,
  t2023.b01001_001e as pop_2023,
  (t2023.b01001_001e - t2020.b01001_001e) as change,
  ROUND(
    ((t2023.b01001_001e::numeric - t2020.b01001_001e::numeric) / 
     t2020.b01001_001e::numeric * 100), 2
  ) as pct_change,
  g.geojson
FROM acs5_2020_b01001 t2020
JOIN acs5_2023_b01001 t2023 ON t2020.geo_name = t2023.geo_name
LEFT JOIN geojson_boundaries g 
  ON g.dataset_id = 'census_counties_ca_2021'
  AND g.geo_name = REPLACE(t2020.geo_name, ', California', '')
WHERE t2020.state_fips = '06'
ORDER BY pct_change DESC
LIMIT 10;
```

---

## 🛠️ Scripts Available

### `scripts/fetch_geojson_boundaries.py`

**What it does:**
- Fetches state and county boundaries from reliable sources
- Stores in database with bounding boxes
- Runs independently without API

**Run it:**
```bash
# Set database URL (already done in .env)
$env:DATABASE_URL="postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata"
.\venv\Scripts\python.exe scripts\fetch_geojson_boundaries.py
```

**Output:**
```
✅ Stored 52 state boundaries
✅ Stored 58 California county boundaries
✅ Stored 3221 US county boundaries
```

---

## 📁 Files in Your Project

### Data Files
```
census_variable_metadata.csv         - 196 variable definitions
census_variable_metadata_updated.csv - Updated export (same data)
```

### Documentation
```
README.md                    - Project overview
RULES.md                     - Architectural rules
COMPLETE_SYSTEM_GUIDE.md     - This file!
```

### Scripts
```
scripts/fetch_geojson_boundaries.py  - GeoJSON boundary fetcher
scripts/test_simple.py               - Test script (can delete)
```

---

## 🔥 Quick Test Examples

### Test 1: List All Boundary Datasets
```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/datasets"

# Output:
# census_counties_ca_2021 | county | 58 features
# census_counties_us_2021 | county | 3221 features
# census_states_2021      | state  | 52 features
```

### Test 2: Get California State Boundary
```powershell
$ca = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_states_2021/06"

# Save to file
$ca.geojson | ConvertTo-Json -Depth 100 | Out-File california_state.geojson
```

### Test 3: Get Los Angeles County Boundary
```powershell
$la = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_counties_us_2021/06037"

# Save to file
$la.geojson | ConvertTo-Json -Depth 100 | Out-File los_angeles_county.geojson
```

### Test 4: Search for Counties
```powershell
# Find all counties with "San" in name
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/search?query=San&dataset_id=census_counties_ca_2021" | 
  Select-Object geo_name, geo_id | Format-Table
```

### Test 5: Get California Counties FeatureCollection
```powershell
# Complete GeoJSON FeatureCollection for all CA counties
$fc = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021"

Write-Host "Type: $($fc.type)"
Write-Host "Features: $($fc.features.Count)"
Write-Host "Feature 1: $($fc.features[0].properties.NAME)"
```

### Test 6: Look Up Column Meaning
```powershell
# What does b01001_026e mean?
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/column/acs5_2021_b01001/b01001_026e"

# Output:
# {
#   "label": "Estimate!!Total:!!Female:",
#   "concept": "SEX BY AGE",
#   "postgres_type": "INTEGER"
# }
```

---

## 🎯 Example Workflows

### Workflow 1: Ingest + Export for Visualization

```powershell
# 1. Ingest Census data for multiple years
$batch = @{
    survey = "acs5"
    years = @(2020, 2021, 2022, 2023)
    table_id = "B19013"  # Median income
    state_fips = "06"
} | ConvertTo-Json

$jobs = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $batch -ContentType "application/json"

Write-Host "Created jobs: $($jobs.job_ids -join ', ')"

# 2. Wait for completion (30 seconds)
Start-Sleep -Seconds 30

# 3. Check status
$status = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/status?job_ids=$($jobs.job_ids -join ',')"
Write-Host "Status: $($status.status_counts | ConvertTo-Json)"

# 4. Export boundaries
$boundaries = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021"
$boundaries | ConvertTo-Json -Depth 100 | Out-File ca_counties_boundaries.geojson

Write-Host "`n✅ Data ready! Query from database and map using ca_counties_boundaries.geojson"
```

### Workflow 2: Build a Choropleth Map

```sql
-- Export data for choropleth (population by county)
\COPY (
  SELECT 
    SUBSTRING(d.geo_id FROM 'US(.*)') as county_fips,
    d.geo_name as county_name,
    d.b01001_001e as population_2023,
    g.geojson
  FROM acs5_2023_b01001 d
  LEFT JOIN geojson_boundaries g 
    ON g.geo_id = SUBSTRING(d.geo_id FROM 'US(.*)')
    AND g.dataset_id = 'census_counties_ca_2021'
  WHERE d.state_fips = '06'
) TO '/tmp/ca_pop_with_geo.csv' WITH CSV HEADER;
```

---

## 🗄️ Data Persistence

All data stored in Docker volume: **`nexdata_postgres_data`**

**Survives:**
- ✅ Container restarts
- ✅ `docker-compose down` (without `-v`)
- ✅ System reboots

**Backup:**
```bash
docker exec nexdata-postgres-1 pg_dump -U nexdata nexdata > census_backup_$(date +%Y%m%d).sql
```

**Restore:**
```bash
docker exec -i nexdata-postgres-1 psql -U nexdata nexdata < census_backup_20251129.sql
```

---

## 📊 Current Database Stats

```sql
-- Summary query
SELECT 
  'Census Data Tables' as category,
  COUNT(DISTINCT table_name) as count
FROM dataset_registry
WHERE source = 'census'

UNION ALL

SELECT 
  'Variable Definitions',
  COUNT(*)
FROM census_variable_metadata

UNION ALL

SELECT 
  'GeoJSON Boundaries',
  COUNT(*)
FROM geojson_boundaries

UNION ALL

SELECT 
  'Completed Jobs',
  COUNT(*)
FROM ingestion_jobs
WHERE status = 'success';
```

**Results:**
```
Category                  | Count
--------------------------|-------
Census Data Tables        | 4
Variable Definitions      | 196
GeoJSON Boundaries        | 3,331
Completed Jobs            | 7
```

---

## 🎨 Mapping Tools Integration

### QGIS
1. Export GeoJSON: `GET /api/v1/geojson/featurecollection/...`
2. Save to `.geojson` file
3. Import into QGIS: Layer → Add Layer → Vector Layer
4. Join with CSV data exported from database

### Mapbox
```javascript
map.addSource('counties', {
  type: 'geojson',
  data: 'http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021'
});
```

### Leaflet
```javascript
fetch('http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021')
  .then(r => r.json())
  .then(data => L.geoJSON(data).addTo(map));
```

### Python/Geopandas
```python
import geopandas as gpd
import requests

url = 'http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021'
geojson = requests.get(url).json()
gdf = gpd.GeoDataFrame.from_features(geojson['features'])
gdf.plot()
```

---

## 🚀 What's Next?

### 1. Add More Years
```powershell
# 2024 (if available), 2019, 2018...
$body = @{
    survey = "acs5"
    years = @(2019, 2024)
    table_id = "B01001"
    state_fips = "06"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
  -Method POST -Body $body -ContentType "application/json"
```

### 2. Add More States
```powershell
# Texas, New York, Florida
$states = @("48", "36", "12")
foreach ($state in $states) {
    $body = @{
        survey = "acs5"
        years = @(2020, 2021, 2022, 2023)
        table_id = "B01001"
        state_fips = $state
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/batch/county" `
      -Method POST -Body $batch -ContentType "application/json"
}
```

### 3. Add More Tables
```powershell
# Median Income (B19013)
# Median Home Value (B25077)
# Employment (B23025)
$tables = @("B19013", "B25077", "B23025")
foreach ($table in $tables) {
    $body.table_id = $table
    # ... create jobs
}
```

### 4. Add Tract-Level Data
```powershell
# Los Angeles County tracts (will be many!)
$body = @{
    survey = "acs5"
    year = 2023
    table_id = "B01001"
    state_fips = "06"
    county_fips = "037"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/tract" `
  -Method POST -Body $body -ContentType "application/json"
```

---

## ✅ System Summary

**Census Data:**
- ✅ 4 years (2020-2023)
- ✅ 232 county records
- ✅ 49 variables per record
- ✅ No sparse/null columns

**GeoJSON Boundaries:**
- ✅ 52 US states
- ✅ 3,221 US counties
- ✅ All with geometry & bounding boxes

**Metadata:**
- ✅ 196 variable definitions
- ✅ Searchable via API
- ✅ Exported to CSV

**APIs:**
- ✅ Single & batch ingestion
- ✅ Metadata lookup & search
- ✅ GeoJSON boundary queries
- ✅ Full CRUD for jobs

**Persistence:**
- ✅ Docker volume
- ✅ Survives restarts
- ✅ Backupable via pg_dump

---

## 🎊 Complete!

You now have a **fully functional Census data + GeoJSON system** ready for:
- Time series analysis
- Geographic visualization
- Demographic studies
- Spatial analysis
- Multi-year comparisons

**Everything is documented, tested, and working!** 🚀

See API docs: **http://localhost:8001/docs**

