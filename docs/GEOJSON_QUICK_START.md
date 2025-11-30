# GeoJSON Boundaries - Quick Start

## ✅ What's Available

You now have **3,331 GeoJSON boundaries** stored in your database!

### Datasets
- **`census_states_2021`** - 52 US states
- **`census_counties_ca_2021`** - 58 California counties  
- **`census_counties_us_2021`** - 3,221 US counties

---

## 🚀 Quick Examples

### 1. List All Boundary Datasets
```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/datasets"
```

### 2. Get California State Boundary
```powershell
$ca = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_states_2021/06"

# Save to file for mapping
$ca.geojson | ConvertTo-Json -Depth 100 | Out-File california.geojson
```

### 3. Get Los Angeles County Boundary
```powershell
$la = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/boundary/census_counties_us_2021/06037"

# Save to file
$la.geojson | ConvertTo-Json -Depth 100 | Out-File la_county.geojson
```

### 4. Get ALL California Counties as FeatureCollection
```powershell
$counties = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021"

# Save complete FeatureCollection
$counties | ConvertTo-Json -Depth 100 | Out-File ca_counties_complete.geojson

# Now you can open this in QGIS, Mapbox, Leaflet, etc.!
```

### 5. Search for Counties
```powershell
# Find San Diego
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/search?query=San%20Diego"
```

### 6. Get Specific States (CA, NY, TX)
```powershell
$states = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/geojson/featurecollection/census_states_2021?geo_ids=06,36,48"

$states | ConvertTo-Json -Depth 100 | Out-File three_states.geojson
```

---

## 🗺️ FIPS Code Reference

### States
- `06` - California
- `36` - New York
- `48` - Texas
- `12` - Florida
- `17` - Illinois

### Counties (5-digit)
- `06037` - Los Angeles County, CA
- `06073` - San Diego County, CA
- `06085` - Santa Clara County, CA
- `36061` - New York County (Manhattan), NY
- `48201` - Harris County (Houston), TX

---

## 📊 Joining Data with Boundaries

```sql
-- California counties with population and boundaries
SELECT 
  d.geo_name,
  d.b01001_001e as population,
  g.geojson
FROM acs5_2023_b01001 d
JOIN geojson_boundaries g 
  ON g.geo_id = SUBSTRING(d.geo_id FROM 'US(.*)')
  AND g.dataset_id = 'census_counties_ca_2021'
WHERE d.state_fips = '06'
ORDER BY d.b01001_001e DESC;
```

---

## 🎨 Visualization Tools

### QGIS
1. Export GeoJSON: `GET /api/v1/geojson/featurecollection/...`
2. Save to `.geojson` file
3. Layer → Add Vector Layer → Select the .geojson file

### Web (Leaflet.js)
```javascript
fetch('http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021')
  .then(r => r.json())
  .then(geojson => {
    L.geoJSON(geojson, {
      onEachFeature: (feature, layer) => {
        layer.bindPopup(feature.properties.NAME);
      }
    }).addTo(map);
  });
```

### Python
```python
import geopandas as gpd
import requests

url = 'http://localhost:8001/api/v1/geojson/featurecollection/census_counties_ca_2021'
geojson = requests.get(url).json()
gdf = gpd.GeoDataFrame.from_features(geojson['features'])
gdf.plot(figsize=(15, 10))
```

---

## 🔄 Re-run Script Anytime

```powershell
# Fetch boundaries again (idempotent - won't duplicate)
$env:DATABASE_URL="postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata"
.\venv\Scripts\python.exe scripts\fetch_geojson_boundaries.py
```

**Script does:**
- ✅ Fetches 52 state boundaries
- ✅ Fetches 58 CA county boundaries
- ✅ Fetches 3,221 US county boundaries
- ✅ Calculates bounding boxes
- ✅ Stores in database (updates if exists)

---

## 📖 Complete Documentation

- **`COMPLETE_SYSTEM_GUIDE.md`** - Full guide with examples
- **`census_variable_metadata.csv`** - Column definitions
- API Docs: http://localhost:8001/docs

---

**Your system now has Census data + GeoJSON boundaries ready for mapping!** 🗺️

