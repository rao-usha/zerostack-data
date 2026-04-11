# Census Metadata API - Quick Reference

## ✅ CORRECTED Endpoints (Use These!)

All Census metadata endpoints are now under `/api/v1/census/metadata/`

---

## 📖 Get All Variables for a Dataset

**Endpoint:**
```
GET /api/v1/census/metadata/variables/{dataset_id}
```

**Example:**
```powershell
$metadata = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/variables/acs5_2021_b01001"

Write-Host "Dataset: $($metadata.dataset_id)"
Write-Host "Total Variables: $($metadata.total_variables)"
$metadata.variables | Select-Object -First 10 | Format-Table
```

**Response:**
```json
{
  "dataset_id": "acs5_2021_b01001",
  "total_variables": 49,
  "variables": [
    {
      "variable_name": "B01001_001E",
      "column_name": "b01001_001e",
      "label": "Estimate!!Total:",
      "concept": "SEX BY AGE",
      "predicate_type": "int",
      "postgres_type": "INTEGER"
    },
    ...
  ]
}
```

---

## 🔍 Search for Variables

**Endpoint:**
```
GET /api/v1/census/metadata/search?dataset_id={dataset_id}&query={search_term}
```

**Examples:**

```powershell
# Find all variables about "female"
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/search?dataset_id=acs5_2021_b01001&query=female"

# Find all variables about "age"
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/search?dataset_id=acs5_2021_b01001&query=age"

# Find "under 5 years"
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/search?dataset_id=acs5_2021_b01001&query=under%205"
```

**Response:**
```json
[
  {
    "variable_name": "B01001_026E",
    "column_name": "b01001_026e",
    "label": "Estimate!!Total:!!Female:",
    "concept": "SEX BY AGE",
    "predicate_type": "int",
    "postgres_type": "INTEGER"
  },
  {
    "variable_name": "B01001_027E",
    "column_name": "b01001_027e",
    "label": "Estimate!!Total:!!Female:!!Under 5 years",
    "concept": "SEX BY AGE",
    "predicate_type": "int",
    "postgres_type": "INTEGER"
  }
]
```

---

## 🎯 Lookup Specific Column

**Endpoint:**
```
GET /api/v1/census/metadata/column/{dataset_id}/{column_name}
```

**Example:**
```powershell
# What does b01001_026e mean?
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/column/acs5_2021_b01001/b01001_026e"
```

**Response:**
```json
{
  "variable_name": "B01001_026E",
  "column_name": "b01001_026e",
  "label": "Estimate!!Total:!!Female:",
  "concept": "SEX BY AGE",
  "predicate_type": "int",
  "postgres_type": "INTEGER"
}
```

---

## 📋 List All Datasets with Metadata

**Endpoint:**
```
GET /api/v1/census/metadata/datasets
```

**Example:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/datasets"
```

**Response:**
```json
[
  "acs5_2020_b01001",
  "acs5_2021_b01001",
  "acs5_2022_b01001",
  "acs5_2023_b01001"
]
```

---

## 🗄️ Database Table

**Table Name:** `census_variable_metadata`

**Schema:**
```sql
CREATE TABLE census_variable_metadata (
    id INTEGER PRIMARY KEY,
    dataset_id VARCHAR(255) NOT NULL,
    variable_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    label TEXT NOT NULL,
    concept VARCHAR(500),
    predicate_type VARCHAR(50),
    postgres_type VARCHAR(50),
    created_at TIMESTAMP NOT NULL
);
```

**Direct Query:**
```sql
-- Get all variables for 2021 dataset
SELECT variable_name, column_name, label
FROM census_variable_metadata
WHERE dataset_id = 'acs5_2021_b01001'
ORDER BY variable_name;

-- Search for female variables
SELECT variable_name, column_name, label
FROM census_variable_metadata
WHERE dataset_id = 'acs5_2021_b01001'
  AND (label ILIKE '%female%' OR concept ILIKE '%female%');

-- Count variables per dataset
SELECT dataset_id, COUNT(*) as variable_count
FROM census_variable_metadata
GROUP BY dataset_id
ORDER BY dataset_id;
```

---

## 📥 Export to CSV

```powershell
# Export all metadata
docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c `
  "\COPY (SELECT * FROM census_variable_metadata ORDER BY dataset_id, variable_name) TO STDOUT WITH CSV HEADER" `
  > census_variables_export.csv
```

---

## 🔗 Common Use Cases

### 1. Understand a Database Column

You see `b01001_026e` in your data and don't know what it means:

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/column/acs5_2021_b01001/b01001_026e"
# Returns: "Estimate!!Total:!!Female:"
```

### 2. Find All Age-Related Variables

```powershell
$ageVars = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/search?dataset_id=acs5_2021_b01001&query=years"
$ageVars | Select-Object variable_name, label
```

### 3. Compare Variable Definitions Across Years

```sql
SELECT 
  dataset_id,
  variable_name,
  label
FROM census_variable_metadata
WHERE variable_name = 'B01001_001E'
ORDER BY dataset_id;
```

### 4. Build a Data Dictionary

```powershell
# Get all variables and create a formatted table
$vars = Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/variables/acs5_2021_b01001"
$vars.variables | 
  Select-Object variable_name, column_name, label, postgres_type | 
  Export-Csv -Path "data_dictionary.csv" -NoTypeInformation
```

---

## ❌ OLD Endpoints (Don't Use!)

These were renamed for better organization:

```
❌ GET /api/v1/metadata/variables/{dataset_id}
❌ GET /api/v1/metadata/search
❌ GET /api/v1/metadata/column/{dataset_id}/{column_name}
❌ GET /api/v1/metadata/datasets
```

---

## ✅ NEW Endpoints (Use These!)

```
✅ GET /api/v1/census/metadata/variables/{dataset_id}
✅ GET /api/v1/census/metadata/search
✅ GET /api/v1/census/metadata/column/{dataset_id}/{column_name}
✅ GET /api/v1/census/metadata/datasets
```

---

## 🎯 Why the Change?

**Before:** Generic `/metadata/` endpoints mixed all sources  
**After:** Source-specific `/census/metadata/` separates concerns

**Benefits:**
- Clear which metadata belongs to Census
- Easy to add BLS metadata: `/api/v1/bls/metadata/`
- Better organization and maintainability
- No conflicts between different data sources

---

## 🔄 Migration Status

✅ **Database:** `variable_metadata` → `census_variable_metadata` (complete)  
✅ **API Endpoints:** `/metadata/` → `/census/metadata/` (complete)  
✅ **Code References:** All updated (complete)  
✅ **Data Migration:** 196 rows migrated (complete)  
✅ **Testing:** All endpoints working (verified)

---

## 📚 Related Documentation

- `FIXES_AND_IMPROVEMENTS.md` - Full list of changes
- `MULTI_YEAR_DATA_GUIDE.md` - Multi-year data usage
- `GEOGRAPHIC_API_GUIDE.md` - Geographic endpoints

---

## 🚀 Quick Test

```powershell
# Test the API is working
Invoke-RestMethod -Uri "http://localhost:8001/api/v1/census/metadata/datasets"

# Should return:
# ["acs5_2020_b01001", "acs5_2021_b01001", "acs5_2022_b01001", "acs5_2023_b01001"]
```

---

**All metadata endpoints are now properly scoped to Census!** 🎉



