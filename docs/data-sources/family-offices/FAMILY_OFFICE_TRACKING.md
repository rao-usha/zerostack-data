# Family Office Tracking System

**Status:** ✅ Fully Implemented  
**Last Updated:** 2025-11-30

## Overview

The Family Office Tracking System provides a comprehensive database and API for managing information about family offices, regardless of SEC registration status. This is separate from and complementary to the SEC Form ADV data.

### Key Features

- **General-purpose tracking** - Works for ALL family offices (registered or not)
- **Contact management** - Store addresses, phones, emails, key personnel
- **Investment profiles** - Track investment focus, sectors, check sizes
- **Interaction history** - Log outreach, meetings, follow-ups
- **RESTful API** - Full CRUD operations via HTTP endpoints
- **Swagger documentation** - Interactive API docs at `/docs`

## Database Schema

### Tables

#### 1. `family_offices`
Main family office tracking table with:
- **Identifiers:** name (unique), legal_name
- **Classification:** region, country, type
- **Contact Info:** address, city, state, postal, phone, email, website, LinkedIn
- **Principals:** principal_family, principal_name, estimated_wealth
- **Investment Profile:** investment_focus[], sectors_of_interest[], geographic_focus[], stage_preference[], check_size_range
- **Investment Details:** investment_thesis, notable_investments[]
- **Data Quality:** data_sources[], sec_crd_number, sec_registered
- **Scale:** estimated_aum, employee_count
- **Status:** status, actively_investing, accepts_outside_capital
- **Metadata:** first_researched_date, last_updated_date, last_verified_date, notes

#### 2. `family_office_contacts`
Detailed contact information for key personnel:
- full_name, title, role
- email, phone, linkedin_url
- bio, previous_experience[], education[]
- investment_areas[], sectors[]
- is_primary_contact, status
- data_source, last_verified

#### 3. `family_office_interactions`
Track outreach and engagement:
- family_office_id, contact_id
- interaction_date, interaction_type
- subject, notes, outcome
- next_action, next_action_date
- created_by

## API Endpoints

Base URL: `http://localhost:8001/api/v1/family-offices`

### Create / Update

#### `POST /api/v1/family-offices/`
Create or update a family office record. If a family office with the same name exists, it updates.

**Request Body:**
```json
{
  "name": "Soros Fund Management",
  "region": "US",
  "country": "United States",
  "principal_family": "Soros",
  "principal_name": "George Soros",
  "estimated_wealth": "$8B+",
  "city": "New York",
  "state_province": "NY",
  "main_phone": "+1-212-555-0100",
  "main_email": "info@example.com",
  "website": "https://www.soros.com",
  "linkedin": "https://linkedin.com/company/soros",
  "investment_focus": ["Private Equity", "Venture Capital"],
  "sectors_of_interest": ["AI/ML", "Healthcare", "Climate Tech"],
  "geographic_focus": ["Global"],
  "check_size_range": "$10M-$100M",
  "investment_thesis": "Focus on transformative technology and social impact",
  "status": "Active",
  "notes": "Founded by George Soros"
}
```

**PowerShell Example:**
```powershell
$body = @{
    name = "Soros Fund Management"
    region = "US"
    country = "United States"
    principal_family = "Soros"
    city = "New York"
    state_province = "NY"
    investment_focus = @("Private Equity", "Venture Capital")
    sectors_of_interest = @("AI/ML", "Healthcare")
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/family-offices/" `
    -Method Post `
    -Body $body `
    -ContentType "application/json"
```

**Python Example:**
```python
import requests

data = {
    "name": "Soros Fund Management",
    "region": "US",
    "country": "United States",
    "principal_family": "Soros",
    "city": "New York",
    "state_province": "NY",
    "investment_focus": ["Private Equity", "Venture Capital"],
    "sectors_of_interest": ["AI/ML", "Healthcare"]
}

response = requests.post(
    "http://localhost:8001/api/v1/family-offices/",
    json=data
)
print(response.json())
```

### Query

#### `GET /api/v1/family-offices/`
List all family offices with filtering and pagination.

**Query Parameters:**
- `limit` - Max results (default: 100, max: 1000)
- `offset` - Pagination offset
- `region` - Filter by region (US, Europe, Asia, etc.)
- `country` - Filter by country
- `status` - Filter by status (Active, Inactive, etc.)

**Examples:**
```powershell
# Get all family offices
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices"

# Filter by region
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?region=US"

# Filter by country and status
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?country=United States&status=Active"

# Pagination
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?limit=50&offset=100"
```

#### `GET /api/v1/family-offices/{office_id}`
Get detailed information for a specific family office.

**Example:**
```powershell
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/1"
```

Returns complete profile including contacts and investment preferences.

#### `GET /api/v1/family-offices/stats/overview`
Get aggregate statistics.

**Example:**
```powershell
$stats = Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/stats/overview"
$stats | ConvertTo-Json -Depth 3
```

**Response:**
```json
{
  "total_family_offices": 12,
  "by_region": {
    "us": 12,
    "europe": 0,
    "asia": 0
  },
  "active_count": 12,
  "contact_info_completeness": {
    "with_email": 2,
    "with_phone": 5,
    "with_website": 9
  },
  "sec_registered": 0
}
```

### Delete

#### `DELETE /api/v1/family-offices/{office_id}`
Delete a family office record (also deletes associated contacts and interactions via CASCADE).

**Example:**
```powershell
Invoke-RestMethod "http://localhost:8001/api/v1/family-offices/1" -Method Delete
```

## Data Loading

### Method 1: PowerShell Script

Load from JSON file:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/family_office/load_data.ps1
```

Default input: `data/family_offices_data.json`

### Method 2: Manual API Calls

```powershell
$offices = Get-Content "data/family_offices_data.json" -Raw | ConvertFrom-Json

foreach ($office in $offices) {
    $body = $office | ConvertTo-Json -Depth 5 -Compress
    Invoke-RestMethod -Uri "http://localhost:8001/api/v1/family-offices/" `
        -Method Post `
        -Body $body `
        -ContentType "application/json"
}
```

### Method 3: CSV Import (Future)

```python
python scripts/family_office/import_from_csv.py data/family_offices_template.csv
```

## Sample Data

The system includes 12 pre-loaded US family offices:

| Name                  | City        | State | Wealth       | Focus Areas                        |
|-----------------------|-------------|-------|--------------|-------------------------------------|
| Soros Fund Management | New York    | NY    | $8B+         | PE, VC, Public Equities            |
| Pritzker Group        | Chicago     | IL    | $15B+        | PE, VC, Real Estate                |
| Cascade Investment    | Kirkland    | WA    | $100B+       | Public Equities, PE, Real Estate   |
| MSD Capital           | New York    | NY    | $50B+        | PE, Public Equities, Real Estate   |
| Emerson Collective    | Palo Alto   | CA    | $20B+        | VC, Impact Investing               |
| Bezos Expeditions     | -           | -     | $150B+       | VC, PE, Space Tech                 |
| Walton Family Office  | Bentonville | AR    | $200B+       | Public Equities, PE, Real Estate   |
| Ballmer Group         | -           | -     | $100B+       | Impact Investing                   |
| Arnold Ventures       | Houston     | TX    | $5B+         | Impact Investing, Philanthropy     |
| Raine Group           | New York    | NY    | -            | PE, Merchant Banking               |
| Hewlett Foundation    | Menlo Park  | CA    | $10B+        | Philanthropy, Impact Investing     |
| Packard Foundation    | Los Altos   | CA    | $8B+         | Philanthropy, Impact Investing     |

## Data Fields Guide

### Required Fields
- `name` - Family office name (must be unique)

### Recommended Fields
- `region` - US, Europe, Asia, LatAm, Middle East
- `country` - Full country name
- `city`, `state_province` - Location
- `principal_family` - Family name (e.g., "Soros", "Gates")
- `investment_focus` - Array: ["Private Equity", "Venture Capital", "Real Estate", etc.]
- `sectors_of_interest` - Array: ["AI/ML", "Healthcare", "Climate Tech", etc.]
- `status` - Active, Inactive, Unknown

### Optional Fields
- `legal_name` - Official legal entity name
- `principal_name` - Main family member
- `estimated_wealth` - Free text (e.g., "$100B+", "$10-50B")
- `headquarters_address` - Street address
- `postal_code` - ZIP/postal code
- `main_phone` - Primary phone number
- `main_email` - General contact email
- `website` - Company website URL
- `linkedin` - LinkedIn company page URL
- `geographic_focus` - Investment geography array
- `stage_preference` - Investment stage array (Seed, Series A, Growth, etc.)
- `check_size_range` - Typical investment size (e.g., "$10M-$100M")
- `investment_thesis` - Free text description
- `notable_investments` - Array of known investments
- `sec_crd_number` - If SEC registered, link to Form ADV
- `sec_registered` - Boolean
- `estimated_aum` - Assets under management estimate
- `employee_count` - Team size (e.g., "10-50", "100+")
- `actively_investing` - Boolean
- `accepts_outside_capital` - Boolean
- `notes` - Free-form notes

## Querying the Database Directly

```sql
-- All family offices
SELECT name, city, state_province, investment_focus, estimated_wealth
FROM family_offices
ORDER BY name;

-- By region
SELECT name, city, principal_family, website
FROM family_offices
WHERE region = 'US' AND status = 'Active'
ORDER BY name;

-- With contact info
SELECT name, main_phone, main_email, website
FROM family_offices
WHERE main_email IS NOT NULL OR main_phone IS NOT NULL
ORDER BY name;

-- By investment focus (array contains)
SELECT name, investment_focus, sectors_of_interest
FROM family_offices
WHERE 'Venture Capital' = ANY(investment_focus)
ORDER BY name;

-- Statistics by region
SELECT region, COUNT(*) as count
FROM family_offices
GROUP BY region
ORDER BY count DESC;
```

## Swagger UI

Access interactive API documentation:

1. Open browser: `http://localhost:8001/docs`
2. Navigate to **"Family Offices - Tracking"** section
3. Test all endpoints directly in browser
4. See request/response schemas
5. Try sample queries

## Use Cases

### 1. Manual Research Entry
Add family offices you discover through research:
```powershell
$body = @{
    name = "New Family Office LLC"
    region = "US"
    city = "San Francisco"
    state_province = "CA"
    website = "https://example.com"
    investment_focus = @("Venture Capital")
    sectors_of_interest = @("AI/ML", "Robotics")
    notes = "Found via LinkedIn search"
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8001/api/v1/family-offices/" `
    -Method Post -Body $body -ContentType "application/json"
```

### 2. Bulk Import from Spreadsheet
1. Export your spreadsheet to CSV
2. Convert to JSON:
   ```powershell
   $csv = Import-Csv "family_offices.csv"
   $json = $csv | ConvertTo-Json
   $json | Out-File "family_offices.json"
   ```
3. Load via script:
   ```powershell
   powershell scripts/family_office/load_data.ps1 -JsonFile "family_offices.json"
   ```

### 3. Generate Reports
```powershell
# Get all US family offices by city
$offices = Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?region=US&limit=1000"
$offices.offices | 
    Group-Object {$_.location.state_province} |
    Sort-Object Count -Descending |
    Format-Table Name, Count
```

### 4. Integration with CRM
Pull data for integration:
```python
import requests
import pandas as pd

response = requests.get("http://localhost:8001/api/v1/family-offices?limit=1000")
offices = response.json()["offices"]

# Convert to DataFrame
df = pd.DataFrame(offices)
df.to_csv("family_offices_export.csv", index=False)
```

## Data Quality Tips

1. **Keep names consistent** - Use official names
2. **Update regularly** - Set `last_updated_date`
3. **Source your data** - Use `data_sources` array
4. **Verify contacts** - Update `last_verified_date`
5. **Tag SEC-registered** - Set `sec_crd_number` if applicable
6. **Use arrays properly** - Multiple values in investment_focus, sectors, etc.

## Relationship to SEC Form ADV Data

| Feature                  | Family Office Tracking | SEC Form ADV |
|--------------------------|------------------------|--------------|
| **Source**               | Manual research        | SEC API      |
| **Coverage**             | ALL family offices     | Registered only |
| **Data Quality**         | You control            | Official filings |
| **Update Frequency**     | Manual                 | Automated    |
| **Contact Info**         | Flexible               | Business only |
| **Investment Details**   | Flexible notes         | Structured   |
| **Best For**             | Prospecting, tracking  | Due diligence|

**Recommendation:** Use both systems together:
- Use this system for comprehensive tracking
- Cross-reference with SEC Form ADV data when available
- Link via `sec_crd_number` field

## Backup and Export

### Export all data to JSON
```powershell
$offices = Invoke-RestMethod "http://localhost:8001/api/v1/family-offices?limit=10000"
$offices.offices | ConvertTo-Json -Depth 10 | Out-File "backup_$(Get-Date -Format 'yyyyMMdd').json"
```

### Database backup
```bash
docker-compose exec postgres pg_dump -U nexdata -d nexdata -t family_offices -t family_office_contacts -t family_office_interactions > family_offices_backup.sql
```

## Troubleshooting

### Issue: Office not appearing in results
- Check `status` field (might be "Inactive")
- Verify `name` is unique (duplicates update existing)

### Issue: Arrays not saving
- Ensure arrays are passed as JSON arrays, not strings
- PowerShell: Use `@("item1", "item2")`
- Python: Use `["item1", "item2"]`

### Issue: Cannot delete office
- Check for dependent records (contacts, interactions)
- They will be CASCADE deleted automatically

## Future Enhancements

Potential additions (implement when explicitly requested):
- [ ] Contact tracking API endpoints
- [ ] Interaction logging endpoints
- [ ] Search by investment focus/sectors
- [ ] Automated data enrichment
- [ ] Email/phone validation
- [ ] Duplicate detection
- [ ] Data quality scoring
- [ ] Export to Excel/CSV
- [ ] Bulk update operations
- [ ] Change history tracking

## Support

- **Swagger UI:** `http://localhost:8001/docs`
- **Database:** Connect directly to PostgreSQL (`nexdata` database)
- **Logs:** `docker-compose logs -f api`

