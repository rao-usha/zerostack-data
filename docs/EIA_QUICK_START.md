# EIA (Energy Information Administration) Quick Start Guide

This guide helps you quickly start ingesting EIA data into your PostgreSQL database.

## Prerequisites

### 1. Get an EIA API Key

EIA requires an API key for all requests. Get a free key:

1. Visit: https://www.eia.gov/opendata/register.php
2. Fill out the registration form
3. You'll receive your API key via email immediately

### 2. Configure API Key

Add your API key to `.env`:

```bash
EIA_API_KEY=your_eia_api_key_here
```

## Available Data Categories

The EIA API provides access to:

### 1. Petroleum Data
- **Consumption**: Petroleum consumption by product and region
- **Production**: Petroleum production by product and region
- **Imports**: Petroleum imports by product
- **Exports**: Petroleum exports by product
- **Stocks**: Petroleum inventory levels

### 2. Natural Gas Data
- **Consumption**: Natural gas consumption by sector and region
- **Production**: Natural gas production by region
- **Storage**: Natural gas storage levels
- **Prices**: Natural gas prices by region

### 3. Electricity Data
- **Retail Sales**: Electricity retail sales by sector and state
- **Generation**: Electricity generation by fuel type
- **Revenue**: Electricity revenue by sector
- **Customers**: Number of electricity customers

### 4. Retail Gas Prices
- Regular, midgrade, premium, and diesel prices
- Weekly and daily data
- National and state-level data

### 5. STEO (Short-Term Energy Outlook) Projections
- Monthly projections for energy supply, demand, and prices
- Forward-looking forecasts

## Quick Examples

### Example 1: Ingest Petroleum Consumption Data

```bash
curl -X POST "http://localhost:8000/api/v1/eia/petroleum/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "subcategory": "consumption",
    "frequency": "annual",
    "start": "2015",
    "end": "2023"
  }'
```

Response:
```json
{
  "job_id": 123,
  "status": "pending",
  "message": "EIA petroleum ingestion job created",
  "check_status": "/api/v1/jobs/123"
}
```

### Example 2: Ingest Electricity Retail Sales

```bash
curl -X POST "http://localhost:8000/api/v1/eia/electricity/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "subcategory": "retail_sales",
    "frequency": "monthly",
    "start": "2020-01",
    "end": "2023-12",
    "facets": {
      "sectorid": "RES",
      "stateid": "CA"
    }
  }'
```

### Example 3: Ingest Retail Gas Prices

```bash
curl -X POST "http://localhost:8000/api/v1/eia/retail-gas-prices/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "frequency": "weekly",
    "start": "2022-01-01",
    "end": "2023-12-31"
  }'
```

### Example 4: Ingest Natural Gas Consumption

```bash
curl -X POST "http://localhost:8000/api/v1/eia/natural-gas/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "subcategory": "consumption",
    "frequency": "annual",
    "start": "2015",
    "end": "2023"
  }'
```

### Example 5: Ingest STEO Projections

```bash
curl -X POST "http://localhost:8000/api/v1/eia/steo/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "frequency": "monthly",
    "start": "2023-01",
    "end": "2025-12"
  }'
```

## Check Job Status

After submitting an ingestion job, check its status:

```bash
curl "http://localhost:8000/api/v1/jobs/123"
```

Response:
```json
{
  "id": 123,
  "source": "eia",
  "status": "success",
  "created_at": "2023-12-01T10:00:00Z",
  "started_at": "2023-12-01T10:00:01Z",
  "completed_at": "2023-12-01T10:02:30Z",
  "rows_inserted": 15420,
  "config": {
    "category": "petroleum",
    "subcategory": "consumption",
    "frequency": "annual"
  }
}
```

## Data Frequencies

EIA data is available at different frequencies depending on the category:

- **Annual**: Use `"frequency": "annual"`, dates like `"2023"`
- **Monthly**: Use `"frequency": "monthly"`, dates like `"2023-12"`
- **Weekly**: Use `"frequency": "weekly"`, dates like `"2023-12-25"`
- **Daily**: Use `"frequency": "daily"`, dates like `"2023-12-25"`

## Facet Filters

Many endpoints support facet filters to narrow down the data:

### Petroleum Facets
```json
{
  "facets": {
    "process": "VPP",
    "product": "EPP0"
  }
}
```

### Electricity Facets
```json
{
  "facets": {
    "sectorid": "RES",    // Residential
    "stateid": "CA"       // California
  }
}
```

Common sector IDs:
- `RES`: Residential
- `COM`: Commercial
- `IND`: Industrial
- `TRA`: Transportation

### Retail Gas Prices Facets
```json
{
  "facets": {
    "product": "EPM0",    // Regular gasoline
    "area": "NUS"         // U.S. National
  }
}
```

## Rate Limits

EIA API rate limits:
- **With API key**: 5,000 requests per hour
- The service defaults to 2 concurrent requests for safety
- Automatic retry with exponential backoff on errors

## Database Tables

Data is stored in typed PostgreSQL tables:

- `eia_petroleum_consumption`
- `eia_petroleum_production`
- `eia_natural_gas_consumption`
- `eia_natural_gas_production`
- `eia_electricity_retail_sales`
- `eia_electricity_generation`
- `eia_retail_gas_prices`
- `eia_steo`

## Query Examples

After ingestion, query the data directly:

### Get Petroleum Consumption Data
```sql
SELECT 
    period,
    product_name,
    area_name,
    value,
    units
FROM eia_petroleum_consumption
WHERE period >= '2020'
ORDER BY period DESC, product_name;
```

### Get Retail Gas Prices by State
```sql
SELECT 
    period,
    area_name,
    product,
    value AS price,
    units
FROM eia_retail_gas_prices
WHERE area_code = 'CA'
  AND period >= '2023-01-01'
ORDER BY period DESC;
```

### Get Electricity Sales by Sector
```sql
SELECT 
    period,
    state_name,
    sector_name,
    value AS sales_mwh,
    units
FROM eia_electricity_retail_sales
WHERE sector = 'RES'
  AND period >= '2020'
ORDER BY period DESC, state_name;
```

## Troubleshooting

### "EIA_API_KEY is required"

Make sure your `.env` file contains:
```bash
EIA_API_KEY=your_actual_api_key
```

Restart the service after updating `.env`.

### Rate Limiting

If you hit rate limits, the service will automatically:
1. Respect `Retry-After` headers
2. Implement exponential backoff
3. Log warnings

You can also reduce concurrency in `.env`:
```bash
MAX_CONCURRENCY=1
```

### No Data Returned

Some date ranges or facet combinations may not have data. Check:
1. The EIA API documentation for valid facet values
2. The date range for the specific data series
3. Job error messages in the job status endpoint

## Interactive Documentation

Visit http://localhost:8000/docs for:
- Interactive API documentation
- Try the endpoints directly in your browser
- See all available parameters and schemas
- View example requests and responses

## Architecture Notes

The EIA integration follows the same plugin architecture as other sources:

- **Client** (`app/sources/eia/client.py`): HTTP client with rate limiting and retry logic
- **Metadata** (`app/sources/eia/metadata.py`): Schema generation and data parsing
- **Ingestion** (`app/sources/eia/ingest.py`): Orchestration and job tracking
- **API** (`app/api/v1/eia.py`): HTTP endpoints

All EIA operations:
- ✅ Use bounded concurrency (semaphores)
- ✅ Implement retry with exponential backoff
- ✅ Store data in typed columns (not JSON blobs)
- ✅ Track all operations in `ingestion_jobs` table
- ✅ Support idempotent operations (ON CONFLICT)

## Next Steps

1. **Explore the interactive docs**: Visit `/docs` to see all endpoints
2. **Customize facets**: Use facet filters to get specific data
3. **Schedule regular updates**: Set up periodic ingestion jobs
4. **Query and analyze**: Use PostgreSQL to analyze the energy data

## Support

For EIA API documentation and support:
- API Documentation: https://www.eia.gov/opendata/
- Register for API key: https://www.eia.gov/opendata/register.php
- Browser tool: https://www.eia.gov/opendata/browser/

For service-specific issues, check the logs:
```bash
# If running with uvicorn
tail -f logs/app.log
```

