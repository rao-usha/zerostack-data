# Foot Traffic Intelligence - Quick Start Guide

## 🎯 Overview

The Foot Traffic Intelligence system collects and analyzes foot traffic data for physical locations (stores, restaurants, offices) to evaluate retail/hospitality investments and real estate opportunities.

**Use Cases:**
- Portfolio company monitoring (track foot traffic at portfolio companies' locations)
- Investment due diligence (evaluate retail/restaurant chains before investing)
- Real estate analysis (assess property value based on foot traffic trends)
- Competitive intelligence (compare foot traffic across competitors)

---

## 📋 Prerequisites

### API Keys (Optional but Recommended)

| Service | Required? | Cost | Description | Get Key |
|---------|-----------|------|-------------|---------|
| **Foursquare** | Recommended | Free tier | POI discovery & enrichment | [developer.foursquare.com](https://developer.foursquare.com/) |
| **SafeGraph** | Recommended | $100-500/mo | Weekly foot traffic patterns | [safegraph.com](https://www.safegraph.com/) |
| **Placer.ai** | Optional | $500-2000+/mo | Retail analytics | [placer.ai](https://www.placer.ai/) |

### Environment Variables

Add to your `.env` file:

```bash
# Required for full functionality
FOURSQUARE_API_KEY=your_foursquare_key

# Optional but highly recommended
SAFEGRAPH_API_KEY=your_safegraph_key

# Optional (enterprise)
PLACER_API_KEY=your_placer_key

# Enable Google Popular Times scraping (ToS risk)
FOOT_TRAFFIC_ENABLE_GOOGLE_SCRAPING=false
```

---

## 🚀 Quick Start

### 1. Discover Locations for a Brand

Find all Chipotle locations in San Francisco:

```bash
curl -X POST "http://localhost:8001/api/v1/foot-traffic/locations/discover" \
  -H "Content-Type: application/json" \
  -d '{
    "brand_name": "Chipotle",
    "city": "San Francisco",
    "state": "CA",
    "limit": 50
  }'
```

**Response:**
```json
{
  "data": {
    "status": "success",
    "brand_name": "Chipotle",
    "locations_found": 15,
    "new_locations": 15,
    "updated_locations": 0,
    "strategies_used": ["foursquare", "safegraph"],
    "reasoning_log": [...]
  },
  "meta": {
    "job_id": 1,
    "source": "foot_traffic_agent"
  }
}
```

### 2. List Tracked Locations

```bash
# All locations for a brand
curl "http://localhost:8001/api/v1/foot-traffic/locations?brand_name=Chipotle"

# Filter by city
curl "http://localhost:8001/api/v1/foot-traffic/locations?brand_name=Chipotle&city=San%20Francisco"
```

### 3. Collect Foot Traffic Data

Collect traffic data for a specific location:

```bash
curl -X POST "http://localhost:8001/api/v1/foot-traffic/locations/123/collect?start_date=2024-01-01&end_date=2024-03-31"
```

Or collect for all locations of a brand:

```bash
curl -X POST "http://localhost:8001/api/v1/foot-traffic/collect" \
  -H "Content-Type: application/json" \
  -d '{
    "brand_name": "Chipotle",
    "city": "San Francisco",
    "start_date": "2024-01-01",
    "end_date": "2024-03-31"
  }'
```

### 4. Query Traffic Data

Get traffic time series for a location:

```bash
curl "http://localhost:8001/api/v1/foot-traffic/locations/123/traffic?start_date=2024-01-01&end_date=2024-03-31&granularity=weekly"
```

### 5. Get Brand Traffic Summary

```bash
curl "http://localhost:8001/api/v1/foot-traffic/brands/Chipotle/aggregate?city=San%20Francisco"
```

**Response:**
```json
{
  "data": {
    "brand_name": "Chipotle",
    "location_count": 15,
    "observation_count": 180,
    "avg_weekly_visits": 850,
    "total_visits": 12750,
    "date_range": {
      "start": "2024-01-01",
      "end": "2024-03-31"
    }
  }
}
```

### 6. Compare Competitors

```bash
curl "http://localhost:8001/api/v1/foot-traffic/compare?brand_names=Chipotle&brand_names=Panera&brand_names=Sweetgreen&city=San%20Francisco"
```

---

## 📊 Data Sources

### 1. Foursquare (Recommended for Discovery)

**Best for:** Location discovery, POI enrichment, metadata (addresses, hours, categories)

**Data quality:** Medium confidence (check-in data is opt-in)

**Cost:** Free tier available

```bash
# Example: Discover Starbucks locations
curl -X POST "http://localhost:8001/api/v1/foot-traffic/locations/discover" \
  -d '{"brand_name": "Starbucks", "city": "Seattle", "strategies": ["foursquare"]}'
```

### 2. SafeGraph (Best for Traffic Data)

**Best for:** Weekly visitor counts, historical data (2+ years), demographics

**Data quality:** High confidence (mobile location data, ~10-15% population sample)

**Cost:** $100-500/month

```bash
# Requires SAFEGRAPH_API_KEY configured
curl -X POST "http://localhost:8001/api/v1/foot-traffic/collect" \
  -d '{"brand_name": "Target", "strategies": ["safegraph"]}'
```

### 3. Placer.ai (Enterprise)

**Best for:** Retail chain analytics, trade area analysis, competitive benchmarking

**Data quality:** High confidence

**Cost:** $500-2,000+/month

### 4. City Open Data (Free)

**Best for:** Street-level pedestrian counts in supported cities

**Supported cities:** Seattle, New York, San Francisco, Chicago

**Data quality:** High confidence (actual sensor counts)

```bash
# Get pedestrian data for Seattle
curl -X POST "http://localhost:8001/api/v1/foot-traffic/collect" \
  -d '{"brand_name": "Pike Place Market", "city": "Seattle", "strategies": ["city_data"]}'
```

### 5. Google Popular Times (Free, ToS Risk)

**Best for:** Peak hours patterns

**Data quality:** Medium (relative 0-100 scale, not absolute counts)

**⚠️ Warning:** Scraping Google may violate their Terms of Service

```bash
# Enable in .env: FOOT_TRAFFIC_ENABLE_GOOGLE_SCRAPING=true
curl -X POST "http://localhost:8001/api/v1/foot-traffic/collect" \
  -d '{"brand_name": "Starbucks", "city": "Seattle", "strategies": ["google_popular_times"]}'
```

---

## 📁 Database Tables

### `locations`
Physical places (stores, restaurants, offices)

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| location_name | TEXT | Business name |
| brand_name | VARCHAR | Chain/brand name |
| street_address | TEXT | Address |
| city, state | VARCHAR | Location |
| latitude, longitude | NUMERIC | Coordinates |
| category | VARCHAR | restaurant, retail, etc. |
| foursquare_fsq_id | VARCHAR | Foursquare ID |
| safegraph_placekey | VARCHAR | SafeGraph ID |

### `foot_traffic_observations`
Time-series traffic data

| Column | Type | Description |
|--------|------|-------------|
| location_id | INT | FK to locations |
| observation_date | DATE | Date of observation |
| observation_period | VARCHAR | daily, weekly, monthly |
| visit_count | INT | Absolute visitor count |
| visit_count_relative | INT | 0-100 relative scale |
| median_dwell_minutes | NUMERIC | Average visit duration |
| source_type | VARCHAR | safegraph, foursquare, etc. |
| source_confidence | VARCHAR | high, medium, low |

---

## 🔍 Common Queries

### Portfolio Company Monitoring

```sql
-- Track foot traffic trend for Chipotle (portfolio company)
SELECT 
    observation_date,
    AVG(visit_count) as avg_visits,
    COUNT(*) as location_count
FROM foot_traffic_observations
WHERE location_id IN (
    SELECT id FROM locations WHERE brand_name = 'Chipotle'
)
AND observation_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY observation_date
ORDER BY observation_date;
```

### Competitive Benchmarking

```sql
-- Compare Chipotle vs competitors in SF
SELECT 
    l.brand_name,
    AVG(fto.visit_count) as avg_weekly_visits,
    COUNT(DISTINCT l.id) as location_count
FROM locations l
JOIN foot_traffic_observations fto ON l.id = fto.location_id
WHERE l.city = 'San Francisco'
AND l.brand_name IN ('Chipotle', 'Panera', 'Sweetgreen')
AND fto.observation_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY l.brand_name
ORDER BY avg_weekly_visits DESC;
```

### High-Traffic Areas

```sql
-- Find high-traffic areas for real estate investment
SELECT 
    l.city,
    l.postal_code,
    AVG(fto.visit_count) as avg_area_traffic,
    COUNT(DISTINCT l.id) as poi_count
FROM locations l
JOIN foot_traffic_observations fto ON l.id = fto.location_id
WHERE l.state = 'CA'
AND fto.observation_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY l.city, l.postal_code
HAVING COUNT(DISTINCT l.id) >= 5
ORDER BY avg_area_traffic DESC
LIMIT 20;
```

---

## 💰 Cost Estimates

| Locations Tracked | Monthly Cost | Annual Cost |
|-------------------|--------------|-------------|
| 100 | $10-50 | $120-600 |
| 500 | $50-200 | $600-2,400 |
| 2,000 | $200-800 | $2,400-9,600 |

*Costs based on SafeGraph + Foursquare combination. Placer.ai adds $500-2,000+/month.*

---

## ✅ Success Criteria

- [ ] 500+ locations tracked (starting with portfolio companies)
- [ ] 80%+ data availability (weekly traffic data)
- [ ] Historical data back to 2022 (if using SafeGraph)
- [ ] Competitive benchmarking for top retail/restaurant chains
- [ ] Traffic trend alerts for portfolio companies

---

## 🐛 Troubleshooting

### "Foursquare API key not configured"

Add to `.env`:
```bash
FOURSQUARE_API_KEY=your_key_here
```

### "No locations found"

- Check brand name spelling (exact match required)
- Try broader geographic scope (remove city filter)
- Verify API key has sufficient permissions

### "Google scraping disabled"

Enable in `.env` (use with caution):
```bash
FOOT_TRAFFIC_ENABLE_GOOGLE_SCRAPING=true
```

### Rate limiting errors

The system automatically handles rate limits with exponential backoff. If you see persistent rate limit errors:
- Wait 5-10 minutes before retrying
- Reduce the number of concurrent requests
- Consider upgrading your API tier

---

## 📚 Related Documentation

- [HANDOFF_foot_traffic_intelligence.md](./AGENT_PROMPTS/HANDOFF_foot_traffic_intelligence.md) - Full implementation spec
- [API_DOCUMENTATION.md](./API_DOCUMENTATION.md) - Complete API reference
- [RULES.md](../RULES.md) - Project rules and guidelines
