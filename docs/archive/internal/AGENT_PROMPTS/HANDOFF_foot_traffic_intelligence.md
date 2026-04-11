# AGENT HANDOFF: Foot Traffic & Location Intelligence

---

## 🎯 MISSION

Build an **agentic system** that collects and analyzes foot traffic data for physical locations (stores, restaurants, offices, venues) to evaluate retail/hospitality investments and real estate opportunities.

**Why This Matters:**
- LPs/FOs invest heavily in retail, hospitality, and real estate
- Foot traffic = leading indicator of revenue and property value
- Traditional data (sales reports) are lagging indicators
- Foot traffic data helps evaluate portfolio companies' performance in real-time

**Business Value:**
- **Portfolio Monitoring:** Track foot traffic at portfolio companies' locations
- **Investment Due Diligence:** Evaluate retail/restaurant chains before investing
- **Real Estate Analysis:** Assess property value based on foot traffic trends
- **Competitive Intelligence:** Compare foot traffic across competitors
- **Early Warning System:** Detect declining traffic before revenue drops

---

## 📋 WHAT YOU'RE BUILDING

### Core Deliverables:
1. **Location ingestion system** - Capture POIs (stores, restaurants, offices)
2. **Multi-source foot traffic collection** - 5+ data sources
3. **Time-series storage** - Track traffic trends over time
4. **Analytics engine** - Growth rates, seasonality, competitive benchmarking
5. **API endpoints** - Query traffic by location, chain, or geographic area

### Database Tables to Create:
- `locations` - Physical places (stores, restaurants, offices, venues)
- `foot_traffic_observations` - Time-series traffic data
- `location_metadata` - Hours, categories, brands
- `foot_traffic_collection_jobs` - Agent job tracking

### Expected Results:
- **500-2,000 locations** tracked (starting with portfolio companies' stores)
- **Weekly/monthly traffic data** going back 1-2 years
- **80%+ data availability** for major retail/restaurant chains
- **Competitive benchmarking** (compare chains in same category)

---

## 📚 BACKGROUND CONTEXT

### Current State:
- **Database:** PostgreSQL with LP/FO and portfolio company data
- **Tech Stack:** FastAPI + SQLAlchemy + httpx
- **Related Data:** Private companies (to link retail chains), real estate data
- **Use Case:** Evaluate retail/hospitality investments for LPs/FOs

### Why Agentic (Not Single API):
Foot traffic data is scattered across:
- SafeGraph (paid API - foot traffic datasets)
- Placer.ai (paid - retail analytics)
- Google Popular Times (free but scraped - peak hours data)
- Foursquare/Mapbox (POI data + some traffic signals)
- Mobile location data aggregators (expensive, privacy concerns)
- Public city data (pedestrian counters in some cities)

**No single affordable source → Agent must combine multiple sources**

---

## 🧠 HOW THE AGENT WORKS

### Agent Workflow:
```
1. DISCOVER: Agent finds locations to track (from portfolio companies)
2. ENRICH: Agent enriches with POI data (address, category, hours)
3. COLLECT: Agent gathers foot traffic from multiple sources
4. VALIDATE: Agent cross-checks data across sources
5. ANALYZE: Agent calculates trends, growth rates, seasonality
6. ALERT: Agent flags significant traffic changes
```

### Example Agent Decision:
```
Input: "Track Chipotle locations in San Francisco"

Agent Reasoning:
→ "Chipotle is restaurant chain → check Google Popular Times" (free)
→ "If available, check SafeGraph foot traffic API" (paid, high quality)
→ "Get POI data from Foursquare" (addresses, hours, categories)
→ "For comparison, get Panera/Sweetgreen traffic in same area"

Execution:
✓ Google Popular Times: Found 15 SF Chipotle locations, peak hours data
✓ Foursquare: Enriched with exact addresses, phone, hours
✓ SafeGraph (if API key): Historical weekly traffic patterns
✓ Competitors: Found 8 Panera, 12 Sweetgreen in SF for benchmarking

Analysis:
→ Chipotle avg traffic: 850 visits/week
→ Trend: -5% QoQ (declining)
→ vs Sweetgreen: +12% QoQ (gaining share)
→ Alert: "Chipotle SF locations underperforming competitors"
```

---

## 🔧 THE 5 DATA COLLECTION STRATEGIES

### Strategy 1: Google Popular Times (Free, Scraping)
**What:** Extract foot traffic patterns from Google Maps "Popular Times" charts
**Coverage:** 60-80% of retail/restaurant locations
**Confidence:** MEDIUM (relative data, not absolute counts)

**Implementation:**
```python
async def collect_google_popular_times(location_name: str, address: str):
    """
    Scrape Google Maps for Popular Times data
    
    Returns: Hourly traffic patterns (0-100 scale) by day of week
    """
    
    # Search Google Maps for location
    search_query = f"{location_name} {address}"
    maps_url = f"https://www.google.com/maps/search/{quote(search_query)}"
    
    # Extract place ID
    html = await fetch_page(maps_url)
    place_id = extract_place_id(html)
    
    # Get place details (includes Popular Times)
    details_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    details_html = await fetch_page(details_url)
    
    # Parse Popular Times JSON (embedded in HTML)
    popular_times = extract_popular_times_json(details_html)
    
    """
    popular_times = {
        "Monday": [10, 15, 25, 45, 70, 85, 90, 85, 75, 50, 30, 15],  # 12 hours
        "Tuesday": [...],
        # ... rest of week
    }
    """
    
    return popular_times
```

**Limitations:**
- Relative data (0-100 scale), not absolute visitor counts
- No historical data (current week only)
- Rate limiting required (Google may block aggressive scraping)
- Violates Google ToS if too aggressive

**Safeguards:**
- 1 request per 5 seconds per location
- Max 100 locations per day
- User-Agent rotation
- Exponential backoff on errors

---

### Strategy 2: SafeGraph Foot Traffic (Paid API)
**What:** Weekly foot traffic patterns from mobile location data
**Coverage:** 80-90% of commercial POIs in US
**Confidence:** HIGH (actual visitor counts from mobile data)

**API:** https://docs.safegraph.com/
**Cost:** $100-500/month depending on tier
**Data:** Weekly visits, dwell time, visitor demographics, visitor origin

**Implementation:**
```python
async def collect_safegraph_traffic(location_placekey: str):
    """
    Fetch SafeGraph foot traffic data
    
    Placekey: SafeGraph's location identifier (POI ID)
    """
    
    api_key = settings.safegraph_api_key
    
    # SafeGraph Patterns API (weekly foot traffic)
    url = f"https://api.safegraph.com/v2/patterns"
    params = {
        'placekey': location_placekey,
        'start_date': '2023-01-01',
        'end_date': '2024-12-31'
    }
    headers = {'Authorization': f'Bearer {api_key}'}
    
    response = await httpx_client.get(url, params=params, headers=headers)
    data = response.json()
    
    """
    data = {
        "placekey": "abc-xyz@123-456",
        "location_name": "Chipotle",
        "street_address": "123 Main St",
        "city": "San Francisco",
        "patterns": [
            {
                "date_range_start": "2024-01-01",
                "date_range_end": "2024-01-07",
                "raw_visit_counts": 850,
                "raw_visitor_counts": 720,  # unique visitors
                "median_dwell": 32.5  # minutes
            },
            # ... weekly data
        ]
    }
    """
    
    return data
```

**Advantages:**
- Absolute visitor counts (not relative)
- Historical data (2+ years)
- Visitor demographics (age, income, home location)
- Dwell time (how long people stay)

**Data Quality:**
- Based on mobile location data (privacy-compliant)
- ~10-15% sample of population → extrapolated
- Updated weekly

---

### Strategy 3: Placer.ai (Paid API)
**What:** Retail analytics focused on foot traffic trends
**Coverage:** Major retail chains, malls, shopping centers
**Confidence:** HIGH (similar to SafeGraph)

**API:** https://www.placer.ai/
**Cost:** $500-2,000+/month (enterprise pricing)
**Data:** Visits, trade area analysis, competitive benchmarking

**Implementation:**
```python
async def collect_placer_traffic(location_id: str):
    """
    Fetch Placer.ai foot traffic data
    Note: Requires enterprise subscription
    """
    
    api_key = settings.placer_api_key
    
    url = f"https://api.placer.ai/v1/venues/{location_id}/insights"
    headers = {'X-API-Key': api_key}
    
    response = await httpx_client.get(url, headers=headers)
    data = response.json()
    
    """
    data = {
        "venue_id": "placer_12345",
        "venue_name": "Target #1234",
        "monthly_visits": [
            {"month": "2024-01", "visits": 45000},
            {"month": "2024-02", "visits": 47000},
            # ...
        ],
        "trade_area": {
            "5_min_drive": {"population": 25000, "median_income": 85000},
            "10_min_drive": {"population": 75000, "median_income": 78000}
        },
        "competitive_set": [
            {"competitor": "Walmart #567", "visits": 52000, "distance_mi": 2.3}
        ]
    }
    """
    
    return data
```

**Advantages:**
- Retail-focused (great for evaluating retail chains)
- Trade area analysis (who lives nearby)
- Competitive benchmarking built-in
- Mall/shopping center data

---

### Strategy 4: Foursquare/Mapbox POI Data (Freemium)
**What:** Place metadata + check-in data (not full foot traffic)
**Coverage:** Global POI database
**Confidence:** MEDIUM (check-ins are opt-in, not representative)

**API:** https://developer.foursquare.com/
**Cost:** Free tier available, $0.01-0.05 per API call for premium
**Data:** POI details, categories, hours, check-in counts

**Implementation:**
```python
async def enrich_location_with_foursquare(location_name: str, lat: float, lon: float):
    """
    Get POI metadata and check-in data from Foursquare
    """
    
    api_key = settings.foursquare_api_key
    
    # Search for POI near coordinates
    url = "https://api.foursquare.com/v3/places/search"
    params = {
        'll': f"{lat},{lon}",
        'query': location_name,
        'radius': 100  # meters
    }
    headers = {'Authorization': api_key}
    
    response = await httpx_client.get(url, params=params, headers=headers)
    data = response.json()
    
    """
    data = {
        "results": [
            {
                "fsq_id": "4b123456789",
                "name": "Starbucks",
                "categories": ["Coffee Shop", "Cafe"],
                "location": {"address": "123 Main St", "locality": "San Francisco"},
                "hours": {"regular": [{"day": 1, "open": "0600", "close": "2000"}]},
                "stats": {
                    "total_check ins": 5432,
                    "total_users": 3210
                }
            }
        ]
    }
    """
    
    return data['results'][0] if data['results'] else None
```

**Use Case:**
- POI enrichment (get exact address, category, hours)
- Check-in data as a weak traffic signal
- Good for discovering locations

---

### Strategy 5: City Open Data (Pedestrian Counters)
**What:** Public pedestrian traffic counters in select cities
**Coverage:** ~20-30 major cities have some data
**Confidence:** HIGH (actual sensor counts)

**Sources:**
- Seattle: https://data.seattle.gov/ (pedestrian counters)
- NYC: https://opendata.cityofnewyork.us/ (turnstile data, bike counters)
- SF: https://datasf.org/ (pedestrian counts)
- Chicago: https://data.cityofchicago.org/

**Implementation:**
```python
async def collect_city_pedestrian_data(city: str, location: str):
    """
    Collect pedestrian counter data from city open data portals
    """
    
    if city == 'Seattle':
        # Seattle has automated pedestrian counters
        url = "https://data.seattle.gov/resource/pedestrian-counts.json"
        params = {'location': location}
        
        response = await httpx_client.get(url, params=params)
        data = response.json()
        
        """
        data = [
            {"date": "2024-01-15", "location": "2nd Ave & Pine St", "count": 8524},
            {"date": "2024-01-16", "location": "2nd Ave & Pine St", "count": 9102},
            # ...
        ]
        """
        
        return data
    
    elif city == 'NYC':
        # NYC MTA turnstile data (subway foot traffic proxy)
        # ... similar implementation
        pass
```

**Advantages:**
- Free, public data
- High accuracy (actual sensor counts)
- Historical data available

**Limitations:**
- Only specific locations (major streets, transit hubs)
- Not store-specific
- Limited to ~20-30 cities

---

## 🗄️ DATABASE SCHEMA

### 1. locations (Core POI Table)
```sql
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    
    -- Identifiers
    location_name TEXT NOT NULL,
    brand_name TEXT, -- e.g., "Starbucks" for "Starbucks #1234"
    chain_id INT REFERENCES private_companies(id), -- link to company
    
    -- Location Details
    street_address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT DEFAULT 'United States',
    latitude NUMERIC(10, 7),
    longitude NUMERIC(10, 7),
    
    -- POI Metadata
    category TEXT, -- restaurant, retail, office, venue, etc.
    subcategory TEXT, -- coffee_shop, fast_food, clothing_store, etc.
    
    hours_of_operation JSONB, -- {"Monday": {"open": "0800", "close": "2000"}, ...}
    phone TEXT,
    website TEXT,
    
    -- External IDs (for API mapping)
    google_place_id TEXT,
    safegraph_placekey TEXT,
    foursquare_fsq_id TEXT,
    placer_venue_id TEXT,
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    opened_date DATE,
    closed_date DATE,
    
    -- Linkage
    portfolio_company_id INT REFERENCES portfolio_companies(id), -- if in LP/FO portfolio
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(google_place_id),
    UNIQUE(safegraph_placekey)
);

CREATE INDEX idx_locations_brand ON locations(brand_name);
CREATE INDEX idx_locations_city ON locations(city, state);
CREATE INDEX idx_locations_category ON locations(category);
CREATE INDEX idx_locations_chain ON locations(chain_id);
CREATE INDEX idx_locations_coords ON locations(latitude, longitude);
```

### 2. foot_traffic_observations (Time Series)
```sql
CREATE TABLE foot_traffic_observations (
    id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    
    -- Time Period
    observation_date DATE NOT NULL,
    observation_period TEXT, -- daily, weekly, monthly, hourly
    
    -- Traffic Metrics
    visit_count INT, -- absolute visitor count (if available)
    visitor_count INT, -- unique visitors (if available)
    visit_count_relative INT, -- 0-100 scale (Google Popular Times)
    
    -- Dwell & Engagement
    median_dwell_minutes NUMERIC,
    avg_dwell_minutes NUMERIC,
    
    -- Hourly Breakdown (for daily observations)
    hourly_traffic JSONB, -- {"00": 10, "01": 5, "02": 3, ..., "23": 15}
    
    -- Day of Week Patterns (for weekly observations)
    daily_traffic JSONB, -- {"Mon": 850, "Tue": 920, ..., "Sun": 650}
    
    -- Visitor Demographics (if available from SafeGraph/Placer)
    visitor_demographics JSONB,
    /*
    {
        "age_ranges": {"18-24": 0.15, "25-34": 0.35, "35-44": 0.25, ...},
        "median_income": 75000,
        "home_distance_mi": {"0-5": 0.60, "5-10": 0.25, "10-25": 0.15}
    }
    */
    
    -- Data Provenance
    source_type TEXT NOT NULL, -- google, safegraph, placer, city_data
    source_confidence TEXT CHECK (source_confidence IN ('high', 'medium', 'low')),
    
    collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(location_id, observation_date, observation_period, source_type)
);

CREATE INDEX idx_traffic_location ON foot_traffic_observations(location_id);
CREATE INDEX idx_traffic_date ON foot_traffic_observations(observation_date);
CREATE INDEX idx_traffic_source ON foot_traffic_observations(source_type);
```

### 3. location_metadata (Enrichment Data)
```sql
CREATE TABLE location_metadata (
    id SERIAL PRIMARY KEY,
    location_id INT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    
    -- Business Info
    square_footage INT,
    employee_count_estimate INT,
    parking_spots INT,
    
    -- Trade Area (from Placer.ai)
    trade_area_5min_population INT,
    trade_area_5min_median_income NUMERIC,
    trade_area_10min_population INT,
    trade_area_10min_median_income NUMERIC,
    
    -- Competitive Set
    nearby_competitors JSONB,
    /*
    [
        {"name": "Panera #123", "distance_mi": 0.5, "category": "fast_casual"},
        {"name": "Sweetgreen", "distance_mi": 0.8, "category": "fast_casual"}
    ]
    */
    
    -- Ratings & Reviews
    google_rating NUMERIC(2, 1),
    google_review_count INT,
    yelp_rating NUMERIC(2, 1),
    yelp_review_count INT,
    
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_metadata_location ON location_metadata(location_id);
```

### 4. foot_traffic_collection_jobs
```sql
CREATE TABLE foot_traffic_collection_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT, -- discover_locations, collect_traffic, enrich_metadata
    
    target_brand TEXT,
    target_location_id INT,
    geographic_scope TEXT, -- city, state, national, specific_address
    
    status TEXT CHECK (status IN ('pending', 'running', 'success', 'partial_success', 'failed')),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    
    -- Results
    locations_found INT,
    locations_enriched INT,
    observations_collected INT,
    sources_checked TEXT[],
    
    -- Agent Reasoning
    reasoning_log JSONB,
    
    errors JSONB,
    warnings TEXT[],
    
    requests_made INT,
    cost_usd NUMERIC
);
```

---

## 📁 FILE STRUCTURE

```
app/
├── agentic/
│   ├── foot_traffic_agent.py           # Main orchestrator
│   ├── location_discovery.py           # Find locations to track
│   └── traffic_strategies/
│       ├── __init__.py
│       ├── google_popular_times.py     # Strategy 1
│       ├── safegraph_strategy.py       # Strategy 2
│       ├── placer_strategy.py          # Strategy 3
│       ├── foursquare_strategy.py      # Strategy 4
│       └── city_data_strategy.py       # Strategy 5
│
└── api/
    └── v1/
        └── foot_traffic.py              # API endpoints

app/core/config.py                       # Add API keys
```

---

## 🔌 API ENDPOINTS

```python
router = APIRouter(prefix="/foot-traffic", tags=["Foot Traffic"])

@router.post("/locations/discover")
async def discover_locations(
    brand_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Discover and track locations for a brand (e.g., "Starbucks")
    """
    pass

@router.post("/locations/{location_id}/collect")
async def collect_traffic_for_location(
    location_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Collect foot traffic data for a specific location
    """
    pass

@router.get("/locations/{location_id}/traffic")
async def get_location_traffic(
    location_id: int,
    start_date: date,
    end_date: date,
    granularity: str = "weekly",  # daily, weekly, monthly
    db: Session = Depends(get_db)
):
    """
    Get foot traffic time series for a location
    """
    pass

@router.get("/brands/{brand_name}/aggregate")
async def get_brand_aggregate_traffic(
    brand_name: str,
    city: Optional[str] = None,
    state: Optional[str] = None,
    start_date: date,
    end_date: date,
    db: Session = Depends(get_db)
):
    """
    Get aggregated traffic across all locations for a brand
    """
    pass

@router.get("/compare")
async def compare_brands(
    brand_names: List[str],
    geographic_scope: str,  # "San Francisco, CA" or "CA" or "US"
    start_date: date,
    end_date: date,
    db: Session = Depends(get_db)
):
    """
    Compare foot traffic across multiple brands/chains
    """
    pass

@router.get("/portfolio-companies/{company_id}/locations")
async def get_portfolio_company_locations(
    company_id: int,
    db: Session = Depends(get_db)
):
    """
    Get all tracked locations for a portfolio company (e.g., Chipotle)
    """
    pass
```

---

## ⚙️ CONFIGURATION

```python
class Settings(BaseSettings):
    # ... existing ...
    
    # Foot Traffic APIs
    safegraph_api_key: Optional[str] = None  # Recommended
    placer_api_key: Optional[str] = None     # Optional (expensive)
    foursquare_api_key: Optional[str] = None # Recommended
    
    # Scraping Settings
    foot_traffic_enable_google_scraping: bool = False  # Set True to enable (ToS risk)
    foot_traffic_requests_per_day_google: int = 100
    
    # Rate Limiting
    foot_traffic_requests_per_second: float = 0.2  # 1 per 5 seconds (conservative)
```

---

## 💰 COST ESTIMATES

### Per Location:
- **Google Popular Times (scraping):** Free (ToS risk)
- **SafeGraph:** $0.01-0.10 per location per month
- **Foursquare:** $0.01 per API call
- **Total:** $0.05-0.20 per location per month

### For 500 Locations:
- **Monthly:** $25-100
- **Annual:** $300-1,200

### For 2,000 Locations:
- **Monthly:** $100-400
- **Annual:** $1,200-4,800

---

## 🎯 USE CASES

### 1. Portfolio Company Monitoring
```sql
-- Track foot traffic trend for Chipotle (portfolio company)
SELECT 
    observation_date,
    AVG(visit_count) as avg_visits,
    COUNT(*) as location_count
FROM foot_traffic_observations
WHERE location_id IN (SELECT id FROM locations WHERE brand_name = 'Chipotle')
  AND observation_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY observation_date
ORDER BY observation_date;
```

### 2. Competitive Benchmarking
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
GROUP BY l.brand_name;
```

### 3. Real Estate Investment Analysis
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

## ✅ SUCCESS CRITERIA

- [ ] 500+ locations tracked (starting with portfolio companies)
- [ ] 80%+ data availability (weekly traffic data)
- [ ] Historical data back to 2022 (if using SafeGraph/Placer)
- [ ] Competitive benchmarking for top 10 retail/restaurant chains
- [ ] Traffic trend alerts for portfolio companies
- [ ] Cost <$200/month for 500 locations

---

## 📈 TIMELINE

- **Week 1:** Database + POI discovery + Foursquare enrichment
- **Week 2:** Google Popular Times scraping (if enabled) + SafeGraph integration
- **Week 3:** Placer.ai (if available) + City data sources
- **Week 4:** Analytics, trends, alerts

**Total:** 3-4 weeks

---

## 🎉 FINAL NOTES

**This data is highly valuable for:**
- 🏪 Retail portfolio evaluation
- 🍔 Restaurant chain performance tracking
- 🏢 Real estate investment decisions
- 📊 Competitive intelligence

**Start with SafeGraph API (best ROI) + Foursquare for POI enrichment!**

Good luck! 🚀
