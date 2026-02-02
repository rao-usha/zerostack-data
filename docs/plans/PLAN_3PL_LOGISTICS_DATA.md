# PLAN: Industrial & Data Center Site Intelligence Platform

## Goal
Build a comprehensive data platform for industrial facility and data center site selection, construction logistics, and ongoing operations intelligence. This system aggregates 50+ public data sources across power, telecom, transportation, labor, risk, and incentives to enable data-driven site decisions.

## Why This Matters
- **Site Selection**: Score and rank potential sites across 100+ factors
- **Construction Logistics**: Plan heavy equipment transport, materials delivery
- **Power Planning**: Assess grid capacity, interconnection queues, pricing
- **Risk Assessment**: Evaluate flood, seismic, climate, and regulatory risks
- **Labor Analysis**: Understand workforce availability, wages, skills
- **Incentive Optimization**: Identify tax incentives, OZ, FTZ benefits
- **Supply Chain**: Track 3PL capacity, warehouse availability, transport routes

## Status
- [ ] Approved

---

## Target Use Cases

### Data Center Build-Outs
1. **Site Scoring**: Rank sites by power availability, fiber density, latency, land cost
2. **Power Analysis**: Grid capacity, substation proximity, interconnection queue times
3. **Connectivity Assessment**: Fiber routes, IX proximity, carrier presence
4. **Cooling Considerations**: Climate data, water availability
5. **Heavy Haul Planning**: Routes for transformers (500+ tons), generators, HVAC

### Industrial Facility Build-Outs
1. **Logistics Optimization**: Proximity to ports, rail, intermodal facilities
2. **Workforce Analysis**: Labor availability, wages, skills match
3. **Supply Chain Access**: 3PL coverage, warehouse capacity, trucking lanes
4. **Regulatory Assessment**: Zoning, permitting timelines, environmental constraints
5. **Incentive Stacking**: State/local incentives, OZ, FTZ benefits

---

## Data Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SITE INTELLIGENCE PLATFORM                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────┐│
│  │    POWER     │ │   TELECOM    │ │  TRANSPORT   │ │   LABOR     ││
│  │  - EIA       │ │  - FCC BDC   │ │  - BTS NTAD  │ │  - BLS OES  ││
│  │  - NREL      │ │  - PeeringDB │ │  - FRA Rail  │ │  - Census   ││
│  │  - ISO/RTO   │ │  - Submarine │ │  - FAA Cargo │ │  - LEHD     ││
│  │  - GridStatus│ │    Cables    │ │  - USACE     │ │  - QCEW     ││
│  └──────────────┘ └──────────────┘ └──────────────┘ └─────────────┘│
│                                                                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌─────────────┐│
│  │    RISK      │ │  REAL ESTATE │ │  INCENTIVES  │ │  LOGISTICS  ││
│  │  - FEMA NFHL │ │  - Regrid    │ │  - FTZ Board │ │  - Port     ││
│  │  - USGS Seis │ │  - Zoning    │ │  - OZ/CDFI   │ │    Metrics  ││
│  │  - NOAA Clim │ │  - Permits   │ │  - State EDO │ │  - Freight  ││
│  │  - EPA Enviro│ │  - EDO Sites │ │  - Tax Found │ │    Rates    ││
│  └──────────────┘ └──────────────┘ └──────────────┘ └─────────────┘│
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐│
│  │                     SITE SCORING ENGINE                         ││
│  │  Weighted composite scores by use case (DC, Warehouse, Mfg)     ││
│  └─────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────┘
```

---

## DOMAIN 1: Power Infrastructure

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **EIA Open Data** | REST API (free key) | Power plants, capacity, utility territories, prices | P0 |
| **NREL Developer** | REST API (free key) | Solar/wind resources, utility rates | P0 |
| **HIFLD Substations** | ArcGIS REST | Substation locations, voltage levels | P0 |
| **GridStatus.io** | API | Real-time grid data, LMPs | P1 |
| **ISO Interconnection Queues** | Scrape/Download | Projects in queue, wait times | P1 |
| **EPA eGRID** | Download | Emissions, grid carbon intensity | P2 |

### Database Tables

```sql
-- Power plant registry
CREATE TABLE power_plant (
    id SERIAL PRIMARY KEY,
    eia_plant_id VARCHAR(20) UNIQUE,
    name VARCHAR(255) NOT NULL,
    operator_name VARCHAR(255),
    state VARCHAR(2),
    county VARCHAR(100),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    primary_fuel VARCHAR(50),           -- natural_gas, coal, solar, wind, nuclear
    nameplate_capacity_mw DECIMAL(12, 2),
    summer_capacity_mw DECIMAL(12, 2),
    winter_capacity_mw DECIMAL(12, 2),
    operating_year INTEGER,
    grid_region VARCHAR(20),            -- PJM, ERCOT, CAISO, etc.
    balancing_authority VARCHAR(100),
    nerc_region VARCHAR(10),
    co2_rate_tons_mwh DECIMAL(8, 4),
    source VARCHAR(50) DEFAULT 'eia',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_power_plant_location ON power_plant(latitude, longitude);
CREATE INDEX idx_power_plant_fuel ON power_plant(primary_fuel);

-- Electrical substations
CREATE TABLE substation (
    id SERIAL PRIMARY KEY,
    hifld_id VARCHAR(50) UNIQUE,
    name VARCHAR(255),
    state VARCHAR(2),
    county VARCHAR(100),
    city VARCHAR(100),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    substation_type VARCHAR(50),        -- transmission, distribution
    max_voltage_kv DECIMAL(10, 2),
    min_voltage_kv DECIMAL(10, 2),
    owner VARCHAR(255),
    status VARCHAR(30),                 -- operational, planned, retired
    source VARCHAR(50) DEFAULT 'hifld',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_substation_location ON substation(latitude, longitude);
CREATE INDEX idx_substation_voltage ON substation(max_voltage_kv);

-- Utility service territories
CREATE TABLE utility_territory (
    id SERIAL PRIMARY KEY,
    eia_utility_id INTEGER,
    utility_name VARCHAR(255) NOT NULL,
    utility_type VARCHAR(50),           -- investor_owned, municipal, coop
    state VARCHAR(2),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    customers_residential INTEGER,
    customers_commercial INTEGER,
    customers_industrial INTEGER,
    avg_rate_residential DECIMAL(8, 4), -- $/kWh
    avg_rate_commercial DECIMAL(8, 4),
    avg_rate_industrial DECIMAL(8, 4),
    source VARCHAR(50) DEFAULT 'eia',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_utility_territory_geom ON utility_territory USING GIST(geometry);

-- Grid interconnection queues (for new power projects)
CREATE TABLE interconnection_queue (
    id SERIAL PRIMARY KEY,
    iso_region VARCHAR(20) NOT NULL,    -- PJM, CAISO, ERCOT, etc.
    queue_id VARCHAR(50),
    project_name VARCHAR(500),
    developer VARCHAR(255),
    fuel_type VARCHAR(50),
    capacity_mw DECIMAL(12, 2),
    state VARCHAR(2),
    county VARCHAR(100),
    point_of_interconnection VARCHAR(255),
    queue_date DATE,
    target_cod DATE,                    -- Commercial Operation Date
    status VARCHAR(50),                 -- active, withdrawn, completed
    study_phase VARCHAR(50),            -- feasibility, system_impact, facilities
    upgrade_cost_million DECIMAL(12, 2),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(iso_region, queue_id)
);

-- Electricity pricing by region/utility
CREATE TABLE electricity_price (
    id SERIAL PRIMARY KEY,
    geography_type VARCHAR(20),         -- state, utility, iso_zone
    geography_id VARCHAR(50),
    geography_name VARCHAR(255),
    period_year INTEGER,
    period_month INTEGER,
    sector VARCHAR(30),                 -- residential, commercial, industrial
    avg_price_cents_kwh DECIMAL(8, 4),
    total_sales_mwh BIGINT,
    total_revenue_thousand BIGINT,
    customer_count INTEGER,
    source VARCHAR(50) DEFAULT 'eia',
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(geography_type, geography_id, period_year, period_month, sector)
);

-- Renewable energy resources (solar/wind potential)
CREATE TABLE renewable_resource (
    id SERIAL PRIMARY KEY,
    resource_type VARCHAR(20) NOT NULL, -- solar, wind
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    state VARCHAR(2),
    county VARCHAR(100),
    -- Solar fields
    ghi_kwh_m2_day DECIMAL(8, 4),       -- Global Horizontal Irradiance
    dni_kwh_m2_day DECIMAL(8, 4),       -- Direct Normal Irradiance
    -- Wind fields
    wind_speed_100m_ms DECIMAL(6, 2),   -- meters/second at 100m
    wind_power_density_w_m2 DECIMAL(8, 2),
    capacity_factor_pct DECIMAL(5, 2),
    source VARCHAR(50) DEFAULT 'nrel',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_renewable_resource_location ON renewable_resource(latitude, longitude);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/power/plants` | GET | Search power plants with filters |
| `/api/v1/power/plants/nearby` | GET | Find plants within radius of coordinates |
| `/api/v1/power/substations` | GET | Search substations |
| `/api/v1/power/substations/nearby` | GET | Find substations within radius |
| `/api/v1/power/utilities` | GET | List utilities with pricing |
| `/api/v1/power/utilities/at-location` | GET | Get utility serving a lat/lng |
| `/api/v1/power/prices` | GET | Query electricity prices |
| `/api/v1/power/interconnection-queue` | GET | Search queue by region |
| `/api/v1/power/grid-capacity` | GET | Assess available capacity at location |
| `/api/v1/power/renewable-potential` | GET | Solar/wind resources at location |

---

## DOMAIN 2: Telecom/Fiber Infrastructure

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **FCC Broadband Data** | Bulk download | ISP coverage, speeds, technology | P0 |
| **PeeringDB** | REST API | IX locations, data centers, networks | P0 |
| **Telegeography Submarine** | Scrape | Submarine cable landing points | P1 |
| **HIFLD Cell Towers** | Download | Tower locations | P2 |
| **RIPE Atlas** | API | Latency measurements | P2 |

### Database Tables

```sql
-- Broadband availability by location
CREATE TABLE broadband_availability (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(50),            -- FCC location ID
    block_geoid VARCHAR(15),            -- Census block
    state VARCHAR(2),
    county VARCHAR(100),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    provider_name VARCHAR(255),
    technology VARCHAR(50),             -- fiber, cable, fixed_wireless, dsl
    max_download_mbps INTEGER,
    max_upload_mbps INTEGER,
    is_business_service BOOLEAN,
    fcc_filing_date DATE,
    source VARCHAR(50) DEFAULT 'fcc',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_broadband_location ON broadband_availability(latitude, longitude);
CREATE INDEX idx_broadband_block ON broadband_availability(block_geoid);

-- Internet Exchange Points
CREATE TABLE internet_exchange (
    id SERIAL PRIMARY KEY,
    peeringdb_id INTEGER UNIQUE,
    name VARCHAR(255) NOT NULL,
    name_long VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(3),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    website VARCHAR(500),
    network_count INTEGER,              -- Number of connected networks
    ipv4_prefixes INTEGER,
    ipv6_prefixes INTEGER,
    speed_gbps INTEGER,                 -- Total exchange capacity
    policy_general VARCHAR(50),         -- open, selective, restrictive
    source VARCHAR(50) DEFAULT 'peeringdb',
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Data center facilities (from PeeringDB)
CREATE TABLE data_center_facility (
    id SERIAL PRIMARY KEY,
    peeringdb_id INTEGER UNIQUE,
    name VARCHAR(255) NOT NULL,
    operator VARCHAR(255),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(3),
    postal_code VARCHAR(20),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    website VARCHAR(500),
    network_count INTEGER,              -- Networks present
    ix_count INTEGER,                   -- IX connections
    floor_space_sqft INTEGER,
    power_mw DECIMAL(8, 2),
    pue DECIMAL(4, 2),                  -- Power Usage Effectiveness
    tier_certification VARCHAR(20),     -- Tier I, II, III, IV
    source VARCHAR(50) DEFAULT 'peeringdb',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_dc_facility_location ON data_center_facility(latitude, longitude);

-- Submarine cable landing points
CREATE TABLE submarine_cable_landing (
    id SERIAL PRIMARY KEY,
    cable_name VARCHAR(255) NOT NULL,
    landing_point_name VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(3),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    cable_length_km INTEGER,
    capacity_tbps DECIMAL(10, 2),
    rfs_date DATE,                      -- Ready for Service
    owners TEXT[],
    source VARCHAR(50) DEFAULT 'telegeography',
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Network latency measurements
CREATE TABLE network_latency (
    id SERIAL PRIMARY KEY,
    source_city VARCHAR(100),
    source_country VARCHAR(3),
    source_latitude DECIMAL(10, 7),
    source_longitude DECIMAL(10, 7),
    target_city VARCHAR(100),
    target_country VARCHAR(3),
    target_latitude DECIMAL(10, 7),
    target_longitude DECIMAL(10, 7),
    measurement_date DATE,
    latency_ms_avg DECIMAL(8, 2),
    latency_ms_min DECIMAL(8, 2),
    latency_ms_max DECIMAL(8, 2),
    latency_ms_p95 DECIMAL(8, 2),
    sample_count INTEGER,
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/telecom/broadband` | GET | Query broadband availability |
| `/api/v1/telecom/broadband/at-location` | GET | Get ISPs serving coordinates |
| `/api/v1/telecom/ix` | GET | List internet exchanges |
| `/api/v1/telecom/ix/nearby` | GET | Find IX within radius |
| `/api/v1/telecom/data-centers` | GET | Search data center facilities |
| `/api/v1/telecom/data-centers/nearby` | GET | Find DCs within radius |
| `/api/v1/telecom/submarine-cables` | GET | List cable landing points |
| `/api/v1/telecom/latency` | GET | Get latency data between points |
| `/api/v1/telecom/connectivity-score` | GET | Composite connectivity score |

---

## DOMAIN 3: Transportation Infrastructure

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **BTS NTAD** | Download | Intermodal, rail, ports, airports | P0 |
| **FRA Rail Network** | Download | Rail lines, crossings, terminals | P0 |
| **USACE Ports** | Download/Scrape | Port throughput, tonnage | P0 |
| **FAA Airport Data** | Download | Airport facilities, cargo | P1 |
| **FHWA Freight Network** | Download | Freight corridors | P1 |
| **State DOT OSOW** | Scrape | Heavy haul permit routes | P1 |

### Database Tables

```sql
-- Intermodal terminals (rail/truck)
CREATE TABLE intermodal_terminal (
    id SERIAL PRIMARY KEY,
    ntad_id VARCHAR(50),
    name VARCHAR(255) NOT NULL,
    operator VARCHAR(255),
    terminal_type VARCHAR(50),          -- ramp, port, warehouse
    railroad VARCHAR(100),              -- BNSF, UP, CSX, NS, etc.
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    county VARCHAR(100),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    annual_lifts INTEGER,               -- Container lifts per year
    track_miles DECIMAL(6, 2),
    parking_spaces INTEGER,
    has_on_dock_rail BOOLEAN,
    source VARCHAR(50) DEFAULT 'bts',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_intermodal_location ON intermodal_terminal(latitude, longitude);

-- Rail network segments
CREATE TABLE rail_line (
    id SERIAL PRIMARY KEY,
    fra_line_id VARCHAR(50),
    railroad VARCHAR(100),
    track_type VARCHAR(50),             -- mainline, branch, yard
    track_class INTEGER,                -- FRA class 1-9
    max_speed_mph INTEGER,
    annual_tonnage_million DECIMAL(10, 2),
    state VARCHAR(2),
    county VARCHAR(100),
    geometry GEOMETRY(LINESTRING, 4326),
    source VARCHAR(50) DEFAULT 'fra',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_rail_line_geom ON rail_line USING GIST(geometry);

-- Ports
CREATE TABLE port (
    id SERIAL PRIMARY KEY,
    port_code VARCHAR(10) NOT NULL,     -- UN/LOCODE
    name VARCHAR(255) NOT NULL,
    port_type VARCHAR(50),              -- seaport, river, great_lakes
    city VARCHAR(100),
    state VARCHAR(2),
    country VARCHAR(3),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    has_container_terminal BOOLEAN,
    has_bulk_terminal BOOLEAN,
    has_liquid_terminal BOOLEAN,
    has_roro_terminal BOOLEAN,
    channel_depth_ft INTEGER,
    source VARCHAR(50) DEFAULT 'usace',
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(port_code)
);

-- Port throughput metrics (time series)
CREATE TABLE port_throughput (
    id SERIAL PRIMARY KEY,
    port_id INTEGER REFERENCES port(id),
    period_year INTEGER,
    period_month INTEGER,
    teu_import INTEGER,
    teu_export INTEGER,
    teu_total INTEGER,
    tonnage_import_thousand DECIMAL(12, 2),
    tonnage_export_thousand DECIMAL(12, 2),
    tonnage_total_thousand DECIMAL(12, 2),
    vessel_calls INTEGER,
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(port_id, period_year, period_month, source)
);

-- Airports with cargo facilities
CREATE TABLE airport (
    id SERIAL PRIMARY KEY,
    faa_code VARCHAR(10),
    icao_code VARCHAR(10),
    name VARCHAR(255) NOT NULL,
    city VARCHAR(100),
    state VARCHAR(2),
    country VARCHAR(3),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    airport_type VARCHAR(50),           -- large_hub, medium_hub, small_hub, cargo
    has_cargo_facility BOOLEAN,
    longest_runway_ft INTEGER,
    cargo_tonnage_annual DECIMAL(12, 2),
    source VARCHAR(50) DEFAULT 'faa',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_airport_location ON airport(latitude, longitude);

-- Highway freight corridors
CREATE TABLE freight_corridor (
    id SERIAL PRIMARY KEY,
    corridor_name VARCHAR(255),
    corridor_type VARCHAR(50),          -- primary, critical_urban, critical_rural
    route_number VARCHAR(50),
    state VARCHAR(2),
    truck_aadt INTEGER,                 -- Average Annual Daily Truck Traffic
    geometry GEOMETRY(LINESTRING, 4326),
    source VARCHAR(50) DEFAULT 'fhwa',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_freight_corridor_geom ON freight_corridor USING GIST(geometry);

-- Heavy haul routes (for transformers, generators, etc.)
CREATE TABLE heavy_haul_route (
    id SERIAL PRIMARY KEY,
    route_name VARCHAR(255),
    state VARCHAR(2),
    max_weight_lbs INTEGER,
    max_height_ft DECIMAL(5, 2),
    max_width_ft DECIMAL(5, 2),
    max_length_ft DECIMAL(5, 2),
    permit_required BOOLEAN,
    restrictions TEXT,
    geometry GEOMETRY(LINESTRING, 4326),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_heavy_haul_geom ON heavy_haul_route USING GIST(geometry);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/transport/intermodal` | GET | Search intermodal terminals |
| `/api/v1/transport/intermodal/nearby` | GET | Find terminals within radius |
| `/api/v1/transport/rail` | GET | Query rail network |
| `/api/v1/transport/rail/access` | GET | Check rail access for location |
| `/api/v1/transport/ports` | GET | List ports with filters |
| `/api/v1/transport/ports/{code}/throughput` | GET | Port throughput history |
| `/api/v1/transport/airports` | GET | Search cargo airports |
| `/api/v1/transport/airports/nearby` | GET | Find airports within radius |
| `/api/v1/transport/freight-corridors` | GET | Query freight network |
| `/api/v1/transport/heavy-haul` | GET | Find heavy haul routes |
| `/api/v1/transport/heavy-haul/route` | POST | Plan route for oversize load |

---

## DOMAIN 4: Labor Market

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **BLS OES** | API | Occupational wages by metro | P0 |
| **BLS QCEW** | API/Download | Employment by industry/county | P0 |
| **Census LEHD** | API | Commuting patterns, job flows | P0 |
| **Census ACS** | API | Demographics, education | P1 |
| **BLS Unemployment** | API | Unemployment rates | P1 |

### Database Tables

```sql
-- Labor market area definitions
CREATE TABLE labor_market_area (
    id SERIAL PRIMARY KEY,
    area_type VARCHAR(30),              -- metro, county, state
    area_code VARCHAR(20),              -- FIPS or CBSA code
    area_name VARCHAR(255),
    state VARCHAR(2),
    population INTEGER,
    labor_force INTEGER,
    employment INTEGER,
    unemployment_rate DECIMAL(5, 2),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(area_type, area_code)
);
CREATE INDEX idx_labor_area_geom ON labor_market_area USING GIST(geometry);

-- Occupational employment and wages
CREATE TABLE occupational_wage (
    id SERIAL PRIMARY KEY,
    area_type VARCHAR(30),
    area_code VARCHAR(20),
    area_name VARCHAR(255),
    occupation_code VARCHAR(20),        -- SOC code
    occupation_title VARCHAR(255),
    employment INTEGER,
    mean_hourly_wage DECIMAL(10, 2),
    median_hourly_wage DECIMAL(10, 2),
    pct_10_hourly DECIMAL(10, 2),
    pct_25_hourly DECIMAL(10, 2),
    pct_75_hourly DECIMAL(10, 2),
    pct_90_hourly DECIMAL(10, 2),
    mean_annual_wage DECIMAL(12, 2),
    median_annual_wage DECIMAL(12, 2),
    period_year INTEGER,
    period_month INTEGER,
    source VARCHAR(50) DEFAULT 'bls_oes',
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(area_code, occupation_code, period_year)
);

-- Industry employment by county (QCEW)
CREATE TABLE industry_employment (
    id SERIAL PRIMARY KEY,
    area_fips VARCHAR(10),
    area_name VARCHAR(255),
    industry_code VARCHAR(10),          -- NAICS
    industry_title VARCHAR(255),
    ownership VARCHAR(30),              -- private, federal, state, local
    period_year INTEGER,
    period_quarter INTEGER,
    establishments INTEGER,
    avg_monthly_employment INTEGER,
    total_wages_thousand BIGINT,
    avg_weekly_wage DECIMAL(10, 2),
    source VARCHAR(50) DEFAULT 'bls_qcew',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_industry_emp_fips ON industry_employment(area_fips);
CREATE INDEX idx_industry_emp_naics ON industry_employment(industry_code);

-- Commuting patterns (LEHD)
CREATE TABLE commute_flow (
    id SERIAL PRIMARY KEY,
    home_county_fips VARCHAR(10),
    home_county_name VARCHAR(255),
    home_state VARCHAR(2),
    work_county_fips VARCHAR(10),
    work_county_name VARCHAR(255),
    work_state VARCHAR(2),
    worker_count INTEGER,
    avg_earnings DECIMAL(10, 2),
    avg_age DECIMAL(4, 1),
    period_year INTEGER,
    source VARCHAR(50) DEFAULT 'census_lehd',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_commute_home ON commute_flow(home_county_fips);
CREATE INDEX idx_commute_work ON commute_flow(work_county_fips);

-- Educational attainment by area
CREATE TABLE educational_attainment (
    id SERIAL PRIMARY KEY,
    area_fips VARCHAR(10),
    area_name VARCHAR(255),
    area_type VARCHAR(30),
    population_25_plus INTEGER,
    pct_high_school DECIMAL(5, 2),
    pct_some_college DECIMAL(5, 2),
    pct_associates DECIMAL(5, 2),
    pct_bachelors DECIMAL(5, 2),
    pct_graduate DECIMAL(5, 2),
    period_year INTEGER,
    source VARCHAR(50) DEFAULT 'census_acs',
    collected_at TIMESTAMP DEFAULT NOW()
);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/labor/areas` | GET | List labor market areas |
| `/api/v1/labor/areas/at-location` | GET | Get labor area for coordinates |
| `/api/v1/labor/wages` | GET | Query occupational wages |
| `/api/v1/labor/wages/comparison` | GET | Compare wages across areas |
| `/api/v1/labor/employment` | GET | Query industry employment |
| `/api/v1/labor/commute-shed` | GET | Get commute patterns to location |
| `/api/v1/labor/education` | GET | Educational attainment by area |
| `/api/v1/labor/workforce-score` | GET | Composite workforce score |

---

## DOMAIN 5: Risk & Environmental

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **FEMA NFHL** | WMS/Download | Flood zones | P0 |
| **USGS Earthquake** | API | Seismic hazard, faults | P0 |
| **NOAA Climate** | API | Historical weather, projections | P0 |
| **EPA Envirofacts** | API | Permits, violations, facilities | P1 |
| **NWI Wetlands** | Download | Wetland boundaries | P1 |
| **USFWS Critical Habitat** | Download | Endangered species | P2 |

### Database Tables

```sql
-- Flood zones
CREATE TABLE flood_zone (
    id SERIAL PRIMARY KEY,
    zone_code VARCHAR(20),              -- A, AE, AH, AO, V, VE, X
    zone_description VARCHAR(255),
    is_high_risk BOOLEAN,               -- Zone A/V = high risk
    is_coastal BOOLEAN,
    base_flood_elevation_ft DECIMAL(8, 2),
    state VARCHAR(2),
    county VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    effective_date DATE,
    source VARCHAR(50) DEFAULT 'fema',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_flood_zone_geom ON flood_zone USING GIST(geometry);

-- Seismic hazard
CREATE TABLE seismic_hazard (
    id SERIAL PRIMARY KEY,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    pga_2pct_50yr DECIMAL(8, 4),        -- Peak Ground Acceleration (g)
    pga_10pct_50yr DECIMAL(8, 4),
    spectral_1sec_2pct DECIMAL(8, 4),
    spectral_02sec_2pct DECIMAL(8, 4),
    site_class VARCHAR(10),             -- A, B, C, D, E
    seismic_design_category VARCHAR(5), -- A, B, C, D, E, F
    source VARCHAR(50) DEFAULT 'usgs',
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Active faults
CREATE TABLE fault_line (
    id SERIAL PRIMARY KEY,
    fault_name VARCHAR(255),
    fault_type VARCHAR(50),             -- strike_slip, normal, reverse
    slip_rate_mm_yr DECIMAL(8, 2),
    age VARCHAR(50),                    -- historic, holocene, quaternary
    geometry GEOMETRY(LINESTRING, 4326),
    source VARCHAR(50) DEFAULT 'usgs',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_fault_geom ON fault_line USING GIST(geometry);

-- Climate normals and extremes
CREATE TABLE climate_data (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(20),
    station_name VARCHAR(255),
    state VARCHAR(2),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    elevation_ft INTEGER,
    -- Temperature (Fahrenheit)
    avg_temp_annual DECIMAL(5, 1),
    avg_temp_jan DECIMAL(5, 1),
    avg_temp_jul DECIMAL(5, 1),
    record_high DECIMAL(5, 1),
    record_low DECIMAL(5, 1),
    days_above_90 INTEGER,
    days_below_32 INTEGER,
    -- Precipitation
    precip_annual_inches DECIMAL(6, 2),
    snowfall_annual_inches DECIMAL(6, 2),
    -- Degree days (for HVAC sizing)
    cooling_degree_days INTEGER,
    heating_degree_days INTEGER,
    -- Extremes
    max_wind_mph INTEGER,
    tornado_risk_score INTEGER,         -- 1-10
    hurricane_risk_score INTEGER,       -- 1-10
    source VARCHAR(50) DEFAULT 'noaa',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_climate_location ON climate_data(latitude, longitude);

-- Environmental permits/facilities
CREATE TABLE environmental_facility (
    id SERIAL PRIMARY KEY,
    epa_id VARCHAR(50),
    facility_name VARCHAR(255),
    facility_type VARCHAR(100),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    zip VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    permits TEXT[],                     -- RCRA, CAA, CWA, etc.
    violations_5yr INTEGER,
    is_superfund BOOLEAN,
    is_brownfield BOOLEAN,
    source VARCHAR(50) DEFAULT 'epa',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_env_facility_location ON environmental_facility(latitude, longitude);

-- Wetlands
CREATE TABLE wetland (
    id SERIAL PRIMARY KEY,
    nwi_code VARCHAR(20),
    wetland_type VARCHAR(100),
    modifier VARCHAR(50),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    acres DECIMAL(12, 2),
    state VARCHAR(2),
    source VARCHAR(50) DEFAULT 'usfws',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_wetland_geom ON wetland USING GIST(geometry);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/risk/flood` | GET | Query flood zones |
| `/api/v1/risk/flood/at-location` | GET | Get flood zone for coordinates |
| `/api/v1/risk/seismic` | GET | Query seismic hazard data |
| `/api/v1/risk/seismic/at-location` | GET | Get seismic risk for coordinates |
| `/api/v1/risk/faults/nearby` | GET | Find faults within radius |
| `/api/v1/risk/climate` | GET | Query climate data |
| `/api/v1/risk/climate/at-location` | GET | Get climate normals for location |
| `/api/v1/risk/environmental` | GET | Query EPA facilities |
| `/api/v1/risk/environmental/nearby` | GET | Find EPA sites within radius |
| `/api/v1/risk/wetlands/at-location` | GET | Check wetlands at location |
| `/api/v1/risk/score` | GET | Composite risk score |

---

## DOMAIN 6: Incentives & Real Estate

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **CDFI Fund OZ** | Download | Opportunity Zone tracts | P0 |
| **FTZ Board** | Scrape | Foreign Trade Zones | P0 |
| **State EDO Sites** | Scrape | Incentive programs, sites | P1 |
| **Good Jobs First** | API/Download | Disclosed incentive deals | P1 |
| **Regrid** | API (paid) | Parcel data, zoning | P2 |
| **National Zoning Atlas** | Download | Zoning classifications | P2 |

### Database Tables

```sql
-- Opportunity Zones
CREATE TABLE opportunity_zone (
    id SERIAL PRIMARY KEY,
    tract_geoid VARCHAR(15) UNIQUE,     -- Census tract FIPS
    state VARCHAR(2),
    county VARCHAR(100),
    tract_name VARCHAR(255),
    designation_date DATE,
    is_low_income BOOLEAN,
    is_contiguous BOOLEAN,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    source VARCHAR(50) DEFAULT 'cdfi',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_oz_geom ON opportunity_zone USING GIST(geometry);

-- Foreign Trade Zones
CREATE TABLE foreign_trade_zone (
    id SERIAL PRIMARY KEY,
    ftz_number INTEGER UNIQUE,
    zone_name VARCHAR(255),
    grantee VARCHAR(255),
    operator VARCHAR(255),
    state VARCHAR(2),
    city VARCHAR(100),
    address VARCHAR(500),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    acreage DECIMAL(10, 2),
    subzones INTEGER,
    status VARCHAR(30),                 -- active, pending
    activation_date DATE,
    source VARCHAR(50) DEFAULT 'ftzb',
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_ftz_location ON foreign_trade_zone(latitude, longitude);

-- State/local incentive programs
CREATE TABLE incentive_program (
    id SERIAL PRIMARY KEY,
    program_name VARCHAR(500) NOT NULL,
    program_type VARCHAR(100),          -- tax_credit, grant, abatement, financing
    geography_type VARCHAR(30),         -- state, county, city
    geography_name VARCHAR(255),
    state VARCHAR(2),
    target_industries TEXT[],
    target_investments TEXT[],          -- manufacturing, data_center, warehouse
    min_investment BIGINT,
    min_jobs INTEGER,
    max_benefit BIGINT,
    benefit_duration_years INTEGER,
    description TEXT,
    requirements TEXT,
    application_url VARCHAR(500),
    source VARCHAR(50),
    source_url VARCHAR(500),
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Disclosed incentive deals (Good Jobs First)
CREATE TABLE incentive_deal (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    parent_company VARCHAR(255),
    subsidy_type VARCHAR(100),
    subsidy_value BIGINT,
    year INTEGER,
    state VARCHAR(2),
    city VARCHAR(100),
    county VARCHAR(100),
    program_name VARCHAR(500),
    jobs_announced INTEGER,
    jobs_created INTEGER,
    investment_announced BIGINT,
    naics_code VARCHAR(10),
    industry VARCHAR(255),
    source VARCHAR(50) DEFAULT 'goodjobsfirst',
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Available industrial sites (from EDOs)
CREATE TABLE industrial_site (
    id SERIAL PRIMARY KEY,
    site_name VARCHAR(255),
    site_type VARCHAR(50),              -- greenfield, building, spec_building
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    county VARCHAR(100),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    acreage DECIMAL(10, 2),
    building_sqft INTEGER,
    available_sqft INTEGER,
    asking_price BIGINT,
    asking_price_per_sqft DECIMAL(10, 2),
    zoning VARCHAR(100),
    utilities_available JSONB,          -- electric, gas, water, sewer, fiber
    rail_served BOOLEAN,
    highway_access VARCHAR(255),
    edo_name VARCHAR(255),
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    listing_url VARCHAR(500),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_industrial_site_location ON industrial_site(latitude, longitude);

-- Zoning districts
CREATE TABLE zoning_district (
    id SERIAL PRIMARY KEY,
    jurisdiction VARCHAR(255),
    state VARCHAR(2),
    zone_code VARCHAR(50),
    zone_name VARCHAR(255),
    zone_category VARCHAR(50),          -- industrial, commercial, residential, mixed
    allows_manufacturing BOOLEAN,
    allows_warehouse BOOLEAN,
    allows_data_center BOOLEAN,
    max_height_ft INTEGER,
    max_far DECIMAL(6, 2),              -- Floor Area Ratio
    min_lot_sqft INTEGER,
    setback_front_ft INTEGER,
    setback_side_ft INTEGER,
    setback_rear_ft INTEGER,
    parking_ratio VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_zoning_geom ON zoning_district USING GIST(geometry);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/incentives/opportunity-zones` | GET | Query OZ tracts |
| `/api/v1/incentives/opportunity-zones/at-location` | GET | Check if location is in OZ |
| `/api/v1/incentives/ftz` | GET | List Foreign Trade Zones |
| `/api/v1/incentives/ftz/nearby` | GET | Find FTZ within radius |
| `/api/v1/incentives/programs` | GET | Search incentive programs |
| `/api/v1/incentives/programs/by-state` | GET | Programs by state |
| `/api/v1/incentives/deals` | GET | Search disclosed deals |
| `/api/v1/incentives/deals/benchmark` | GET | Benchmark deals by industry |
| `/api/v1/real-estate/sites` | GET | Search industrial sites |
| `/api/v1/real-estate/sites/nearby` | GET | Find sites within radius |
| `/api/v1/real-estate/zoning/at-location` | GET | Get zoning for coordinates |

---

## DOMAIN 7: Freight & Logistics Operations

### Data Sources

| Source | API/Method | Data | Priority |
|--------|------------|------|----------|
| **Freightos FBX** | Scrape/API | Container rates | P1 |
| **USDA AMS** | API | Truck rates | P1 |
| **DAT (limited)** | Public data | Spot rates | P2 |
| **CASS Freight** | Download | Shipment indices | P2 |

### Database Tables

```sql
-- Freight rate indices (container, trucking)
CREATE TABLE freight_rate_index (
    id SERIAL PRIMARY KEY,
    index_name VARCHAR(100) NOT NULL,
    index_code VARCHAR(50) NOT NULL,
    route_origin VARCHAR(100),
    route_destination VARCHAR(100),
    mode VARCHAR(30) NOT NULL,          -- ocean, trucking, rail, air
    rate_date DATE NOT NULL,
    rate_value DECIMAL(12, 2),
    rate_unit VARCHAR(30),              -- per_feu, per_mile, per_ton
    currency VARCHAR(3) DEFAULT 'USD',
    change_pct_wow DECIMAL(8, 4),
    change_pct_mom DECIMAL(8, 4),
    change_pct_yoy DECIMAL(8, 4),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(index_code, rate_date)
);

-- Trucking spot rates by lane
CREATE TABLE trucking_lane_rate (
    id SERIAL PRIMARY KEY,
    origin_market VARCHAR(100),
    origin_state VARCHAR(2),
    destination_market VARCHAR(100),
    destination_state VARCHAR(2),
    equipment_type VARCHAR(30),         -- van, reefer, flatbed
    rate_date DATE,
    rate_per_mile DECIMAL(8, 4),
    fuel_surcharge DECIMAL(8, 4),
    total_rate_per_mile DECIMAL(8, 4),
    load_count INTEGER,
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Warehouse/3PL facilities
CREATE TABLE warehouse_facility (
    id SERIAL PRIMARY KEY,
    facility_name VARCHAR(255),
    operator_name VARCHAR(255) NOT NULL,
    facility_type VARCHAR(50),          -- distribution, fulfillment, cold_storage, cross_dock
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(2),
    county VARCHAR(100),
    zip VARCHAR(10),
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    sqft_total INTEGER,
    sqft_available INTEGER,
    clear_height_ft INTEGER,
    dock_doors INTEGER,
    drive_in_doors INTEGER,
    trailer_parking INTEGER,
    has_cold_storage BOOLEAN,
    has_freezer BOOLEAN,
    has_hazmat BOOLEAN,
    has_ftz BOOLEAN,
    has_rail BOOLEAN,
    certifications JSONB,
    asking_rent_psf DECIMAL(8, 2),
    source VARCHAR(50),
    collected_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_warehouse_location ON warehouse_facility(latitude, longitude);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/logistics/freight-rates` | GET | Query freight rate indices |
| `/api/v1/logistics/freight-rates/latest` | GET | Get latest rates |
| `/api/v1/logistics/trucking-rates` | GET | Query trucking lane rates |
| `/api/v1/logistics/warehouses` | GET | Search warehouse facilities |
| `/api/v1/logistics/warehouses/nearby` | GET | Find warehouses within radius |

---

## DOMAIN 8: Site Scoring Engine

### Composite Scoring Model

```sql
-- Site scoring configuration
CREATE TABLE site_score_config (
    id SERIAL PRIMARY KEY,
    config_name VARCHAR(100) NOT NULL,
    use_case VARCHAR(50) NOT NULL,      -- data_center, warehouse, manufacturing
    factor_weights JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Example factor weights for data center:
-- {
--   "power_capacity": 0.25,
--   "power_price": 0.15,
--   "fiber_availability": 0.20,
--   "ix_proximity": 0.10,
--   "flood_risk": -0.10,
--   "seismic_risk": -0.05,
--   "labor_availability": 0.05,
--   "incentives": 0.10,
--   "climate_cooling": 0.10
-- }

-- Cached site scores
CREATE TABLE site_score (
    id SERIAL PRIMARY KEY,
    latitude DECIMAL(10, 7),
    longitude DECIMAL(10, 7),
    config_id INTEGER REFERENCES site_score_config(id),
    overall_score DECIMAL(5, 2),        -- 0-100
    factor_scores JSONB,                -- Individual factor scores
    computed_at TIMESTAMP DEFAULT NOW(),
    valid_until TIMESTAMP,
    UNIQUE(latitude, longitude, config_id)
);
CREATE INDEX idx_site_score_location ON site_score(latitude, longitude);
```

### Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/sites/score` | POST | Score a specific location |
| `/api/v1/sites/compare` | POST | Compare multiple locations |
| `/api/v1/sites/search` | POST | Find sites matching criteria |
| `/api/v1/sites/heatmap` | GET | Generate score heatmap for region |
| `/api/v1/sites/report` | POST | Generate detailed site report |

---

## Files to Create

### Source Modules

| File | Description |
|------|-------------|
| `app/sources/site_intel/__init__.py` | Package init |
| `app/sources/site_intel/types.py` | Enums, Pydantic models |
| **Power** | |
| `app/sources/site_intel/power/eia_collector.py` | EIA data collector |
| `app/sources/site_intel/power/nrel_collector.py` | NREL resources |
| `app/sources/site_intel/power/hifld_collector.py` | Substations |
| `app/sources/site_intel/power/iso_queue_collector.py` | Interconnection queues |
| **Telecom** | |
| `app/sources/site_intel/telecom/fcc_collector.py` | Broadband data |
| `app/sources/site_intel/telecom/peeringdb_collector.py` | IX and DC data |
| **Transport** | |
| `app/sources/site_intel/transport/bts_collector.py` | NTAD data |
| `app/sources/site_intel/transport/fra_collector.py` | Rail network |
| `app/sources/site_intel/transport/usace_collector.py` | Port data |
| **Labor** | |
| `app/sources/site_intel/labor/bls_collector.py` | Wages, employment |
| `app/sources/site_intel/labor/census_collector.py` | LEHD, ACS |
| **Risk** | |
| `app/sources/site_intel/risk/fema_collector.py` | Flood zones |
| `app/sources/site_intel/risk/usgs_collector.py` | Seismic data |
| `app/sources/site_intel/risk/noaa_collector.py` | Climate data |
| `app/sources/site_intel/risk/epa_collector.py` | Environmental |
| **Incentives** | |
| `app/sources/site_intel/incentives/oz_collector.py` | Opportunity Zones |
| `app/sources/site_intel/incentives/ftz_collector.py` | FTZ data |
| `app/sources/site_intel/incentives/edo_collector.py` | State EDO sites |
| **Scoring** | |
| `app/sources/site_intel/scoring/engine.py` | Scoring engine |
| `app/sources/site_intel/scoring/factors.py` | Factor calculations |
| **Orchestrator** | |
| `app/sources/site_intel/runner.py` | Main orchestrator |

### API Routers

| File | Description |
|------|-------------|
| `app/api/v1/power.py` | Power infrastructure endpoints |
| `app/api/v1/telecom.py` | Telecom/fiber endpoints |
| `app/api/v1/transport.py` | Transportation endpoints |
| `app/api/v1/labor.py` | Labor market endpoints |
| `app/api/v1/risk.py` | Risk assessment endpoints |
| `app/api/v1/incentives.py` | Incentives endpoints |
| `app/api/v1/logistics.py` | Freight/warehouse endpoints |
| `app/api/v1/sites.py` | Site scoring endpoints |

### Core Updates

| File | Action |
|------|--------|
| `app/core/models.py` | Add all new tables |
| `app/main.py` | Register all routers |

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)
- [ ] Create all database tables
- [ ] Set up `site_intel` source structure
- [ ] Implement base collector patterns
- [ ] Create API router stubs

### Phase 2: Power & Telecom (Week 3-4)
- [ ] EIA power plant/utility collector
- [ ] HIFLD substation collector
- [ ] NREL renewable collector
- [ ] FCC broadband collector
- [ ] PeeringDB IX/DC collector
- [ ] Power and telecom API endpoints

### Phase 3: Transport & Labor (Week 5-6)
- [ ] BTS NTAD collector (intermodal, ports, airports)
- [ ] FRA rail network collector
- [ ] BLS wage/employment collector
- [ ] Census LEHD commute collector
- [ ] Transport and labor API endpoints

### Phase 4: Risk & Incentives (Week 7-8)
- [ ] FEMA flood zone collector
- [ ] USGS seismic collector
- [ ] NOAA climate collector
- [ ] EPA environmental collector
- [ ] OZ/FTZ collectors
- [ ] Risk and incentives API endpoints

### Phase 5: Scoring & Integration (Week 9-10)
- [ ] Implement scoring engine
- [ ] Create factor calculation functions
- [ ] Site comparison and search
- [ ] Report generation
- [ ] Integration testing

### Phase 6: Data Population (Week 11-12)
- [ ] Backfill historical data
- [ ] Set up scheduled collection jobs
- [ ] Performance optimization
- [ ] Documentation

---

## Example Usage

### Score a Data Center Site
```bash
curl -X POST http://localhost:8001/api/v1/sites/score \
  -H "Content-Type: application/json" \
  -d '{
    "latitude": 39.0458,
    "longitude": -76.6413,
    "use_case": "data_center",
    "min_power_mw": 50
  }'
```

### Compare Multiple Sites
```bash
curl -X POST http://localhost:8001/api/v1/sites/compare \
  -H "Content-Type: application/json" \
  -d '{
    "locations": [
      {"name": "Ashburn VA", "lat": 39.0458, "lng": -77.4875},
      {"name": "Columbus OH", "lat": 39.9612, "lng": -82.9988},
      {"name": "Phoenix AZ", "lat": 33.4484, "lng": -112.0740}
    ],
    "use_case": "data_center",
    "factors": ["power", "telecom", "risk", "incentives"]
  }'
```

### Search for Industrial Sites
```bash
curl -X POST http://localhost:8001/api/v1/sites/search \
  -H "Content-Type: application/json" \
  -d '{
    "use_case": "warehouse",
    "region": {"states": ["TX", "AZ", "NV"]},
    "requirements": {
      "min_acreage": 50,
      "rail_required": true,
      "max_flood_risk": "moderate"
    },
    "sort_by": "overall_score",
    "limit": 20
  }'
```

### Get Power Infrastructure Near Site
```bash
curl "http://localhost:8001/api/v1/power/substations/nearby?lat=39.0458&lng=-77.4875&radius_miles=25&min_voltage_kv=115"
```

### Check Flood Risk
```bash
curl "http://localhost:8001/api/v1/risk/flood/at-location?lat=29.7604&lng=-95.3698"
```

---

## Dependencies

### Required
- PostgreSQL 14+ with PostGIS extension
- httpx for async HTTP
- shapely/geopandas for spatial operations
- pandas for data processing

### API Keys (Free)
- EIA API key: https://www.eia.gov/opendata/register.php
- NREL API key: https://developer.nrel.gov/signup/
- Census API key: https://api.census.gov/data/key_signup.html

### Optional (Paid)
- Regrid API for parcel data
- GridStatus.io for real-time grid data
- CoStar for real estate data

---

## Success Criteria

- [ ] 40+ database tables created
- [ ] 15+ data collectors operational
- [ ] 50+ API endpoints functional
- [ ] Site scoring engine with 20+ factors
- [ ] Sub-second query performance for location lookups
- [ ] Historical data backfilled (5+ years where available)
- [ ] Scheduled collection jobs running
- [ ] API documentation complete

---

## Approval

- [ ] User approved plan
- [ ] Ready to implement
