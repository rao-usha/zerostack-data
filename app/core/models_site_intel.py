"""
SQLAlchemy models for Site Intelligence Platform.

Tables for industrial and data center site selection across 9 domains:
- Power Infrastructure
- Telecom/Fiber Infrastructure
- Transportation Infrastructure
- Labor Market
- Risk & Environmental
- Incentives & Real Estate
- Freight & Logistics
- Water & Utilities
- Site Scoring
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column,
    Integer,
    BigInteger,
    String,
    DateTime,
    Date,
    Text,
    JSON,
    Boolean,
    Numeric,
    ForeignKey,
    Index,
    UniqueConstraint,
    Enum,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import relationship
import enum

# Import Base from main models to share the same declarative base
from app.core.models import Base

# Try to import GeoAlchemy2, fall back gracefully if not available
try:
    from geoalchemy2 import Geometry

    HAS_POSTGIS = True
except ImportError:
    HAS_POSTGIS = False
    Geometry = None


# =============================================================================
# DOMAIN 1: POWER INFRASTRUCTURE
# =============================================================================


class PowerPlant(Base):
    """Power plant registry from EIA."""

    __tablename__ = "power_plant"

    id = Column(Integer, primary_key=True, autoincrement=True)
    eia_plant_id = Column(String(20), unique=True, index=True)
    name = Column(String(255), nullable=False)
    operator_name = Column(String(255))
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    primary_fuel = Column(
        String(50), index=True
    )  # natural_gas, coal, solar, wind, nuclear
    nameplate_capacity_mw = Column(Numeric(12, 2))
    summer_capacity_mw = Column(Numeric(12, 2))
    winter_capacity_mw = Column(Numeric(12, 2))
    operating_year = Column(Integer)
    grid_region = Column(String(20))  # PJM, ERCOT, CAISO, etc.
    balancing_authority = Column(String(100))
    nerc_region = Column(String(10))
    co2_rate_tons_mwh = Column(Numeric(8, 4))
    source = Column(String(50), default="eia")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_power_plant_location", "latitude", "longitude"),)


class Substation(Base):
    """Electrical substations from HIFLD."""

    __tablename__ = "substation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    hifld_id = Column(String(50), unique=True, index=True)
    name = Column(String(255))
    state = Column(String(2), index=True)
    county = Column(String(100))
    city = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    substation_type = Column(String(50))  # transmission, distribution
    max_voltage_kv = Column(Numeric(10, 2), index=True)
    min_voltage_kv = Column(Numeric(10, 2))
    owner = Column(String(255))
    status = Column(String(30))  # operational, planned, retired
    source = Column(String(50), default="hifld")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_substation_location", "latitude", "longitude"),)


class UtilityTerritory(Base):
    """Utility service territories from EIA."""

    __tablename__ = "utility_territory"

    id = Column(Integer, primary_key=True, autoincrement=True)
    eia_utility_id = Column(Integer, index=True)
    utility_name = Column(String(255), nullable=False)
    utility_type = Column(String(50))  # investor_owned, municipal, coop
    state = Column(String(2), index=True)
    # Geometry stored as GeoJSON in JSONB for compatibility
    geometry_geojson = Column(JSON)
    customers_residential = Column(Integer)
    customers_commercial = Column(Integer)
    customers_industrial = Column(Integer)
    avg_rate_residential = Column(Numeric(8, 4))  # $/kWh
    avg_rate_commercial = Column(Numeric(8, 4))
    avg_rate_industrial = Column(Numeric(8, 4))
    source = Column(String(50), default="eia")
    collected_at = Column(DateTime, default=datetime.utcnow)


class InterconnectionQueue(Base):
    """Grid interconnection queue entries."""

    __tablename__ = "interconnection_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    iso_region = Column(String(20), nullable=False, index=True)  # PJM, CAISO, ERCOT
    queue_id = Column(String(50))
    project_name = Column(String(500))
    developer = Column(String(255))
    fuel_type = Column(String(50))
    capacity_mw = Column(Numeric(12, 2))
    state = Column(String(2))
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    point_of_interconnection = Column(String(255))
    queue_date = Column(Date)
    target_cod = Column(Date)  # Commercial Operation Date
    status = Column(String(50))  # active, withdrawn, completed
    study_phase = Column(String(50))  # feasibility, system_impact, facilities
    upgrade_cost_million = Column(Numeric(12, 2))
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("iso_region", "queue_id", name="uq_interconnection_queue"),
        Index("idx_interconnection_queue_location", "latitude", "longitude"),
    )


class ElectricityPrice(Base):
    """Electricity pricing by region/utility."""

    __tablename__ = "electricity_price"

    id = Column(Integer, primary_key=True, autoincrement=True)
    geography_type = Column(String(20))  # state, utility, iso_zone
    geography_id = Column(String(50))
    geography_name = Column(String(255))
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer)
    sector = Column(String(30))  # residential, commercial, industrial
    avg_price_cents_kwh = Column(Numeric(8, 4))
    total_sales_mwh = Column(BigInteger)
    total_revenue_thousand = Column(BigInteger)
    customer_count = Column(Integer)
    source = Column(String(50), default="eia")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "geography_type",
            "geography_id",
            "period_year",
            "period_month",
            "sector",
            name="uq_electricity_price",
        ),
        Index("idx_electricity_price_geo", "geography_type", "geography_id"),
    )


class RenewableResource(Base):
    """Solar/wind resource potential from NREL."""

    __tablename__ = "renewable_resource"

    id = Column(Integer, primary_key=True, autoincrement=True)
    resource_type = Column(String(20), nullable=False)  # solar, wind
    latitude = Column(Numeric(10, 7), nullable=False)
    longitude = Column(Numeric(10, 7), nullable=False)
    state = Column(String(2))
    county = Column(String(100))
    # Solar fields
    ghi_kwh_m2_day = Column(Numeric(8, 4))  # Global Horizontal Irradiance
    dni_kwh_m2_day = Column(Numeric(8, 4))  # Direct Normal Irradiance
    # Wind fields
    wind_speed_100m_ms = Column(Numeric(6, 2))  # meters/second at 100m
    wind_power_density_w_m2 = Column(Numeric(8, 2))
    capacity_factor_pct = Column(Numeric(5, 2))
    source = Column(String(50), default="nrel")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_renewable_resource_location", "latitude", "longitude"),
        Index("idx_renewable_resource_type", "resource_type"),
    )


# =============================================================================
# DOMAIN 2: TELECOM/FIBER INFRASTRUCTURE
# =============================================================================


class BroadbandAvailability(Base):
    """Broadband availability by location from FCC."""

    __tablename__ = "broadband_availability"

    id = Column(Integer, primary_key=True, autoincrement=True)
    location_id = Column(String(50))  # FCC location ID
    block_geoid = Column(String(15), index=True)  # Census block
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    provider_name = Column(String(255))
    technology = Column(String(50))  # fiber, cable, fixed_wireless, dsl
    max_download_mbps = Column(Integer)
    max_upload_mbps = Column(Integer)
    is_business_service = Column(Boolean)
    fcc_filing_date = Column(Date)
    source = Column(String(50), default="fcc")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_broadband_location", "latitude", "longitude"),)


class InternetExchange(Base):
    """Internet Exchange Points from PeeringDB."""

    __tablename__ = "internet_exchange"

    id = Column(Integer, primary_key=True, autoincrement=True)
    peeringdb_id = Column(Integer, unique=True, index=True)
    name = Column(String(255), nullable=False)
    name_long = Column(String(500))
    city = Column(String(100))
    state = Column(String(50))
    country = Column(String(3))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    website = Column(String(500))
    network_count = Column(Integer)  # Number of connected networks
    ipv4_prefixes = Column(Integer)
    ipv6_prefixes = Column(Integer)
    speed_gbps = Column(Integer)  # Total exchange capacity
    policy_general = Column(String(50))  # open, selective, restrictive
    source = Column(String(50), default="peeringdb")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_ix_location", "latitude", "longitude"),)


class DataCenterFacility(Base):
    """Data center facilities from PeeringDB."""

    __tablename__ = "data_center_facility"

    id = Column(Integer, primary_key=True, autoincrement=True)
    peeringdb_id = Column(Integer, unique=True, index=True)
    name = Column(String(255), nullable=False)
    operator = Column(String(255))
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(50))
    country = Column(String(3))
    postal_code = Column(String(20))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    website = Column(String(500))
    network_count = Column(Integer)  # Networks present
    ix_count = Column(Integer)  # IX connections
    floor_space_sqft = Column(Integer)
    power_mw = Column(Numeric(8, 2))
    pue = Column(Numeric(4, 2))  # Power Usage Effectiveness
    tier_certification = Column(String(20))  # Tier I, II, III, IV
    source = Column(String(50), default="peeringdb")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_dc_facility_location", "latitude", "longitude"),)


class SubmarineCableLanding(Base):
    """Submarine cable landing points."""

    __tablename__ = "submarine_cable_landing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cable_name = Column(String(255), nullable=False)
    landing_point_name = Column(String(255))
    city = Column(String(100))
    state = Column(String(50))
    country = Column(String(3))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    cable_length_km = Column(Integer)
    capacity_tbps = Column(Numeric(10, 2))
    rfs_date = Column(Date)  # Ready for Service
    owners = Column(JSON)  # Array of owner names
    source = Column(String(50), default="telegeography")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_submarine_cable_location", "latitude", "longitude"),)


class NetworkLatency(Base):
    """Network latency measurements."""

    __tablename__ = "network_latency"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_city = Column(String(100))
    source_country = Column(String(3))
    source_latitude = Column(Numeric(10, 7))
    source_longitude = Column(Numeric(10, 7))
    target_city = Column(String(100))
    target_country = Column(String(3))
    target_latitude = Column(Numeric(10, 7))
    target_longitude = Column(Numeric(10, 7))
    measurement_date = Column(Date)
    latency_ms_avg = Column(Numeric(8, 2))
    latency_ms_min = Column(Numeric(8, 2))
    latency_ms_max = Column(Numeric(8, 2))
    latency_ms_p95 = Column(Numeric(8, 2))
    sample_count = Column(Integer)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# DOMAIN 3: TRANSPORTATION INFRASTRUCTURE
# =============================================================================


class IntermodalTerminal(Base):
    """Intermodal terminals (rail/truck) from BTS NTAD."""

    __tablename__ = "intermodal_terminal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ntad_id = Column(String(50), unique=True, index=True)
    name = Column(String(255), nullable=False)
    operator = Column(String(255))
    terminal_type = Column(String(50))  # ramp, port, warehouse
    railroad = Column(String(100))  # BNSF, UP, CSX, NS, etc.
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    annual_lifts = Column(Integer)  # Container lifts per year
    track_miles = Column(Numeric(6, 2))
    parking_spaces = Column(Integer)
    has_on_dock_rail = Column(Boolean)
    source = Column(String(50), default="bts")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_intermodal_location", "latitude", "longitude"),)


class RailLine(Base):
    """Rail network segments from FRA."""

    __tablename__ = "rail_line"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fra_line_id = Column(String(50), unique=True, index=True)
    railroad = Column(String(100))
    track_type = Column(String(50))  # mainline, branch, yard
    track_class = Column(Integer)  # FRA class 1-9
    max_speed_mph = Column(Integer)
    annual_tonnage_million = Column(Numeric(10, 2))
    state = Column(String(2))
    county = Column(String(100))
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50), default="fra")
    collected_at = Column(DateTime, default=datetime.utcnow)


class Port(Base):
    """Ports from USACE."""

    __tablename__ = "port"

    id = Column(Integer, primary_key=True, autoincrement=True)
    port_code = Column(String(10), nullable=False, unique=True, index=True)  # UN/LOCODE
    name = Column(String(255), nullable=False)
    port_type = Column(String(50))  # seaport, river, great_lakes
    city = Column(String(100))
    state = Column(String(2), index=True)
    country = Column(String(3))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    has_container_terminal = Column(Boolean)
    has_bulk_terminal = Column(Boolean)
    has_liquid_terminal = Column(Boolean)
    has_roro_terminal = Column(Boolean)
    channel_depth_ft = Column(Integer)
    source = Column(String(50), default="usace")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_port_location", "latitude", "longitude"),)


class PortThroughput(Base):
    """Port throughput metrics (time series)."""

    __tablename__ = "port_throughput"

    id = Column(Integer, primary_key=True, autoincrement=True)
    port_id = Column(Integer, ForeignKey("port.id"), index=True)
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer)
    teu_import = Column(Integer)
    teu_export = Column(Integer)
    teu_total = Column(Integer)
    tonnage_import_thousand = Column(Numeric(12, 2))
    tonnage_export_thousand = Column(Numeric(12, 2))
    tonnage_total_thousand = Column(Numeric(12, 2))
    vessel_calls = Column(Integer)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "port_id",
            "period_year",
            "period_month",
            "source",
            name="uq_port_throughput",
        ),
    )


class Airport(Base):
    """Airports with cargo facilities from FAA."""

    __tablename__ = "airport"

    id = Column(Integer, primary_key=True, autoincrement=True)
    faa_code = Column(String(10), unique=True, index=True)
    icao_code = Column(String(10))
    name = Column(String(255), nullable=False)
    city = Column(String(100))
    state = Column(String(2), index=True)
    country = Column(String(3))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    airport_type = Column(String(50))  # large_hub, medium_hub, small_hub, cargo
    has_cargo_facility = Column(Boolean)
    longest_runway_ft = Column(Integer)
    cargo_tonnage_annual = Column(Numeric(12, 2))
    source = Column(String(50), default="faa")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_airport_location", "latitude", "longitude"),)


class FreightCorridor(Base):
    """Highway freight corridors from FHWA."""

    __tablename__ = "freight_corridor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    corridor_name = Column(String(255))
    corridor_type = Column(String(50))  # primary, critical_urban, critical_rural
    route_number = Column(String(50))
    state = Column(String(2))
    truck_aadt = Column(Integer)  # Average Annual Daily Truck Traffic
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50), default="fhwa")
    collected_at = Column(DateTime, default=datetime.utcnow)


class HeavyHaulRoute(Base):
    """Heavy haul routes for transformers, generators, etc."""

    __tablename__ = "heavy_haul_route"

    id = Column(Integer, primary_key=True, autoincrement=True)
    route_name = Column(String(255))
    state = Column(String(2), index=True)
    max_weight_lbs = Column(Integer)
    max_height_ft = Column(Numeric(5, 2))
    max_width_ft = Column(Numeric(5, 2))
    max_length_ft = Column(Numeric(5, 2))
    permit_required = Column(Boolean)
    restrictions = Column(Text)
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# DOMAIN 4: LABOR MARKET
# =============================================================================


class LaborMarketArea(Base):
    """Labor market area definitions."""

    __tablename__ = "labor_market_area"

    id = Column(Integer, primary_key=True, autoincrement=True)
    area_type = Column(String(30), nullable=False)  # metro, county, state
    area_code = Column(String(20), nullable=False)  # FIPS or CBSA code
    area_name = Column(String(255))
    state = Column(String(2), index=True)
    population = Column(Integer)
    labor_force = Column(Integer)
    employment = Column(Integer)
    unemployment_rate = Column(Numeric(5, 2))
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("area_type", "area_code", name="uq_labor_market_area"),
        Index("idx_labor_area_type_code", "area_type", "area_code"),
    )


class OccupationalWage(Base):
    """Occupational employment and wages from BLS OES."""

    __tablename__ = "occupational_wage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    area_type = Column(String(30))
    area_code = Column(String(20), index=True)
    area_name = Column(String(255))
    occupation_code = Column(String(20), index=True)  # SOC code
    occupation_title = Column(String(255))
    employment = Column(Integer)
    mean_hourly_wage = Column(Numeric(10, 2))
    median_hourly_wage = Column(Numeric(10, 2))
    pct_10_hourly = Column(Numeric(10, 2))
    pct_25_hourly = Column(Numeric(10, 2))
    pct_75_hourly = Column(Numeric(10, 2))
    pct_90_hourly = Column(Numeric(10, 2))
    mean_annual_wage = Column(Numeric(12, 2))
    median_annual_wage = Column(Numeric(12, 2))
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer)
    source = Column(String(50), default="bls_oes")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "area_code", "occupation_code", "period_year", name="uq_occupational_wage"
        ),
    )


class IndustryEmployment(Base):
    """Industry employment by county from BLS QCEW."""

    __tablename__ = "industry_employment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    area_fips = Column(String(10), index=True)
    area_name = Column(String(255))
    industry_code = Column(String(10), index=True)  # NAICS
    industry_title = Column(String(255))
    ownership = Column(String(30))  # private, federal, state, local
    period_year = Column(Integer, nullable=False)
    period_quarter = Column(Integer)
    establishments = Column(Integer)
    avg_monthly_employment = Column(Integer)
    total_wages_thousand = Column(BigInteger)
    avg_weekly_wage = Column(Numeric(10, 2))
    source = Column(String(50), default="bls_qcew")
    collected_at = Column(DateTime, default=datetime.utcnow)


class CommuteFlow(Base):
    """Commuting patterns from Census LEHD."""

    __tablename__ = "commute_flow"

    id = Column(Integer, primary_key=True, autoincrement=True)
    home_county_fips = Column(String(10), index=True)
    home_county_name = Column(String(255))
    home_state = Column(String(2))
    work_county_fips = Column(String(10), index=True)
    work_county_name = Column(String(255))
    work_state = Column(String(2))
    worker_count = Column(Integer)
    avg_earnings = Column(Numeric(10, 2))
    avg_age = Column(Numeric(4, 1))
    period_year = Column(Integer, nullable=False)
    source = Column(String(50), default="census_lehd")
    collected_at = Column(DateTime, default=datetime.utcnow)


class EducationalAttainment(Base):
    """Educational attainment by area from Census ACS."""

    __tablename__ = "educational_attainment"

    id = Column(Integer, primary_key=True, autoincrement=True)
    area_fips = Column(String(10), index=True)
    area_name = Column(String(255))
    area_type = Column(String(30))
    population_25_plus = Column(Integer)
    pct_high_school = Column(Numeric(5, 2))
    pct_some_college = Column(Numeric(5, 2))
    pct_associates = Column(Numeric(5, 2))
    pct_bachelors = Column(Numeric(5, 2))
    pct_graduate = Column(Numeric(5, 2))
    period_year = Column(Integer, nullable=False)
    source = Column(String(50), default="census_acs")
    collected_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# DOMAIN 5: RISK & ENVIRONMENTAL
# =============================================================================


class FloodZone(Base):
    """Flood zones from FEMA NFHL."""

    __tablename__ = "flood_zone"

    id = Column(Integer, primary_key=True, autoincrement=True)
    zone_code = Column(String(20))  # A, AE, AH, AO, V, VE, X
    zone_description = Column(String(255))
    is_high_risk = Column(Boolean)  # Zone A/V = high risk
    is_coastal = Column(Boolean)
    base_flood_elevation_ft = Column(Numeric(8, 2))
    state = Column(String(2), index=True)
    county = Column(String(100))
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    effective_date = Column(Date)
    source = Column(String(50), default="fema")
    collected_at = Column(DateTime, default=datetime.utcnow)


class SeismicHazard(Base):
    """Seismic hazard data from USGS."""

    __tablename__ = "seismic_hazard"

    id = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(Numeric(10, 7), nullable=False)
    longitude = Column(Numeric(10, 7), nullable=False)
    pga_2pct_50yr = Column(Numeric(8, 4))  # Peak Ground Acceleration (g)
    pga_10pct_50yr = Column(Numeric(8, 4))
    spectral_1sec_2pct = Column(Numeric(8, 4))
    spectral_02sec_2pct = Column(Numeric(8, 4))
    site_class = Column(String(10))  # A, B, C, D, E
    seismic_design_category = Column(String(5))  # A, B, C, D, E, F
    source = Column(String(50), default="usgs")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("latitude", "longitude", name="uq_seismic_location"),
        Index("idx_seismic_location", "latitude", "longitude"),
    )


class FaultLine(Base):
    """Active faults from USGS."""

    __tablename__ = "fault_line"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fault_name = Column(String(255), unique=True)
    fault_type = Column(String(50))  # strike_slip, normal, reverse
    slip_rate_mm_yr = Column(Numeric(8, 2))
    age = Column(String(50))  # historic, holocene, quaternary
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50), default="usgs")
    collected_at = Column(DateTime, default=datetime.utcnow)


class NationalRiskIndex(Base):
    """FEMA National Risk Index by county."""

    __tablename__ = "national_risk_index"

    id = Column(Integer, primary_key=True, autoincrement=True)
    county_fips = Column(String(5), unique=True, nullable=False, index=True)
    county_name = Column(String(100))
    state = Column(String(2), index=True)
    # Overall scores
    risk_score = Column(Numeric(10, 4))
    risk_rating = Column(String(20))  # Very Low, Relatively Low, etc.
    # Individual hazard scores (JSON for flexibility)
    hazard_scores = Column(JSON)  # {earthquake: {score, rating}, flood: {...}, ...}
    # Key individual scores
    earthquake_score = Column(Numeric(10, 4))
    flood_score = Column(Numeric(10, 4))
    tornado_score = Column(Numeric(10, 4))
    hurricane_score = Column(Numeric(10, 4))
    wildfire_score = Column(Numeric(10, 4))
    # Social vulnerability
    social_vulnerability = Column(Numeric(10, 4))
    community_resilience = Column(Numeric(10, 4))
    expected_annual_loss = Column(Numeric(15, 2))  # dollars
    source = Column(String(50), default="fema_nri")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_nri_state", "state"),)


class ClimateData(Base):
    """Climate normals and extremes from NOAA."""

    __tablename__ = "climate_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String(20), index=True)
    station_name = Column(String(255))
    state = Column(String(2), index=True)
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    elevation_ft = Column(Integer)
    # Temperature (Fahrenheit)
    avg_temp_annual = Column(Numeric(5, 1))
    avg_temp_jan = Column(Numeric(5, 1))
    avg_temp_jul = Column(Numeric(5, 1))
    record_high = Column(Numeric(5, 1))
    record_low = Column(Numeric(5, 1))
    days_above_90 = Column(Integer)
    days_below_32 = Column(Integer)
    # Precipitation
    precip_annual_inches = Column(Numeric(6, 2))
    snowfall_annual_inches = Column(Numeric(6, 2))
    # Degree days (for HVAC sizing)
    cooling_degree_days = Column(Integer)
    heating_degree_days = Column(Integer)
    # Extremes
    max_wind_mph = Column(Integer)
    tornado_risk_score = Column(Integer)  # 1-10
    hurricane_risk_score = Column(Integer)  # 1-10
    source = Column(String(50), default="noaa")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_climate_location", "latitude", "longitude"),)


class EnvironmentalFacility(Base):
    """Environmental permits/facilities from EPA."""

    __tablename__ = "environmental_facility"

    id = Column(Integer, primary_key=True, autoincrement=True)
    epa_id = Column(String(50), unique=True, index=True)
    facility_name = Column(String(255))
    facility_type = Column(String(100))
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(2), index=True)
    zip = Column(String(10))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    permits = Column(JSON)  # Array: RCRA, CAA, CWA, etc.
    violations_5yr = Column(Integer)
    is_superfund = Column(Boolean)
    is_brownfield = Column(Boolean)
    source = Column(String(50), default="epa")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_env_facility_location", "latitude", "longitude"),)


class Wetland(Base):
    """Wetlands from NWI."""

    __tablename__ = "wetland"

    id = Column(Integer, primary_key=True, autoincrement=True)
    nwi_code = Column(String(20))
    wetland_type = Column(String(100))
    modifier = Column(String(50))
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    acres = Column(Numeric(12, 2))
    state = Column(String(2), index=True)
    source = Column(String(50), default="usfws")
    collected_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# DOMAIN 6: INCENTIVES & REAL ESTATE
# =============================================================================


class OpportunityZone(Base):
    """Opportunity Zones from CDFI Fund."""

    __tablename__ = "opportunity_zone"

    id = Column(Integer, primary_key=True, autoincrement=True)
    tract_geoid = Column(String(15), unique=True, index=True)  # Census tract FIPS
    state = Column(String(2), index=True)
    county = Column(String(100))
    tract_name = Column(String(255))
    designation_date = Column(Date)
    is_low_income = Column(Boolean)
    is_contiguous = Column(Boolean)
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50), default="cdfi")
    collected_at = Column(DateTime, default=datetime.utcnow)


class ForeignTradeZone(Base):
    """Foreign Trade Zones from FTZ Board."""

    __tablename__ = "foreign_trade_zone"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ftz_number = Column(Integer, unique=True, index=True)
    zone_name = Column(String(255))
    grantee = Column(String(255))
    operator = Column(String(255))
    state = Column(String(2), index=True)
    city = Column(String(100))
    address = Column(String(500))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    acreage = Column(Numeric(10, 2))
    subzones = Column(Integer)
    status = Column(String(30))  # active, pending
    activation_date = Column(Date)
    source = Column(String(50), default="ftzb")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_ftz_location", "latitude", "longitude"),)


class IncentiveProgram(Base):
    """State/local incentive programs."""

    __tablename__ = "incentive_program"

    id = Column(Integer, primary_key=True, autoincrement=True)
    program_name = Column(String(500), nullable=False)
    program_type = Column(String(100))  # tax_credit, grant, abatement, financing
    geography_type = Column(String(30))  # state, county, city
    geography_name = Column(String(255))
    state = Column(String(2), index=True)
    target_industries = Column(JSON)  # Array of industries
    target_investments = Column(JSON)  # manufacturing, data_center, warehouse
    min_investment = Column(BigInteger)
    min_jobs = Column(Integer)
    max_benefit = Column(BigInteger)
    benefit_duration_years = Column(Integer)
    description = Column(Text)
    requirements = Column(Text)
    application_url = Column(String(500))
    source = Column(String(50))
    source_url = Column(String(500))
    collected_at = Column(DateTime, default=datetime.utcnow)


class IncentiveDeal(Base):
    """Disclosed incentive deals from Good Jobs First."""

    __tablename__ = "incentive_deal"

    id = Column(Integer, primary_key=True, autoincrement=True)
    gjf_id = Column(String(100), unique=True, index=True)  # Good Jobs First record ID
    company_name = Column(String(255), index=True)
    parent_company = Column(String(255))
    subsidy_type = Column(String(100))
    subsidy_value = Column(BigInteger)
    year = Column(Integer, index=True)
    state = Column(String(2), index=True)
    city = Column(String(100))
    county = Column(String(100))
    program_name = Column(String(500))
    jobs_announced = Column(Integer)
    jobs_created = Column(Integer)
    investment_announced = Column(BigInteger)
    naics_code = Column(String(10))
    industry = Column(String(255))
    source = Column(String(50), default="goodjobsfirst")
    collected_at = Column(DateTime, default=datetime.utcnow)


class IndustrialSite(Base):
    """Available industrial sites from EDOs."""

    __tablename__ = "industrial_site"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_name = Column(String(255))
    site_type = Column(String(50))  # greenfield, building, spec_building
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    acreage = Column(Numeric(10, 2))
    building_sqft = Column(Integer)
    available_sqft = Column(Integer)
    asking_price = Column(BigInteger)
    asking_price_per_sqft = Column(Numeric(10, 2))
    zoning = Column(String(100))
    utilities_available = Column(JSON)  # electric, gas, water, sewer, fiber
    rail_served = Column(Boolean)
    highway_access = Column(String(255))
    edo_name = Column(String(255))
    contact_email = Column(String(255))
    contact_phone = Column(String(50))
    listing_url = Column(String(500))
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_industrial_site_location", "latitude", "longitude"),)


class ZoningDistrict(Base):
    """Zoning districts."""

    __tablename__ = "zoning_district"

    id = Column(Integer, primary_key=True, autoincrement=True)
    jurisdiction = Column(String(255))
    state = Column(String(2), index=True)
    zone_code = Column(String(50))
    zone_name = Column(String(255))
    zone_category = Column(String(50))  # industrial, commercial, residential, mixed
    allows_manufacturing = Column(Boolean)
    allows_warehouse = Column(Boolean)
    allows_data_center = Column(Boolean)
    max_height_ft = Column(Integer)
    max_far = Column(Numeric(6, 2))  # Floor Area Ratio
    min_lot_sqft = Column(Integer)
    setback_front_ft = Column(Integer)
    setback_side_ft = Column(Integer)
    setback_rear_ft = Column(Integer)
    parking_ratio = Column(String(100))
    # Geometry stored as GeoJSON
    geometry_geojson = Column(JSON)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# DOMAIN 7: FREIGHT & LOGISTICS
# =============================================================================


class FreightRateIndex(Base):
    """Freight rate indices (container, trucking)."""

    __tablename__ = "freight_rate_index"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_name = Column(String(100), nullable=False)
    index_code = Column(String(50), nullable=False, index=True)
    route_origin = Column(String(100))
    route_destination = Column(String(100))
    mode = Column(String(30), nullable=False)  # ocean, trucking, rail, air
    rate_date = Column(Date, nullable=False)
    rate_value = Column(Numeric(12, 2))
    rate_unit = Column(String(30))  # per_feu, per_mile, per_ton
    currency = Column(String(3), default="USD")
    change_pct_wow = Column(Numeric(8, 4))
    change_pct_mom = Column(Numeric(8, 4))
    change_pct_yoy = Column(Numeric(8, 4))
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("index_code", "rate_date", name="uq_freight_rate_index"),
    )


class TruckingLaneRate(Base):
    """Trucking spot rates by lane."""

    __tablename__ = "trucking_lane_rate"

    id = Column(Integer, primary_key=True, autoincrement=True)
    origin_market = Column(String(100), index=True)
    origin_state = Column(String(2))
    destination_market = Column(String(100), index=True)
    destination_state = Column(String(2))
    equipment_type = Column(String(30))  # van, reefer, flatbed
    rate_date = Column(Date, nullable=False)
    rate_per_mile = Column(Numeric(8, 4))
    fuel_surcharge = Column(Numeric(8, 4))
    total_rate_per_mile = Column(Numeric(8, 4))
    load_count = Column(Integer)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)


class WarehouseFacility(Base):
    """Warehouse/3PL facilities."""

    __tablename__ = "warehouse_facility"

    id = Column(Integer, primary_key=True, autoincrement=True)
    facility_name = Column(String(255))
    operator_name = Column(String(255), nullable=False)
    facility_type = Column(
        String(50)
    )  # distribution, fulfillment, cold_storage, cross_dock
    address = Column(String(500))
    city = Column(String(100))
    state = Column(String(2), index=True)
    county = Column(String(100))
    zip = Column(String(10))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    sqft_total = Column(Integer)
    sqft_available = Column(Integer)
    clear_height_ft = Column(Integer)
    dock_doors = Column(Integer)
    drive_in_doors = Column(Integer)
    trailer_parking = Column(Integer)
    has_cold_storage = Column(Boolean)
    has_freezer = Column(Boolean)
    has_hazmat = Column(Boolean)
    has_ftz = Column(Boolean)
    has_rail = Column(Boolean)
    certifications = Column(JSON)
    asking_rent_psf = Column(Numeric(8, 2))
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (Index("idx_warehouse_location", "latitude", "longitude"),)


class ContainerFreightIndex(Base):
    """Container freight rate indices from multiple providers (Freightos, Drewry, SCFI, CCFI)."""

    __tablename__ = "container_freight_index"

    id = Column(Integer, primary_key=True, autoincrement=True)
    index_code = Column(
        String(50), nullable=False, index=True
    )  # FBX01, WCI, SCFI, CCFI
    provider = Column(
        String(50), nullable=False, index=True
    )  # freightos, drewry, scfi, ccfi
    route_origin_region = Column(String(100))  # Asia, Europe, etc.
    route_origin_port = Column(String(100))  # Shanghai, Rotterdam, etc.
    route_destination_region = Column(String(100))
    route_destination_port = Column(String(100))
    container_type = Column(String(20))  # 20ft, 40ft, 40hc, reefer
    rate_value = Column(Numeric(12, 2))  # USD per container
    rate_date = Column(Date, nullable=False)
    change_pct_wow = Column(Numeric(8, 4))  # Week-over-week
    change_pct_mom = Column(Numeric(8, 4))  # Month-over-month
    change_pct_yoy = Column(Numeric(8, 4))  # Year-over-year
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("index_code", "rate_date", name="uq_container_freight_index"),
        Index(
            "idx_container_freight_route",
            "route_origin_region",
            "route_destination_region",
        ),
    )


class UsdaTruckRate(Base):
    """USDA AMS agricultural refrigerated truck rates by lane."""

    __tablename__ = "usda_truck_rate"

    id = Column(Integer, primary_key=True, autoincrement=True)
    origin_region = Column(
        String(100), nullable=False, index=True
    )  # Central Valley, Imperial Valley
    origin_state = Column(String(2))
    destination_city = Column(String(100), nullable=False)
    destination_state = Column(String(2), index=True)
    commodity = Column(String(100), index=True)  # Produce, Vegetables, Citrus
    mileage_band = Column(
        String(30)
    )  # local (<200), short (200-500), medium (500-1000), long (1000+)
    rate_per_mile = Column(Numeric(8, 4))
    rate_per_truckload = Column(Numeric(10, 2))
    fuel_price = Column(Numeric(6, 3))  # Diesel price at time of rate
    report_date = Column(Date, nullable=False)
    source = Column(String(50), default="usda_ams")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "origin_region",
            "destination_city",
            "commodity",
            "report_date",
            name="uq_usda_truck_rate",
        ),
        Index("idx_usda_truck_lane", "origin_state", "destination_state"),
    )


class MotorCarrier(Base):
    """FMCSA motor carrier registry."""

    __tablename__ = "motor_carrier"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dot_number = Column(String(20), unique=True, nullable=False, index=True)
    mc_number = Column(String(20), index=True)
    legal_name = Column(String(500), nullable=False)
    dba_name = Column(String(500))
    physical_address = Column(String(500))
    physical_city = Column(String(100))
    physical_state = Column(String(2), index=True)
    physical_zip = Column(String(10))
    mailing_address = Column(String(500))
    mailing_city = Column(String(100))
    mailing_state = Column(String(2))
    mailing_zip = Column(String(10))
    telephone = Column(String(20))
    email = Column(String(255))
    power_units = Column(Integer)  # Number of trucks
    drivers = Column(Integer)
    mcs150_date = Column(Date)  # Last MCS-150 filing date
    mcs150_mileage = Column(BigInteger)  # Annual mileage from MCS-150
    carrier_operation = Column(
        String(50)
    )  # interstate, intrastate_hazmat, intrastate_non_hazmat
    cargo_carried = Column(JSON)  # List of cargo types
    operation_classification = Column(
        String(50)
    )  # authorized_for_hire, exempt_for_hire, private
    is_active = Column(Boolean, default=True)
    out_of_service_date = Column(Date)
    source = Column(String(50), default="fmcsa")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_motor_carrier_state", "physical_state"),
        Index("idx_motor_carrier_size", "power_units"),
    )


class CarrierSafety(Base):
    """FMCSA SMS safety scores and inspection data."""

    __tablename__ = "carrier_safety"

    id = Column(Integer, primary_key=True, autoincrement=True)
    dot_number = Column(String(20), nullable=False, index=True)
    safety_rating = Column(
        String(30)
    )  # Satisfactory, Conditional, Unsatisfactory, None
    rating_date = Column(Date)
    # BASIC (Behavior Analysis Safety Improvement Categories) scores (0-100, higher = worse)
    unsafe_driving_score = Column(Numeric(6, 2))
    hours_of_service_score = Column(Numeric(6, 2))
    driver_fitness_score = Column(Numeric(6, 2))
    controlled_substances_score = Column(Numeric(6, 2))
    vehicle_maintenance_score = Column(Numeric(6, 2))
    hazmat_compliance_score = Column(Numeric(6, 2))
    crash_indicator_score = Column(Numeric(6, 2))
    # Inspection data
    vehicle_oos_rate = Column(Numeric(6, 2))  # Out of service rate %
    driver_oos_rate = Column(Numeric(6, 2))
    total_inspections = Column(Integer)
    total_violations = Column(Integer)
    total_crashes = Column(Integer)
    fatal_crashes = Column(Integer)
    injury_crashes = Column(Integer)
    tow_crashes = Column(Integer)
    inspection_date = Column(Date)  # Date of last inspection data
    source = Column(String(50), default="fmcsa")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("dot_number", "inspection_date", name="uq_carrier_safety"),
        Index("idx_carrier_safety_rating", "safety_rating"),
    )


class PortThroughputMonthly(Base):
    """Enhanced port throughput metrics time series."""

    __tablename__ = "port_throughput_monthly"

    id = Column(Integer, primary_key=True, autoincrement=True)
    port_code = Column(String(10), nullable=False, index=True)  # UN/LOCODE
    port_name = Column(String(255))
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer, nullable=False)
    # TEU breakdown
    teu_loaded_import = Column(Integer)
    teu_loaded_export = Column(Integer)
    teu_empty_import = Column(Integer)
    teu_empty_export = Column(Integer)
    teu_total = Column(Integer)
    # Vessel data
    container_vessel_calls = Column(Integer)
    avg_berthing_hours = Column(Numeric(8, 2))
    avg_vessel_turnaround_hours = Column(Numeric(8, 2))
    # Tonnage
    tonnage_import = Column(BigInteger)
    tonnage_export = Column(BigInteger)
    tonnage_total = Column(BigInteger)
    # Cargo types
    bulk_tonnage = Column(BigInteger)
    breakbulk_tonnage = Column(BigInteger)
    roro_units = Column(Integer)
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "port_code",
            "period_year",
            "period_month",
            name="uq_port_throughput_monthly",
        ),
        Index("idx_port_throughput_period", "period_year", "period_month"),
    )


class AirCargoStats(Base):
    """Airport cargo statistics from BTS T-100."""

    __tablename__ = "air_cargo_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    airport_code = Column(String(10), nullable=False, index=True)  # FAA code
    airport_name = Column(String(255))
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer, nullable=False)
    # Freight in pounds/tons
    freight_tons_enplaned = Column(Numeric(12, 2))  # Outbound
    freight_tons_deplaned = Column(Numeric(12, 2))  # Inbound
    freight_tons_total = Column(Numeric(12, 2))
    # Domestic vs international
    freight_domestic = Column(Numeric(12, 2))
    freight_international = Column(Numeric(12, 2))
    # Mail
    mail_tons = Column(Numeric(12, 2))
    # Carrier breakdown (JSON: {carrier_code: tons, ...})
    carrier_breakdown = Column(JSON)
    # Aircraft movements
    cargo_aircraft_departures = Column(Integer)
    cargo_aircraft_arrivals = Column(Integer)
    source = Column(String(50), default="bts")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "airport_code", "period_year", "period_month", name="uq_air_cargo_stats"
        ),
        Index("idx_air_cargo_period", "period_year", "period_month"),
    )


class TradeGatewayStats(Base):
    """Import/export statistics by port/customs district."""

    __tablename__ = "trade_gateway_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    customs_district = Column(String(50), index=True)  # e.g., "Los Angeles, CA"
    district_code = Column(String(10))  # USITC district code
    port_code = Column(String(10), index=True)  # UN/LOCODE if applicable
    port_name = Column(String(255))
    period_year = Column(Integer, nullable=False)
    period_month = Column(Integer, nullable=False)
    # Trade values (in millions USD)
    import_value_million = Column(Numeric(14, 2))
    export_value_million = Column(Numeric(14, 2))
    trade_balance_million = Column(Numeric(14, 2))  # export - import
    # Top commodities (JSON arrays)
    top_import_hs_codes = Column(JSON)  # [{hs_code, description, value_million}, ...]
    top_export_hs_codes = Column(JSON)
    # Top trading partners (JSON arrays)
    top_import_countries = Column(JSON)  # [{country, value_million}, ...]
    top_export_countries = Column(JSON)
    # Mode breakdown (percentages)
    vessel_pct = Column(Numeric(5, 2))
    air_pct = Column(Numeric(5, 2))
    truck_pct = Column(Numeric(5, 2))
    rail_pct = Column(Numeric(5, 2))
    other_pct = Column(Numeric(5, 2))
    source = Column(String(50), default="census")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "customs_district",
            "period_year",
            "period_month",
            name="uq_trade_gateway_stats",
        ),
        Index("idx_trade_gateway_period", "period_year", "period_month"),
    )


class ThreePLCompany(Base):
    """3PL company directory and rankings."""

    __tablename__ = "three_pl_company"

    id = Column(Integer, primary_key=True, autoincrement=True)
    company_name = Column(String(255), nullable=False, unique=True, index=True)
    parent_company = Column(String(255))
    headquarters_city = Column(String(100))
    headquarters_state = Column(String(2), index=True)
    headquarters_country = Column(String(3), default="USA")
    website = Column(String(500))
    # Financials
    annual_revenue_million = Column(Numeric(12, 2))
    revenue_year = Column(Integer)
    employee_count = Column(Integer)
    facility_count = Column(Integer)
    # Services offered (JSON array)
    services = Column(
        JSON
    )  # ["warehousing", "transportation", "freight_forwarding", ...]
    # Industries served (JSON array)
    industries_served = Column(JSON)  # ["retail", "manufacturing", "automotive", ...]
    # Geographic coverage
    regions_served = Column(JSON)  # ["North America", "Asia Pacific", ...]
    states_coverage = Column(ARRAY(String(2)))  # States with facilities
    countries_coverage = Column(JSON)
    # Rankings
    armstrong_rank = Column(Integer)  # Armstrong & Associates ranking
    transport_topics_rank = Column(Integer)  # Transport Topics Top 100
    # Specializations
    has_cold_chain = Column(Boolean)
    has_hazmat = Column(Boolean)
    has_ecommerce_fulfillment = Column(Boolean)
    has_cross_dock = Column(Boolean)
    is_asset_based = Column(Boolean)  # Owns trucks/warehouses
    is_non_asset = Column(Boolean)  # Broker model
    source = Column(String(50))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_3pl_revenue", "annual_revenue_million"),
        Index("idx_3pl_rank", "transport_topics_rank"),
    )


class WarehouseListing(Base):
    """Active warehouse/industrial property listings."""

    __tablename__ = "warehouse_listing"

    id = Column(Integer, primary_key=True, autoincrement=True)
    listing_id = Column(String(100), unique=True, nullable=False, index=True)
    source = Column(String(50), nullable=False)  # loopnet, costar, edo
    property_name = Column(String(255))
    listing_type = Column(String(30))  # for_lease, for_sale, for_sublease
    property_type = Column(String(50))  # warehouse, distribution, manufacturing, flex
    address = Column(String(500))
    city = Column(String(100), index=True)
    state = Column(String(2), nullable=False, index=True)
    zip = Column(String(10))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    # Size
    total_sqft = Column(Integer)
    available_sqft = Column(Integer)
    min_divisible_sqft = Column(Integer)
    land_acres = Column(Numeric(10, 2))
    # Building specs
    clear_height_ft = Column(Integer)
    dock_doors = Column(Integer)
    drive_in_doors = Column(Integer)
    column_spacing = Column(String(50))  # e.g., "50x50"
    floor_load_capacity = Column(String(50))  # e.g., "4000 psf"
    year_built = Column(Integer)
    # Features
    has_rail_spur = Column(Boolean)
    has_cold_storage = Column(Boolean)
    has_freezer = Column(Boolean)
    has_sprinkler = Column(Boolean)
    has_fenced_yard = Column(Boolean)
    trailer_parking_spaces = Column(Integer)
    # Pricing
    asking_rent_psf = Column(Numeric(8, 2))  # Per sq ft per year
    asking_rent_nnn = Column(Boolean)  # Triple net lease
    asking_price = Column(BigInteger)  # For sale price
    # Listing info
    listing_date = Column(Date)
    broker_name = Column(String(255))
    broker_company = Column(String(255))
    broker_phone = Column(String(50))
    listing_url = Column(String(500))
    is_active = Column(Boolean, default=True)
    collected_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_warehouse_listing_location", "latitude", "longitude"),
        Index("idx_warehouse_listing_size", "total_sqft"),
        Index("idx_warehouse_listing_state", "state", "city"),
    )


# =============================================================================
# DOMAIN 8: WATER & UTILITIES
# =============================================================================


class WaterMonitoringSite(Base):
    """USGS water monitoring stations with real-time data."""

    __tablename__ = "water_monitoring_site"

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_number = Column(String(20), unique=True, nullable=False, index=True)
    site_name = Column(String(500), nullable=False)
    site_type = Column(String(50), index=True)  # stream, well, spring, lake, estuary
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    drainage_area_sq_mi = Column(Numeric(12, 2))
    aquifer_code = Column(String(50))
    aquifer_name = Column(String(255))
    well_depth_ft = Column(Numeric(10, 2))
    # Latest readings
    latest_streamflow_cfs = Column(Numeric(12, 2))  # Cubic feet per second
    latest_gage_height_ft = Column(Numeric(10, 2))
    latest_water_temp_c = Column(Numeric(6, 2))
    latest_dissolved_oxygen = Column(Numeric(6, 2))  # mg/L
    latest_ph = Column(Numeric(4, 2))
    latest_turbidity = Column(Numeric(8, 2))  # NTU
    measurement_date = Column(DateTime)
    # Data availability
    has_streamflow = Column(Boolean, default=False)
    has_groundwater = Column(Boolean, default=False)
    has_quality = Column(Boolean, default=False)
    source = Column(String(50), default="usgs")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_water_monitoring_location", "latitude", "longitude"),
        Index("idx_water_monitoring_type", "site_type"),
    )


class PublicWaterSystem(Base):
    """EPA SDWIS public water infrastructure."""

    __tablename__ = "public_water_system"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pwsid = Column(
        String(15), unique=True, nullable=False, index=True
    )  # e.g., "CA1234567"
    pws_name = Column(String(500), nullable=False)
    pws_type = Column(String(30), index=True)  # CWS (community), TNCWS, NTNCWS
    state = Column(String(2), index=True)
    county = Column(String(100))
    city = Column(String(100))
    zip_code = Column(String(10))
    # Service area
    population_served = Column(Integer, index=True)
    service_connections = Column(Integer)
    service_area_type = Column(String(50))  # residential, commercial, industrial, mixed
    # Water source
    primary_source_code = Column(
        String(10)
    )  # GW (groundwater), SW (surface), GU (purchased ground), SW (purchased surface)
    primary_source_name = Column(String(255))
    source_water_protection = Column(Boolean)
    # Infrastructure
    treatment_plant_count = Column(Integer)
    storage_capacity_mg = Column(Numeric(12, 2))  # Million gallons
    distribution_miles = Column(Numeric(10, 2))
    # Status
    is_active = Column(Boolean, default=True)
    compliance_status = Column(String(50))  # compliant, non_compliant, pending
    last_compliance_date = Column(Date)
    # Contact
    admin_contact_name = Column(String(255))
    admin_contact_phone = Column(String(20))
    source = Column(String(50), default="epa_sdwis")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_pws_population", "population_served"),
        Index("idx_pws_state_city", "state", "city"),
    )


class WaterSystemViolation(Base):
    """EPA water quality violations."""

    __tablename__ = "water_system_violation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pwsid = Column(String(15), nullable=False, index=True)
    violation_id = Column(String(50), unique=True, nullable=False, index=True)
    violation_type = Column(
        String(50), index=True
    )  # MCL, MRDL, TT, monitoring, reporting
    contaminant_code = Column(String(10))
    contaminant_name = Column(String(255))
    contaminant_group = Column(
        String(100)
    )  # disinfectants, organics, inorganics, microorganisms
    violation_date = Column(Date, nullable=False)
    compliance_period = Column(String(20))  # e.g., "2024-Q1"
    is_health_based = Column(Boolean, default=False)
    severity_level = Column(String(20))  # tier1, tier2, tier3
    enforcement_action = Column(String(100))
    enforcement_date = Column(Date)
    returned_to_compliance = Column(Boolean, default=False)
    returned_to_compliance_date = Column(Date)
    public_notification_date = Column(Date)
    source = Column(String(50), default="epa_sdwis")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_violation_date", "violation_date"),
        Index("idx_violation_type", "violation_type"),
        Index("idx_violation_pwsid", "pwsid"),
    )


class NaturalGasPipeline(Base):
    """EIA interstate/intrastate natural gas pipelines."""

    __tablename__ = "natural_gas_pipeline"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(String(50), unique=True, nullable=False, index=True)
    pipeline_name = Column(String(255), nullable=False)
    operator_name = Column(String(255))
    operator_id = Column(String(20))
    # Route
    origin_state = Column(String(2))
    origin_location = Column(String(255))
    destination_state = Column(String(2))
    destination_location = Column(String(255))
    states_crossed = Column(ARRAY(String(2)))
    # Capacity
    capacity_mmcfd = Column(Numeric(12, 2))  # Million cubic feet per day
    diameter_inches = Column(Numeric(6, 2))
    length_miles = Column(Numeric(10, 2))
    # Type
    pipeline_type = Column(String(30), index=True)  # interstate, intrastate, gathering
    is_bidirectional = Column(Boolean, default=False)
    commodity = Column(String(50), default="natural_gas")  # natural_gas, ngl, propane
    # Location for mapping
    latitude = Column(Numeric(10, 7))  # Midpoint or start
    longitude = Column(Numeric(10, 7))
    geometry_geojson = Column(JSON)
    # Status
    status = Column(String(30))  # operational, planned, under_construction
    in_service_date = Column(Date)
    source = Column(String(50), default="eia")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_gas_pipeline_location", "latitude", "longitude"),
        Index("idx_gas_pipeline_type", "pipeline_type"),
    )


class NaturalGasStorage(Base):
    """EIA underground natural gas storage facilities."""

    __tablename__ = "natural_gas_storage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    facility_id = Column(String(50), unique=True, nullable=False, index=True)
    facility_name = Column(String(255), nullable=False)
    operator_name = Column(String(255))
    operator_id = Column(String(20))
    # Location
    state = Column(String(2), index=True)
    county = Column(String(100))
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    # Storage type
    storage_type = Column(
        String(30), index=True
    )  # depleted_field, salt_cavern, aquifer
    field_name = Column(String(255))
    reservoir_depth_ft = Column(Integer)
    # Capacity
    base_gas_bcf = Column(Numeric(12, 4))  # Billion cubic feet (cushion gas)
    working_gas_bcf = Column(Numeric(12, 4))  # Available for withdrawal
    total_capacity_bcf = Column(Numeric(12, 4))
    deliverability_mmcfd = Column(Numeric(12, 2))  # Max withdrawal rate
    injection_capacity_mmcfd = Column(Numeric(12, 2))
    # Current status
    current_inventory_bcf = Column(Numeric(12, 4))
    inventory_date = Column(Date)
    utilization_pct = Column(Numeric(5, 2))
    # Status
    status = Column(String(30))  # operational, planned, inactive
    in_service_year = Column(Integer)
    source = Column(String(50), default="eia")
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_gas_storage_location", "latitude", "longitude"),
        Index("idx_gas_storage_type", "storage_type"),
    )


class UtilityRate(Base):
    """OpenEI/EIA utility electricity rates."""

    __tablename__ = "utility_rate"

    id = Column(Integer, primary_key=True, autoincrement=True)
    utility_id = Column(String(50), nullable=False, index=True)
    utility_name = Column(String(255), nullable=False)
    state = Column(String(2), index=True)
    service_territory = Column(String(255))
    # Rate details
    rate_schedule_id = Column(String(100), unique=True, nullable=False, index=True)
    rate_schedule_name = Column(String(500))
    customer_class = Column(
        String(30), index=True
    )  # residential, commercial, industrial
    sector = Column(String(30))  # general, lighting, agricultural, etc.
    # Pricing structure
    energy_rate_kwh = Column(Numeric(10, 6))  # Base $/kWh rate
    demand_charge_kw = Column(Numeric(10, 4))  # $/kW demand charge
    fixed_monthly_charge = Column(Numeric(10, 2))  # Fixed monthly fee
    minimum_charge = Column(Numeric(10, 2))
    # Rate tiers (JSON for flexibility)
    energy_tiers = Column(JSON)  # [{kwh_limit, rate}, ...]
    demand_tiers = Column(JSON)  # [{kw_limit, rate}, ...]
    # Time of use
    has_time_of_use = Column(Boolean, default=False)
    tou_periods = Column(JSON)  # {peak: {start, end, rate}, off_peak: {...}}
    has_demand_charges = Column(Boolean, default=False)
    has_net_metering = Column(Boolean, default=False)
    # Power factor
    power_factor_adjustment = Column(Boolean, default=False)
    min_power_factor = Column(Numeric(4, 2))
    # Dates
    effective_date = Column(Date)
    end_date = Column(Date)
    approved_date = Column(Date)
    # Metadata
    description = Column(Text)
    source = Column(String(50))  # openei, eia, utility_website
    source_url = Column(String(500))
    collected_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_utility_rate_class", "customer_class"),
        Index("idx_utility_rate_state", "state"),
    )


# =============================================================================
# DOMAIN 9: SITE SCORING
# =============================================================================


class SiteScoreConfig(Base):
    """Site scoring configuration."""

    __tablename__ = "site_score_config"

    id = Column(Integer, primary_key=True, autoincrement=True)
    config_name = Column(String(100), nullable=False)
    use_case = Column(
        String(50), nullable=False
    )  # data_center, warehouse, manufacturing
    factor_weights = Column(JSON, nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SiteScore(Base):
    """Cached site scores."""

    __tablename__ = "site_score"

    id = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(Numeric(10, 7), nullable=False)
    longitude = Column(Numeric(10, 7), nullable=False)
    config_id = Column(Integer, ForeignKey("site_score_config.id"), index=True)
    overall_score = Column(Numeric(5, 2))  # 0-100
    factor_scores = Column(JSON)  # Individual factor scores
    computed_at = Column(DateTime, default=datetime.utcnow)
    valid_until = Column(DateTime)

    __table_args__ = (
        UniqueConstraint("latitude", "longitude", "config_id", name="uq_site_score"),
        Index("idx_site_score_location", "latitude", "longitude"),
    )


# =============================================================================
# COLLECTION JOB TRACKING
# =============================================================================


class SiteIntelCollectionJob(Base):
    """Site intelligence collection job tracking."""

    __tablename__ = "site_intel_collection_job"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(50), nullable=False)  # power, telecom, transport, etc.
    source = Column(String(50), nullable=False)  # eia, fcc, bts, etc.
    job_type = Column(String(50), nullable=False)  # full_sync, incremental, single_item
    status = Column(String(20), default="pending")  # pending, running, success, failed
    config = Column(JSON)

    # Progress tracking
    total_items = Column(Integer, default=0)
    processed_items = Column(Integer, default=0)
    inserted_items = Column(Integer, default=0)
    updated_items = Column(Integer, default=0)
    failed_items = Column(Integer, default=0)

    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_site_intel_job_domain", "domain"),
        Index("idx_site_intel_job_status", "status"),
        Index("idx_site_intel_job_created", "created_at"),
    )


class CollectionWatermark(Base):
    """
    Tracks last-collected timestamp per domain/source/state for incremental collection.

    NULL watermark = full sync (existing behavior). When a watermark exists,
    collectors can use it as a date filter for incremental updates.
    """

    __tablename__ = "collection_watermarks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(String(50), nullable=False, index=True)
    source = Column(String(50), nullable=False, index=True)
    state = Column(String(10), nullable=True)  # NULL = national
    last_collected_at = Column(DateTime, nullable=False)
    last_job_id = Column(Integer, nullable=True)
    records_collected = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "domain", "source", "state", name="uq_watermark_domain_source_state"
        ),
        Index("idx_watermark_domain_source", "domain", "source"),
    )
