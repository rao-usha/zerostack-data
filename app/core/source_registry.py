"""
Centralized source context registry.

Business-level metadata for every data source: display name, category,
description, update frequency, tags, and table prefix.  Consumed by the
``/api/v1/sources`` endpoints to give users a discoverable directory of
all available data.

Static context lives here; dynamic status (row counts, last-run info) is
computed at request time by the router.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from app.core.api_registry import API_REGISTRY, APIKeyRequirement
from app.core.domains import DOMAIN_LABELS


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SourceCollection:
    """One triggerable ingestion within a source."""

    name: str        # "Petroleum"
    endpoint: str    # "POST /eia/petroleum/ingest"
    description: str # "Weekly/monthly supply, demand, stocks, imports/exports"
    table: str       # "eia_petroleum_*"


@dataclass(frozen=True)
class SourceContext:
    """Immutable metadata for a single data source."""

    key: str
    display_name: str
    short_name: str
    category: str  # domain key from domains.py
    description: str
    update_frequency: str  # daily | weekly | monthly | quarterly | annual | varies
    api_key_required: bool
    url: str
    table_prefix: str
    tags: List[str] = field(default_factory=list)
    collections: List[SourceCollection] = field(default_factory=list)

    @property
    def category_label(self) -> str:
        return DOMAIN_LABELS.get(self.category, "Platform")


# ---------------------------------------------------------------------------
# Helper to derive api_key_required from api_registry.py
# ---------------------------------------------------------------------------

def _key_required(source: str) -> bool:
    """Return True if the source's API key is REQUIRED in the API registry."""
    cfg = API_REGISTRY.get(source)
    if cfg is None:
        return False
    return cfg.api_key_requirement == APIKeyRequirement.REQUIRED


# ---------------------------------------------------------------------------
# SOURCE REGISTRY — 36 data sources
# ---------------------------------------------------------------------------

SOURCE_REGISTRY: Dict[str, SourceContext] = {
    # ── Macro Economic (5) ────────────────────────────────────────────────
    "fred": SourceContext(
        key="fred",
        display_name="Federal Reserve Economic Data (FRED)",
        short_name="FRED",
        category="macro_economic",
        description="800,000+ economic time series from the St. Louis Fed — GDP, employment, inflation, interest rates, exchange rates, and more.",
        update_frequency="daily",
        api_key_required=_key_required("fred"),
        url="https://fred.stlouisfed.org",
        table_prefix="fred_",
        tags=["economics", "time-series", "monetary-policy", "rates"],
        collections=[
            SourceCollection(
                name="Interest Rates",
                endpoint="POST /fred/ingest category=interest_rates",
                description="Fed Funds Rate, 10Y/30Y/2Y/5Y/3M Treasury yields, Bank Prime Rate. 7 daily series.",
                table="fred_interest_rates",
            ),
            SourceCollection(
                name="Monetary Aggregates",
                endpoint="POST /fred/ingest category=monetary_aggregates",
                description="M1/M2 money stock, monetary base, currency in circulation. 4 series.",
                table="fred_monetary_aggregates",
            ),
            SourceCollection(
                name="Industrial Production",
                endpoint="POST /fred/ingest category=industrial_production",
                description="Total index, manufacturing, mining, utilities, capacity utilization. 5 monthly series.",
                table="fred_industrial_production",
            ),
            SourceCollection(
                name="Economic Indicators",
                endpoint="POST /fred/ingest category=economic_indicators",
                description="GDP, Real GDP, unemployment rate, CPI, PCE, retail sales. 6 series, mixed frequency.",
                table="fred_economic_indicators",
            ),
        ],
    ),
    "bea": SourceContext(
        key="bea",
        display_name="Bureau of Economic Analysis (BEA)",
        short_name="BEA",
        category="macro_economic",
        description="GDP, personal income, PCE, regional economic accounts, and international transactions from the U.S. Commerce Department.",
        update_frequency="quarterly",
        api_key_required=_key_required("bea"),
        url="https://www.bea.gov",
        table_prefix="bea_",
        tags=["gdp", "income", "regional-economics", "trade-balance"],
        collections=[
            SourceCollection(
                name="NIPA (GDP & Income)",
                endpoint="POST /bea/nipa/ingest",
                description="National Income and Product Accounts — GDP, personal income, PCE, investment, government spending.",
                table="bea_nipa_*",
            ),
            SourceCollection(
                name="Regional Accounts",
                endpoint="POST /bea/regional/ingest",
                description="State, county, and metro area GDP, personal income, and per capita income.",
                table="bea_regional_*",
            ),
            SourceCollection(
                name="GDP by Industry",
                endpoint="POST /bea/gdp-industry/ingest",
                description="Value added, gross output, and real value added by NAICS industry code.",
                table="bea_gdp_industry_*",
            ),
            SourceCollection(
                name="International Transactions",
                endpoint="POST /bea/international/ingest",
                description="U.S. trade balance, exports, imports, and foreign direct investment by country.",
                table="bea_international_*",
            ),
        ],
    ),
    "bls": SourceContext(
        key="bls",
        display_name="Bureau of Labor Statistics (BLS)",
        short_name="BLS",
        category="macro_economic",
        description="Employment (CES/CPS), CPI, PPI, JOLTS job openings, OES occupational wages, and productivity data.",
        update_frequency="monthly",
        api_key_required=_key_required("bls"),
        url="https://www.bls.gov",
        table_prefix="bls_",
        tags=["employment", "cpi", "wages", "labor-market"],
        collections=[
            SourceCollection(
                name="CES Employment",
                endpoint="POST /bls/ces/ingest",
                description="Nonfarm payrolls, private employment, manufacturing, construction, avg hourly earnings. 11 monthly series.",
                table="bls_ces_employment",
            ),
            SourceCollection(
                name="CPS Labor Force",
                endpoint="POST /bls/cps/ingest",
                description="Unemployment rate, labor force participation, employment-population ratio, U-6. 7 monthly series.",
                table="bls_cps_labor_force",
            ),
            SourceCollection(
                name="JOLTS",
                endpoint="POST /bls/jolts/ingest",
                description="Job openings, hires, quits, layoffs — levels and rates. 10 monthly series.",
                table="bls_jolts",
            ),
            SourceCollection(
                name="CPI",
                endpoint="POST /bls/cpi/ingest",
                description="All items, core (ex food & energy), food, energy, shelter, medical, transport. 13 monthly series.",
                table="bls_cpi",
            ),
            SourceCollection(
                name="PPI",
                endpoint="POST /bls/ppi/ingest",
                description="Final demand, goods, services, intermediate demand, crude materials, manufacturing. 8 monthly series.",
                table="bls_ppi",
            ),
            SourceCollection(
                name="OES Wages",
                endpoint="POST /bls/oes/ingest",
                description="Occupational employment and wage estimates for healthcare roles. 16 annual series.",
                table="bls_oes",
            ),
        ],
    ),
    "treasury": SourceContext(
        key="treasury",
        display_name="U.S. Treasury FiscalData",
        short_name="Treasury",
        category="macro_economic",
        description="Daily Treasury balance, federal debt outstanding, interest rates, auction results, and revenue/spending statements.",
        update_frequency="daily",
        api_key_required=_key_required("treasury"),
        url="https://fiscaldata.treasury.gov",
        table_prefix="treasury_",
        tags=["debt", "interest-rates", "auctions", "fiscal-policy"],
        collections=[
            SourceCollection(
                name="Federal Debt",
                endpoint="POST /treasury/debt/ingest",
                description="Total public debt outstanding, debt held by public, intragovernmental holdings.",
                table="treasury_debt",
            ),
            SourceCollection(
                name="Interest Rates",
                endpoint="POST /treasury/interest-rates/ingest",
                description="Treasury yield curve — Bills, Notes, Bonds, TIPS, and FRN rates.",
                table="treasury_interest_rates",
            ),
            SourceCollection(
                name="Revenue & Spending",
                endpoint="POST /treasury/revenue-spending/ingest",
                description="Monthly Treasury Statement — receipts, outlays, and budget surplus/deficit.",
                table="treasury_monthly_statement",
            ),
            SourceCollection(
                name="Auction Results",
                endpoint="POST /treasury/auctions/ingest",
                description="Bond, Note, and Bill auction results — yields, bid-to-cover ratios, allotments.",
                table="treasury_auctions",
            ),
        ],
    ),
    "international_econ": SourceContext(
        key="international_econ",
        display_name="International Economic Data",
        short_name="Intl Econ",
        category="macro_economic",
        description="Global macro indicators from the World Bank WDI, IMF IFS, OECD, and BIS — GDP, trade, FX, and development metrics.",
        update_frequency="quarterly",
        api_key_required=False,
        url="https://data.worldbank.org",
        table_prefix="intl_",
        tags=["global", "world-bank", "imf", "oecd", "development"],
        collections=[
            SourceCollection(
                name="World Bank WDI",
                endpoint="POST /international/worldbank/wdi/ingest",
                description="World Development Indicators — 1,600+ indicators for 200+ countries (GDP, poverty, health, education).",
                table="intl_wdi_*",
            ),
            SourceCollection(
                name="World Bank Countries",
                endpoint="POST /international/worldbank/countries/ingest",
                description="Country metadata — ISO codes, regions, income levels, lending types.",
                table="intl_worldbank_countries",
            ),
            SourceCollection(
                name="World Bank Indicators",
                endpoint="POST /international/worldbank/indicators/ingest",
                description="Indicator metadata — codes, names, descriptions, source organizations.",
                table="intl_worldbank_indicators",
            ),
            SourceCollection(
                name="IMF IFS",
                endpoint="POST /international/imf/ifs/ingest",
                description="International Financial Statistics — exchange rates, interest rates, government finance for 190+ countries.",
                table="intl_imf_*",
            ),
            SourceCollection(
                name="OECD MEI",
                endpoint="POST /international/oecd/mei/ingest",
                description="Main Economic Indicators — industrial production, inflation, unemployment for OECD members.",
                table="intl_oecd_mei",
            ),
            SourceCollection(
                name="OECD KEI",
                endpoint="POST /international/oecd/kei/ingest",
                description="Key Economic Indicators — industrial production, CPI, unemployment, retail trade.",
                table="intl_oecd_kei",
            ),
            SourceCollection(
                name="OECD Labour",
                endpoint="POST /international/oecd/labor/ingest",
                description="Annual Labour Force Statistics — employment, unemployment, participation rates.",
                table="intl_oecd_labor",
            ),
            SourceCollection(
                name="OECD Trade in Services",
                endpoint="POST /international/oecd/trade/ingest",
                description="Balanced Trade in Services — exports/imports by category for OECD countries.",
                table="intl_oecd_trade",
            ),
            SourceCollection(
                name="OECD Tax Revenue",
                endpoint="POST /international/oecd/tax/ingest",
                description="Tax Revenue Statistics — total tax revenue by type and country.",
                table="intl_oecd_tax",
            ),
            SourceCollection(
                name="BIS Exchange Rates",
                endpoint="POST /international/bis/eer/ingest",
                description="Effective Exchange Rates — nominal and real trade-weighted rates for 60+ economies.",
                table="intl_bis_eer",
            ),
            SourceCollection(
                name="BIS Property Prices",
                endpoint="POST /international/bis/property/ingest",
                description="Residential property price indices for 60+ countries.",
                table="intl_bis_property",
            ),
        ],
    ),

    # ── Trade & Commerce (6) ──────────────────────────────────────────────
    "census": SourceContext(
        key="census",
        display_name="U.S. Census Bureau (ACS 5-Year)",
        short_name="Census",
        category="trade_commerce",
        description="American Community Survey 5-year estimates — demographics, income, education, housing, and commute data by geography.",
        update_frequency="annual",
        api_key_required=_key_required("census"),
        url="https://data.census.gov",
        table_prefix="acs5_",
        tags=["demographics", "income", "housing", "geography"],
        collections=[
            SourceCollection(
                name="State Level",
                endpoint="POST /census/state",
                description="ACS 5-year estimates at state level. Demographics, income, education, housing.",
                table="acs5_{year}_{table_id}",
            ),
            SourceCollection(
                name="County Level",
                endpoint="POST /census/county",
                description="Same data at county level. Optional state FIPS filter.",
                table="acs5_{year}_{table_id}",
            ),
            SourceCollection(
                name="Tract Level",
                endpoint="POST /census/tract",
                description="Census tract level (requires state FIPS). Finest non-block geography.",
                table="acs5_{year}_{table_id}",
            ),
            SourceCollection(
                name="ZIP (ZCTA) Level",
                endpoint="POST /census/zip",
                description="ZIP Code Tabulation Area level. Optional state filter.",
                table="acs5_{year}_{table_id}",
            ),
        ],
    ),
    "us_trade": SourceContext(
        key="us_trade",
        display_name="U.S. International Trade",
        short_name="US Trade",
        category="trade_commerce",
        description="Census Bureau international trade data — imports and exports by HS code, country, district, and port.",
        update_frequency="monthly",
        api_key_required=False,
        url="https://usatrade.census.gov",
        table_prefix="us_trade_",
        tags=["imports", "exports", "hs-codes", "trade-balance"],
        collections=[
            SourceCollection(
                name="Exports by HS Code",
                endpoint="POST /us-trade/exports/hs/ingest",
                description="U.S. exports by Harmonized System code with commodity and country breakdown.",
                table="us_trade_exports_hs",
            ),
            SourceCollection(
                name="Imports by HS Code",
                endpoint="POST /us-trade/imports/hs/ingest",
                description="U.S. imports by HS code — general, consumption, duty-free/dutiable breakdown.",
                table="us_trade_imports_hs",
            ),
            SourceCollection(
                name="State Exports",
                endpoint="POST /us-trade/exports/state/ingest",
                description="State-level export data by commodity and destination country.",
                table="us_trade_state_exports",
            ),
            SourceCollection(
                name="Port/District Trade",
                endpoint="POST /us-trade/port/ingest",
                description="Trade data by customs district (port of entry) with commodity and country breakdown.",
                table="us_trade_port",
            ),
            SourceCollection(
                name="Trade Summary",
                endpoint="POST /us-trade/summary/ingest",
                description="Aggregated trade summary by country — exports, imports, and trade balance.",
                table="us_trade_summary",
            ),
        ],
    ),
    "cftc_cot": SourceContext(
        key="cftc_cot",
        display_name="CFTC Commitments of Traders",
        short_name="CFTC COT",
        category="trade_commerce",
        description="Weekly futures and options positioning — commercial hedgers, managed money, swap dealers, and non-reportable traders.",
        update_frequency="weekly",
        api_key_required=_key_required("cftc_cot"),
        url="https://www.cftc.gov/MarketReports/CommitmentsofTraders",
        table_prefix="cftc_cot_",
        tags=["futures", "positioning", "commodities", "derivatives"],
        collections=[
            SourceCollection(
                name="COT Reports",
                endpoint="POST /cftc-cot/ingest",
                description="Legacy, Disaggregated, and Traders in Financial Futures (TFF) reports. Configurable report_type parameter.",
                table="cftc_cot_*",
            ),
        ],
    ),
    "irs_soi": SourceContext(
        key="irs_soi",
        display_name="IRS Statistics of Income (SOI)",
        short_name="IRS SOI",
        category="trade_commerce",
        description="ZIP-level income distribution, migration flows, business returns, and wealth data from IRS tax filings.",
        update_frequency="annual",
        api_key_required=_key_required("irs_soi"),
        url="https://www.irs.gov/statistics/soi-tax-stats",
        table_prefix="irs_soi_",
        tags=["income", "migration", "tax", "wealth", "zip-code"],
        collections=[
            SourceCollection(
                name="ZIP Income",
                endpoint="POST /irs-soi/zip-income/ingest",
                description="Individual income by ZIP code — AGI brackets, wages, dividends, capital gains, deductions.",
                table="irs_soi_zip_income",
            ),
            SourceCollection(
                name="County Income",
                endpoint="POST /irs-soi/county-income/ingest",
                description="Individual income by county with FIPS codes — same variables as ZIP but at county level.",
                table="irs_soi_county_income",
            ),
            SourceCollection(
                name="Migration Flows",
                endpoint="POST /irs-soi/migration/ingest",
                description="County-to-county migration — inflow/outflow of returns, exemptions, and adjusted gross income.",
                table="irs_soi_migration",
            ),
            SourceCollection(
                name="Business Income",
                endpoint="POST /irs-soi/business-income/ingest",
                description="Business income by ZIP — Schedule C, partnerships, S-corps, rental income, farm income.",
                table="irs_soi_business_income",
            ),
        ],
    ),
    "bts": SourceContext(
        key="bts",
        display_name="Bureau of Transportation Statistics",
        short_name="BTS",
        category="trade_commerce",
        description="Border crossing data, FAF5 freight flows, vehicle miles traveled, and transportation infrastructure metrics.",
        update_frequency="monthly",
        api_key_required=_key_required("bts"),
        url="https://www.bts.gov",
        table_prefix="bts_",
        tags=["freight", "border-crossings", "transportation", "logistics"],
        collections=[
            SourceCollection(
                name="Border Crossings",
                endpoint="POST /bts/border-crossing/ingest",
                description="Monthly port-of-entry stats: trucks, containers, trains, buses, personal vehicles, pedestrians. 327+ ports.",
                table="bts_border_crossing",
            ),
            SourceCollection(
                name="Vehicle Miles Traveled",
                endpoint="POST /bts/vmt/ingest",
                description="Monthly state-level VMT by road type. Economic activity proxy.",
                table="bts_vmt",
            ),
            SourceCollection(
                name="Freight Analysis (FAF5)",
                endpoint="POST /bts/faf/ingest",
                description="Freight tonnage, value, ton-miles by origin-destination zone, commodity (43 types), and transport mode. Bulk CSV ~100MB.",
                table="bts_faf_regional",
            ),
        ],
    ),
    "dunl": SourceContext(
        key="dunl",
        display_name="DUNL (S&P Data Unlocked)",
        short_name="DUNL",
        category="trade_commerce",
        description="S&P Global open reference data — standardized currencies, ports, units of measure, and business calendars.",
        update_frequency="monthly",
        api_key_required=_key_required("dunl"),
        url="https://dunl.org",
        table_prefix="dunl_",
        tags=["reference-data", "currencies", "ports", "standards"],
        collections=[
            SourceCollection(
                name="Currencies",
                endpoint="POST /dunl/currencies/ingest",
                description="ISO 4217 currency definitions — ~208 records with codes, names, and symbols.",
                table="dunl_currencies",
            ),
            SourceCollection(
                name="Ports",
                endpoint="POST /dunl/ports/ingest",
                description="Global port definitions — ~301 records with UN/LOCODE, country, and coordinates.",
                table="dunl_ports",
            ),
            SourceCollection(
                name="Units of Measure",
                endpoint="POST /dunl/uom/ingest",
                description="Units of measure (~210) and conversion factors (~635) for commodities and trade.",
                table="dunl_uom, dunl_uom_conversions",
            ),
            SourceCollection(
                name="Holiday Calendars",
                endpoint="POST /dunl/calendars/ingest",
                description="Business holiday calendars by country and year for settlement date calculations.",
                table="dunl_calendars",
            ),
        ],
    ),

    # ── Energy & Agriculture (3) ──────────────────────────────────────────
    "eia": SourceContext(
        key="eia",
        display_name="Energy Information Administration (EIA)",
        short_name="EIA",
        category="energy_agriculture",
        description="U.S. energy data — petroleum supply/demand, electricity rates, natural gas prices, and renewable generation.",
        update_frequency="weekly",
        api_key_required=_key_required("eia"),
        url="https://www.eia.gov",
        table_prefix="eia_",
        tags=["energy", "petroleum", "electricity", "natural-gas", "renewables"],
        collections=[
            SourceCollection(
                name="Petroleum",
                endpoint="POST /eia/petroleum/ingest",
                description="Consumption, production, imports, exports, stocks. Filterable by product/process/area. Weekly/monthly/annual.",
                table="eia_petroleum_*",
            ),
            SourceCollection(
                name="Natural Gas",
                endpoint="POST /eia/natural-gas/ingest",
                description="Consumption, production, storage, prices. Filterable by area/product.",
                table="eia_natural_gas_*",
            ),
            SourceCollection(
                name="Electricity",
                endpoint="POST /eia/electricity/ingest",
                description="Retail sales, generation, revenue, customer counts by state and sector.",
                table="eia_electricity_*",
            ),
            SourceCollection(
                name="Retail Gas Prices",
                endpoint="POST /eia/retail-gas-prices/ingest",
                description="Regular, midgrade, premium, diesel prices by region. Weekly/daily.",
                table="eia_retail_gas_prices",
            ),
            SourceCollection(
                name="Short-Term Energy Outlook",
                endpoint="POST /eia/steo/ingest",
                description="Monthly projections for energy supply, demand, and prices.",
                table="eia_steo",
            ),
        ],
    ),
    "usda": SourceContext(
        key="usda",
        display_name="USDA NASS QuickStats",
        short_name="USDA",
        category="energy_agriculture",
        description="Crop production, yields, livestock inventory, and agricultural prices from the National Agricultural Statistics Service.",
        update_frequency="monthly",
        api_key_required=_key_required("usda"),
        url="https://quickstats.nass.usda.gov",
        table_prefix="usda_",
        tags=["agriculture", "crops", "livestock", "farming"],
        collections=[
            SourceCollection(
                name="Crop Data",
                endpoint="POST /usda/crop/ingest",
                description="Crop production and yield for a specific commodity (corn, soybeans, wheat, cotton, rice, oats, barley, sorghum).",
                table="usda_crop_*",
            ),
            SourceCollection(
                name="Livestock Inventory",
                endpoint="POST /usda/livestock/ingest",
                description="Livestock inventory data — cattle, hogs, sheep, chickens, turkeys by state.",
                table="usda_livestock_*",
            ),
            SourceCollection(
                name="Annual Crop Summary",
                endpoint="POST /usda/annual-summary/ingest",
                description="Annual crop production summary for all major crops — area planted, harvested, yield, production.",
                table="usda_annual_*",
            ),
        ],
    ),
    "noaa": SourceContext(
        key="noaa",
        display_name="NOAA Climate Data Online",
        short_name="NOAA",
        category="energy_agriculture",
        description="Weather observations, climate normals, storm events, and historical temperature/precipitation data.",
        update_frequency="daily",
        api_key_required=_key_required("noaa"),
        url="https://www.ncdc.noaa.gov/cdo-web",
        table_prefix="noaa_",
        tags=["weather", "climate", "temperature", "precipitation", "storms"],
        collections=[
            SourceCollection(
                name="Climate Data",
                endpoint="POST /noaa/ingest",
                description="GHCND daily observations, climate normals, GSOM monthly summaries, and hourly precipitation. Configurable dataset, date range, and location.",
                table="noaa_ghcnd_daily, noaa_normal_daily, noaa_gsom",
            ),
        ],
    ),

    # ── Financial & Regulatory (5) ────────────────────────────────────────
    "sec": SourceContext(
        key="sec",
        display_name="SEC EDGAR",
        short_name="SEC",
        category="financial_regulatory",
        description="10-K/10-Q filings, company financials, 13F institutional holdings, and insider transactions from SEC EDGAR.",
        update_frequency="daily",
        api_key_required=_key_required("sec"),
        url="https://www.sec.gov/edgar",
        table_prefix="sec_",
        tags=["filings", "financials", "13f", "insider-trading"],
        collections=[
            SourceCollection(
                name="Company Filings",
                endpoint="POST /sec/ingest/company",
                description="10-K, 10-Q, 8-K filings by CIK. Metadata: filing date, accession number, document URL.",
                table="sec_{filing_type}",
            ),
            SourceCollection(
                name="Batch Filings",
                endpoint="POST /sec/ingest/multiple",
                description="Same as Company Filings but multiple CIKs in parallel.",
                table="sec_{filing_type}",
            ),
            SourceCollection(
                name="XBRL Financials",
                endpoint="POST /sec/ingest/financial-data",
                description="Structured income statement, balance sheet, cash flow from SEC Company Facts API.",
                table="sec_financial_facts, sec_income_statement, sec_balance_sheet, sec_cash_flow_statement",
            ),
            SourceCollection(
                name="Industrial Batch",
                endpoint="POST /sec/ingest/industrial-companies",
                description="XBRL financials for all companies in industrial_companies table. Sequential with 1s delay.",
                table="sec_financial_facts",
            ),
            SourceCollection(
                name="Form ADV",
                endpoint="POST /sec/form-adv/ingest/family-offices",
                description="Investment adviser registrations from IAPD. AUM, client types, key personnel.",
                table="sec_form_adv, sec_form_adv_personnel",
            ),
        ],
    ),
    "form_adv": SourceContext(
        key="form_adv",
        display_name="SEC Form ADV",
        short_name="Form ADV",
        category="financial_regulatory",
        description="Investment adviser registrations and disclosures — AUM, client types, fee structures, and disciplinary history.",
        update_frequency="quarterly",
        api_key_required=False,
        url="https://adviserinfo.sec.gov",
        table_prefix="form_adv",
        tags=["advisers", "aum", "ria", "regulatory"],
        collections=[
            SourceCollection(
                name="ADV Filings",
                endpoint="POST /form-adv/ingest",
                description="Ingest Form ADV data — adviser registrations, AUM, client types, key personnel.",
                table="form_adv, form_adv_personnel",
            ),
        ],
    ),
    "form_d": SourceContext(
        key="form_d",
        display_name="SEC Form D",
        short_name="Form D",
        category="financial_regulatory",
        description="Private placement and Reg D exempt offering filings — fundraise amounts, issuer details, and investor accreditation.",
        update_frequency="weekly",
        api_key_required=False,
        url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=D",
        table_prefix="form_d",
        tags=["private-placements", "fundraising", "reg-d", "venture"],
        collections=[
            SourceCollection(
                name="Form D Filings",
                endpoint="POST /form-d/ingest",
                description="Ingest Form D filings — issuer details, offering amounts, investor types, exemptions claimed.",
                table="form_d_filings, form_d_issuers",
            ),
        ],
    ),
    "fdic": SourceContext(
        key="fdic",
        display_name="FDIC BankFind",
        short_name="FDIC",
        category="financial_regulatory",
        description="Bank financials, demographics, failed banks, and branch-level deposit data for 4,000+ U.S. insured institutions.",
        update_frequency="quarterly",
        api_key_required=_key_required("fdic"),
        url="https://www.fdic.gov/analysis",
        table_prefix="fdic_",
        tags=["banking", "deposits", "failed-banks", "branch-data"],
        collections=[
            SourceCollection(
                name="Bank Financials",
                endpoint="POST /fdic/financials/ingest",
                description="Quarterly Call Report data: 1,100+ financial metrics (assets, income, ROA, ROE, capital ratios). Filter by CERT or state.",
                table="fdic_bank_financials",
            ),
            SourceCollection(
                name="Institutions",
                endpoint="POST /fdic/institutions/ingest",
                description="Demographics for ~4,700 active banks: address, charter type, regulator, summary financials.",
                table="fdic_institutions",
            ),
            SourceCollection(
                name="Failed Banks",
                endpoint="POST /fdic/failed-banks/ingest",
                description="Historical bank failures since 1934: resolution, acquirer, estimated assets/deposits, FDIC cost.",
                table="fdic_failed_banks",
            ),
            SourceCollection(
                name="Summary of Deposits",
                endpoint="POST /fdic/deposits/ingest",
                description="Branch-level deposit data: ~85K+ branches with deposits, geo coordinates, main/branch flag.",
                table="fdic_summary_deposits",
            ),
        ],
    ),
    "fcc_broadband": SourceContext(
        key="fcc_broadband",
        display_name="FCC National Broadband Map",
        short_name="FCC",
        category="financial_regulatory",
        description="ISP coverage, broadband technology deployment, digital divide metrics, and telecom infrastructure by geography.",
        update_frequency="quarterly",
        api_key_required=_key_required("fcc_broadband"),
        url="https://broadbandmap.fcc.gov",
        table_prefix="fcc_",
        tags=["broadband", "isp", "telecom", "digital-divide"],
        collections=[
            SourceCollection(
                name="State Coverage",
                endpoint="POST /fcc-broadband/state/ingest",
                description="State-level broadband coverage — provider availability, technology types, download/upload speeds.",
                table="fcc_broadband_coverage",
            ),
            SourceCollection(
                name="All States (Bulk)",
                endpoint="POST /fcc-broadband/all-states/ingest",
                description="Broadband coverage for all 50 states + DC. ~31,000+ coverage records.",
                table="fcc_broadband_coverage",
            ),
            SourceCollection(
                name="County Coverage",
                endpoint="POST /fcc-broadband/county/ingest",
                description="County-level broadband coverage by 5-digit FIPS code.",
                table="fcc_broadband_summary",
            ),
        ],
    ),

    # ── Real Estate (1) ───────────────────────────────────────────────────
    "realestate": SourceContext(
        key="realestate",
        display_name="Real Estate / Housing Data",
        short_name="Real Estate",
        category="real_estate",
        description="FHFA House Price Index, HUD building permits, and regional housing market indicators.",
        update_frequency="quarterly",
        api_key_required=False,
        url="https://www.fhfa.gov/DataTools/Downloads/Pages/House-Price-Index.aspx",
        table_prefix="realestate_",
        tags=["housing", "home-prices", "permits", "fhfa"],
        collections=[
            SourceCollection(
                name="FHFA House Price Index",
                endpoint="POST /realestate/fhfa/ingest",
                description="Quarterly house price index — tracks single-family home value changes by state and metro area.",
                table="realestate_hpi",
            ),
            SourceCollection(
                name="HUD Building Permits",
                endpoint="POST /realestate/hud/ingest",
                description="Monthly building permits and housing starts by region and structure type.",
                table="realestate_hud_permits",
            ),
            SourceCollection(
                name="Redfin Market Data",
                endpoint="POST /realestate/redfin/ingest",
                description="Weekly housing market data — median prices, inventory, days on market, sale-to-list ratio.",
                table="realestate_redfin",
            ),
            SourceCollection(
                name="OSM Building Footprints",
                endpoint="POST /realestate/osm/ingest",
                description="OpenStreetMap building footprints via Overpass API — footprint area, type, coordinates.",
                table="realestate_osm_buildings",
            ),
        ],
    ),

    # ── Healthcare (1) ────────────────────────────────────────────────────
    "cms": SourceContext(
        key="cms",
        display_name="CMS Medicare Data",
        short_name="CMS",
        category="healthcare",
        description="Medicare utilization, hospital cost reports, provider payments, and drug pricing from CMS/HHS.",
        update_frequency="annual",
        api_key_required=False,
        url="https://data.cms.gov",
        table_prefix="cms_",
        tags=["medicare", "hospital-costs", "drug-pricing", "healthcare"],
        collections=[
            SourceCollection(
                name="Medicare Utilization",
                endpoint="POST /cms/ingest/medicare-utilization",
                description="Provider utilization and payment data — services, charges, payments, beneficiary counts by provider.",
                table="cms_medicare_utilization",
            ),
            SourceCollection(
                name="Hospital Cost Reports",
                endpoint="POST /cms/ingest/hospital-cost-reports",
                description="HCRIS hospital cost reporting — financials, utilization metrics, provider characteristics.",
                table="cms_hospital_cost_reports",
            ),
            SourceCollection(
                name="Drug Pricing",
                endpoint="POST /cms/ingest/drug-pricing",
                description="Medicare Part D drug spending — brand/generic names, total spending, claims, per-unit costs.",
                table="cms_drug_pricing",
            ),
        ],
    ),

    # ── Public Safety (2) ─────────────────────────────────────────────────
    "fbi_crime": SourceContext(
        key="fbi_crime",
        display_name="FBI Crime Data (UCR/NIBRS)",
        short_name="FBI Crime",
        category="site_intel",
        description="Uniform Crime Reporting and NIBRS incident data — crime statistics by agency, state, and national level.",
        update_frequency="annual",
        api_key_required=_key_required("fbi_crime"),
        url="https://crime-data-explorer.fr.cloud.gov",
        table_prefix="fbi_crime",
        tags=["crime", "public-safety", "law-enforcement", "ucr"],
        collections=[
            SourceCollection(
                name="Crime Estimates",
                endpoint="POST /fbi-crime/estimates/ingest",
                description="National and state-level crime estimates — violent crime, property crime, by offense type.",
                table="fbi_crime_estimates",
            ),
            SourceCollection(
                name="Summarized Agency Data",
                endpoint="POST /fbi-crime/summarized/ingest",
                description="Summarized crime counts by law enforcement agency with date range filtering.",
                table="fbi_crime_summarized",
            ),
            SourceCollection(
                name="NIBRS Incidents",
                endpoint="POST /fbi-crime/nibrs/ingest",
                description="Incident-based crime data — offenses, victims, offenders, demographics by state.",
                table="fbi_crime_nibrs",
            ),
            SourceCollection(
                name="Hate Crime Statistics",
                endpoint="POST /fbi-crime/hate-crime/ingest",
                description="Hate crime incidents — bias motivation, offense type, victim type at national and state level.",
                table="fbi_crime_hate",
            ),
            SourceCollection(
                name="LEOKA",
                endpoint="POST /fbi-crime/leoka/ingest",
                description="Law Enforcement Officers Killed and Assaulted — officer casualties and assault circumstances.",
                table="fbi_crime_leoka",
            ),
        ],
    ),
    "fema": SourceContext(
        key="fema",
        display_name="OpenFEMA",
        short_name="FEMA",
        category="site_intel",
        description="Disaster declarations, Public Assistance grants, Hazard Mitigation projects, and NFIP flood insurance claims.",
        update_frequency="weekly",
        api_key_required=_key_required("fema"),
        url="https://www.fema.gov/about/openfema",
        table_prefix="fema_",
        tags=["disasters", "flood-insurance", "hazard-mitigation", "emergency"],
        collections=[
            SourceCollection(
                name="Disaster Declarations",
                endpoint="POST /fema/disasters/ingest",
                description="All federally declared disasters since 1953. Type (Major/Emergency/Fire), state/county, IA/PA/HM eligibility. ~65K records.",
                table="fema_disaster_declarations",
            ),
            SourceCollection(
                name="Public Assistance",
                endpoint="POST /fema/public-assistance/ingest",
                description="Funded PA projects: damage category, obligated amounts, federal share. ~1M+ records.",
                table="fema_pa_projects",
            ),
            SourceCollection(
                name="Hazard Mitigation",
                endpoint="POST /fema/hazard-mitigation/ingest",
                description="HMA projects: HMGP/PDM/FMA programs, benefit-cost ratio, project amount, federal share. ~50K records.",
                table="fema_hma_projects",
            ),
        ],
    ),

    # ── Alternative Data (8) ──────────────────────────────────────────────
    "prediction_markets": SourceContext(
        key="prediction_markets",
        display_name="Prediction Markets (Kalshi + Polymarket)",
        short_name="Prediction Mkts",
        category="alt_data",
        description="Real-time event contract prices and implied probabilities from Kalshi and Polymarket.",
        update_frequency="daily",
        api_key_required=False,
        url="https://kalshi.com",
        table_prefix="prediction_market",
        tags=["prediction-markets", "event-contracts", "probabilities"],
        collections=[
            SourceCollection(
                name="Monitor All Platforms",
                endpoint="POST /prediction-markets/monitor/all",
                description="Fetch markets from Kalshi + Polymarket — prices, probabilities, volume, and alert on big moves.",
                table="prediction_markets, market_observations, market_alerts",
            ),
            SourceCollection(
                name="Kalshi Only",
                endpoint="POST /prediction-markets/monitor/kalshi",
                description="Monitor Kalshi markets — FED, CPI, GDP, election, crypto categories.",
                table="prediction_markets, market_observations",
            ),
            SourceCollection(
                name="Polymarket Only",
                endpoint="POST /prediction-markets/monitor/polymarket",
                description="Monitor Polymarket — politics, economics, sports, crypto, world events.",
                table="prediction_markets, market_observations",
            ),
        ],
    ),
    "job_postings": SourceContext(
        key="job_postings",
        display_name="Job Posting Intelligence",
        short_name="Job Postings",
        category="alt_data",
        description="Aggregated job listings from Greenhouse, Lever, Workday, and Ashby ATS boards — hiring velocity as a leading indicator.",
        update_frequency="daily",
        api_key_required=False,
        url="",
        table_prefix="job_posting",
        tags=["hiring", "ats", "workforce", "leading-indicator"],
        collections=[
            SourceCollection(
                name="Single Company",
                endpoint="POST /job-postings/collect/{company_id}",
                description="Auto-detect ATS (Greenhouse/Lever/Workday/Ashby), fetch all open roles, normalize titles & seniority, create daily snapshot.",
                table="job_postings, company_ats_config, job_posting_snapshots",
            ),
            SourceCollection(
                name="Bulk Collect",
                endpoint="POST /job-postings/collect-all",
                description="Collect from all companies with websites. Skips recently crawled (24h default).",
                table="job_postings, company_ats_config, job_posting_snapshots",
            ),
            SourceCollection(
                name="ATS Discovery",
                endpoint="POST /job-postings/discover-ats/{company_id}",
                description="Detect ATS platform type only (no job fetch). Tests Greenhouse, Lever, Ashby, Workday, SmartRecruiters endpoints.",
                table="company_ats_config",
            ),
            SourceCollection(
                name="Skills Extraction",
                endpoint="POST /job-postings/extract-skills/backfill",
                description="LLM-powered extraction of skills, certifications, education, years experience from job descriptions.",
                table="job_postings (updates requirements JSONB)",
            ),
        ],
    ),
    "web_traffic": SourceContext(
        key="web_traffic",
        display_name="Web Traffic (Tranco Rankings)",
        short_name="Web Traffic",
        category="alt_data",
        description="Tranco top-1M domain rankings — research-grade website popularity and traffic trends.",
        update_frequency="daily",
        api_key_required=False,
        url="https://tranco-list.eu",
        table_prefix="company_web_",
        tags=["web-traffic", "domain-rankings", "popularity"],
    ),
    "github": SourceContext(
        key="github",
        display_name="GitHub Analytics",
        short_name="GitHub",
        category="alt_data",
        description="Repository metrics, stars, forks, contributor trends, and developer velocity for public repos.",
        update_frequency="daily",
        api_key_required=False,
        url="https://github.com",
        table_prefix="github_",
        tags=["developer", "open-source", "repos", "stars"],
        collections=[
            SourceCollection(
                name="Organization Fetch",
                endpoint="POST /github/org/{org}/fetch",
                description="Fetch fresh data for a GitHub organization — repos, stars, forks, contributors, velocity score.",
                table="github_organizations, github_repositories, github_activity",
            ),
        ],
    ),
    "app_rankings": SourceContext(
        key="app_rankings",
        display_name="App Store Rankings",
        short_name="App Rankings",
        category="alt_data",
        description="iOS and Android app store metrics — ratings, reviews, ranking history, and download estimates.",
        update_frequency="daily",
        api_key_required=False,
        url="",
        table_prefix="app_store_",
        tags=["mobile", "app-store", "ratings", "downloads"],
        collections=[
            SourceCollection(
                name="Android Apps",
                endpoint="POST /apps/android",
                description="Add or update Android app data — package name, rating, reviews, installs, category.",
                table="app_store_apps",
            ),
            SourceCollection(
                name="Ranking Snapshots",
                endpoint="POST /apps/rankings",
                description="Record app ranking position over time — category rank, overall rank, platform.",
                table="app_store_rankings",
            ),
        ],
    ),
    "glassdoor": SourceContext(
        key="glassdoor",
        display_name="Glassdoor Company Data",
        short_name="Glassdoor",
        category="alt_data",
        description="Company reviews, overall ratings, CEO approval, salary data, and employee sentiment trends.",
        update_frequency="weekly",
        api_key_required=False,
        url="https://www.glassdoor.com",
        table_prefix="glassdoor_",
        tags=["reviews", "salary", "sentiment", "employer-brand"],
        collections=[
            SourceCollection(
                name="Company Data",
                endpoint="POST /glassdoor/company",
                description="Add or update company data — overall rating, CEO approval, recommend %, employee count, industry.",
                table="glassdoor_companies",
            ),
            SourceCollection(
                name="Salary Data (Bulk)",
                endpoint="POST /glassdoor/salaries/bulk",
                description="Bulk import salary data for a company — job title, base/total pay, location, experience level.",
                table="glassdoor_salaries",
            ),
            SourceCollection(
                name="Review Summaries",
                endpoint="POST /glassdoor/reviews/summary",
                description="Add review summary for a time period — rating trends, pros/cons themes, sentiment scores.",
                table="glassdoor_reviews",
            ),
        ],
    ),
    "yelp": SourceContext(
        key="yelp",
        display_name="Yelp Fusion",
        short_name="Yelp",
        category="alt_data",
        description="Local business listings, reviews, ratings, and activity data via the Yelp Fusion API.",
        update_frequency="varies",
        api_key_required=_key_required("yelp"),
        url="https://www.yelp.com",
        table_prefix="yelp_",
        tags=["local-business", "reviews", "ratings", "hospitality"],
        collections=[
            SourceCollection(
                name="Business Search",
                endpoint="POST /yelp/businesses/ingest",
                description="Ingest Yelp business listings for a single location — name, rating, reviews, category, coordinates.",
                table="yelp_businesses",
            ),
            SourceCollection(
                name="Multi-Location Search",
                endpoint="POST /yelp/businesses/multi-location/ingest",
                description="Ingest businesses across multiple locations in parallel.",
                table="yelp_businesses",
            ),
            SourceCollection(
                name="Categories",
                endpoint="POST /yelp/categories/ingest",
                description="Ingest all Yelp business categories — category aliases, titles, and parent categories.",
                table="yelp_categories",
            ),
        ],
    ),
    "foot_traffic": SourceContext(
        key="foot_traffic",
        display_name="Foot Traffic Intelligence",
        short_name="Foot Traffic",
        category="alt_data",
        description="Retail and hospitality foot traffic patterns from SafeGraph, Placer.ai, and Foursquare.",
        update_frequency="weekly",
        api_key_required=True,
        url="",
        table_prefix="foot_traffic_",
        tags=["foot-traffic", "retail", "location-analytics", "visits"],
        collections=[
            SourceCollection(
                name="Location Discovery",
                endpoint="POST /foot-traffic/locations/discover",
                description="Discover and track locations for a brand using multiple data sources.",
                table="locations",
            ),
            SourceCollection(
                name="Traffic Collection",
                endpoint="POST /foot-traffic/collect",
                description="Collect foot traffic observations for locations — daily/weekly visit counts by brand or location batch.",
                table="foot_traffic_observations",
            ),
            SourceCollection(
                name="Location Enrichment",
                endpoint="POST /foot-traffic/locations/{location_id}/enrich",
                description="Enrich a location with trade area demographics, competitive set, and ratings metadata.",
                table="location_metadata",
            ),
        ],
    ),

    # ── Other (5) ─────────────────────────────────────────────────────────
    "kaggle": SourceContext(
        key="kaggle",
        display_name="Kaggle Competition Datasets",
        short_name="Kaggle",
        category="alt_data",
        description="Competition datasets (M5 Forecasting, etc.) downloaded via the Kaggle API for modeling and research.",
        update_frequency="varies",
        api_key_required=_key_required("kaggle"),
        url="https://www.kaggle.com",
        table_prefix="m5_",
        tags=["competitions", "machine-learning", "forecasting"],
        collections=[
            SourceCollection(
                name="M5 Forecasting",
                endpoint="POST /kaggle/m5/ingest",
                description="M5 Forecasting competition dataset — 30K products, 1,941 days of sales, calendar, and pricing data.",
                table="m5_calendar, m5_items, m5_prices, m5_sales",
            ),
        ],
    ),
    "data_commons": SourceContext(
        key="data_commons",
        display_name="Google Data Commons",
        short_name="Data Commons",
        category="macro_economic",
        description="Unified public data from 200+ sources — demographics, economy, health, crime, and education via Google's knowledge graph.",
        update_frequency="varies",
        api_key_required=_key_required("data_commons"),
        url="https://datacommons.org",
        table_prefix="data_commons_",
        tags=["aggregator", "demographics", "health", "knowledge-graph"],
        collections=[
            SourceCollection(
                name="Statistical Variable",
                endpoint="POST /data-commons/stat-var/ingest",
                description="Ingest a single statistical variable for specified places (e.g., population for all US states).",
                table="data_commons_observations",
            ),
            SourceCollection(
                name="Place Statistics",
                endpoint="POST /data-commons/place-stats/ingest",
                description="Ingest multiple statistical variables for a single place (e.g., all health indicators for California).",
                table="data_commons_observations",
            ),
            SourceCollection(
                name="US States (Batch)",
                endpoint="POST /data-commons/us-states/ingest",
                description="Ingest key statistical variables for all 50 US states — demographics, economy, health, education.",
                table="data_commons_observations",
            ),
        ],
    ),
    "opencorporates": SourceContext(
        key="opencorporates",
        display_name="OpenCorporates",
        short_name="OpenCorporates",
        category="financial_regulatory",
        description="Global corporate registry data from 140+ jurisdictions — company filings, officers, and registration details.",
        update_frequency="daily",
        api_key_required=False,
        url="https://opencorporates.com",
        table_prefix="opencorporates_",
        tags=["corporate-registry", "global", "officers", "filings"],
        collections=[
            SourceCollection(
                name="Company Ingestion",
                endpoint="POST /opencorporates/ingest",
                description="Search and ingest company data — registration details, officers, and filings by company name and jurisdiction.",
                table="oc_companies, oc_officers, oc_filings",
            ),
        ],
    ),
    "uspto": SourceContext(
        key="uspto",
        display_name="USPTO PatentsView",
        short_name="USPTO",
        category="financial_regulatory",
        description="U.S. patent grants, applications, inventors, assignees, and CPC classification data via the PatentsView API.",
        update_frequency="weekly",
        api_key_required=_key_required("uspto"),
        url="https://patentsview.org",
        table_prefix="uspto_",
        tags=["patents", "intellectual-property", "inventors", "innovation"],
        collections=[
            SourceCollection(
                name="By Assignee",
                endpoint="POST /uspto/ingest/assignee",
                description="Patents by company name (e.g., 'Apple Inc.'). Title, abstract, dates, claims, citations, CPC codes.",
                table="uspto_patents, uspto_assignees",
            ),
            SourceCollection(
                name="By CPC Code",
                endpoint="POST /uspto/ingest/cpc",
                description="Patents by technology classification (e.g., G06N=AI/ML, H01L=Semiconductors). Up to 10K patents.",
                table="uspto_patents, uspto_inventors",
            ),
            SourceCollection(
                name="By Search",
                endpoint="POST /uspto/ingest/search",
                description="Free-text search across patent titles and abstracts.",
                table="uspto_patents",
            ),
        ],
    ),
    "medspa_discovery": SourceContext(
        key="medspa_discovery",
        display_name="Med-Spa Market Discovery",
        short_name="Med-Spa",
        category="healthcare",
        description="Med-spa acquisition prospect discovery using Yelp business data combined with ZIP-level affluence scoring.",
        update_frequency="varies",
        api_key_required=_key_required("yelp"),
        url="",
        table_prefix="medspa_",
        tags=["medspa", "acquisition", "yelp", "zip-score"],
        collections=[
            SourceCollection(
                name="Market Discovery",
                endpoint="POST /medspa-discovery/discover",
                description="Search Yelp for med-spa businesses in top affluent ZIP codes, score and rank prospects for acquisition.",
                table="medspa_prospects",
            ),
        ],
    ),
}


# ---------------------------------------------------------------------------
# Accessor helpers
# ---------------------------------------------------------------------------

def get_source(key: str) -> Optional[SourceContext]:
    """Return a single source by key, or None if not found."""
    return SOURCE_REGISTRY.get(key)


def get_all_sources() -> List[SourceContext]:
    """Return all registered sources, sorted by key."""
    return sorted(SOURCE_REGISTRY.values(), key=lambda s: s.key)


def get_sources_by_category(category: str) -> List[SourceContext]:
    """Return sources belonging to a given domain category key."""
    return [s for s in SOURCE_REGISTRY.values() if s.category == category]
