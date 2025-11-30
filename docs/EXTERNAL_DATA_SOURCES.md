# External Data Sources Checklist

## 1. U.S. Census Bureau

- [x] ACS 5-Year (2020–2023) — API Key Required
- [x] Decennial Census 2020 — API Key Required
- [x] PUMS — API Key Required
- [x] TIGER/Line GeoJSON — No API Key
- [x] Crosswalk Files (state/county/tract/zip) — No API Key

## 2. Bureau of Labor Statistics (BLS)

- [x] CPS — API Key Optional (Recommended)
- [x] CES — API Key Optional (Recommended)
- [x] OES — API Key Optional (Recommended)
- [x] JOLTS — API Key Optional (Recommended)
- [x] CPI/PPI — API Key Optional (Recommended)

## 3. Bureau of Economic Analysis (BEA)

- [ ] NIPA Tables (GDP, PCE, Investment) — API Key Required
- [ ] Regional GDP — API Key Required
- [ ] Industry Input–Output Tables — API Key Required
- [ ] Fixed Assets — API Key Required

## 4. Federal Reserve (FRED)

- [x] Core Time Series — API Key Optional (Recommended) ✅ IMPLEMENTED
- [x] H.15 Interest Rates — API Key Optional (Recommended) ✅ IMPLEMENTED
- [x] Monetary Aggregates (M1, M2) — API Key Optional (Recommended) ✅ IMPLEMENTED
- [x] Industrial Production — API Key Optional (Recommended) ✅ IMPLEMENTED

## 5. NOAA (Weather & Climate)

- [x] Daily/Hourly Weather Observations — Token Required (Free)
- [x] Climate Normals — Token Required (Free)
- [ ] Storm Events Database — No API Key (CSV Downloads)
- [ ] NEXRAD Indexes — No API Key (AWS Open Data)

## 6. EIA (Energy Information Administration)

- [x] Petroleum & Gas Data — API Key Required ✅ IMPLEMENTED
- [x] Electricity Data — API Key Required ✅ IMPLEMENTED
- [x] Retail Gas Prices — API Key Required ✅ IMPLEMENTED
- [x] STEO Projections — API Key Required ✅ IMPLEMENTED

## 7. USDA (Agriculture)

- [ ] WASDE — No API Key
- [ ] Crop Progress — No API Key
- [ ] Food Price Monitoring — No API Key
- [ ] Census of Agriculture — No API Key

## 8. CMS / HHS (Healthcare)

- [ ] Medicare Utilization — No API Key
- [ ] Hospital Cost Reports — No API Key
- [ ] Drug Pricing Benchmarks — No API Key

## 9. SEC EDGAR (Corporate Filings)

- [x] 10-K — No API Key ✅ IMPLEMENTED
- [x] 10-Q — No API Key ✅ IMPLEMENTED
- [x] 8-K — No API Key ✅ IMPLEMENTED
- [x] S-1 / S-3 / S-4 — No API Key ✅ IMPLEMENTED
- [x] XBRL Extraction — No API Key ✅ IMPLEMENTED

## 10. USPTO (Patents)

- [ ] Bulk Patent Text — No API Key
- [ ] Patent Metadata — No API Key
- [ ] Citation Graphs — No API Key

## 11. Federal Register & Regulations.gov

- [ ] Federal Register Rules — No API Key
- [ ] Proposed Rules — No API Key
- [ ] Notices — No API Key
- [ ] Public Comments (Regulations.gov) — No API Key

## 12. Real Estate / Housing

- [x] FHFA House Price Index — No API Key ✅ IMPLEMENTED
- [x] HUD Permits & Starts — No API Key ✅ IMPLEMENTED
- [x] Redfin Data Dump — No API Key ✅ IMPLEMENTED
- [x] OpenStreetMap Building Footprints — No API Key ✅ IMPLEMENTED

## 13. Mobility & Consumer Activity

- [ ] Google Mobility — No API Key
- [ ] Apple Mobility — No API Key
- [ ] Census Retail Trade — No API Key
- [ ] BEA PCE (Consumer Spending) — API Key Required


## 14. Public Pension LP Investment Strategies

### **U.S. Mega Public Pension Funds**

* [x] CalPERS — No API Key ✅ IMPLEMENTED
* [x] CalSTRS — No API Key ✅ IMPLEMENTED
* [x] New York State Common Retirement Fund (NYSCRF) — No API Key ✅ IMPLEMENTED
* [x] Texas Teachers Retirement System (TRS) — No API Key ✅ IMPLEMENTED
* [x] Florida SBA — No API Key ✅ IMPLEMENTED
* [x] Illinois Teachers' Retirement System (TRS Illinois) — No API Key ✅ IMPLEMENTED
* [x] Pennsylvania Public School Employees' Retirement System (PSERS) — No API Key ✅ IMPLEMENTED
* [x] Washington State Investment Board (WSIB) — No API Key ✅ IMPLEMENTED
* [x] New Jersey Division of Investment — No API Key ✅ IMPLEMENTED
* [x] Ohio Public Employees Retirement System (OPERS) — No API Key ✅ IMPLEMENTED
* [x] Ohio State Teachers Retirement System (STRS Ohio) — No API Key ✅ IMPLEMENTED
* [x] North Carolina Retirement Systems — No API Key ✅ IMPLEMENTED
* [ ] Georgia Teachers Retirement System — No API Key
* [x] Virginia Retirement System (VRS) — No API Key ✅ IMPLEMENTED
* [x] Massachusetts PRIM — No API Key ✅ IMPLEMENTED
* [ ] Colorado PERA — No API Key
* [x] Wisconsin Investment Board (SWIB) — No API Key ✅ IMPLEMENTED
* [ ] Minnesota State Board of Investment (SBI) — No API Key
* [ ] Arizona State Retirement System (ASRS) — No API Key
* [ ] Michigan Office of Retirement Services — No API Key

### **Other U.S. State / Municipal Funds**

* [ ] New York City Retirement Systems (NYCERS, TRS NYC, BERS) — No API Key
* [ ] Los Angeles Fire & Police Pensions (LAFPP) — No API Key
* [ ] Los Angeles City Employees’ Retirement System (LACERS) — No API Key
* [ ] San Francisco Employees’ Retirement System (SFERS) — No API Key
* [ ] Houston Firefighters’ Relief & Retirement Fund — No API Key
* [ ] Chicago Teachers’ Pension Fund (CTPF) — No API Key
* [ ] Kentucky Teachers’ Retirement System — No API Key
* [ ] Maryland State Retirement & Pension System — No API Key
* [ ] Nevada PERS — No API Key
* [ ] Alaska Permanent Fund Corporation (APFC) — No API Key (technically sovereign wealth–like, but public)

### **U.S. University Endowments (Publicly Reported Data)**

*(universities don't disclose as much, but they publish annual reports and investment policies)*

* [x] Harvard Management Company — No API Key ✅ IMPLEMENTED
* [x] Yale Investments Office — No API Key ✅ IMPLEMENTED
* [x] Stanford Management Company — No API Key ✅ IMPLEMENTED
* [ ] MITIMCo — No API Key
* [ ] Princeton University Investment Company (PRINCO) — No API Key
* [ ] University of California Regents — No API Key
* [ ] University of Michigan Endowment — No API Key
* [ ] UTIMCO (University of Texas/Texas A&M) — No API Key
* [ ] Northwestern University — No API Key
* [ ] Duke University — No API Key

### **Canadian Pensions (top global LPs)**

* [x] CPP Investments (CPPIB) — No API Key ✅ IMPLEMENTED
* [x] Ontario Teachers' Pension Plan (OTPP) — No API Key ✅ IMPLEMENTED
* [x] Ontario Municipal Employees Retirement System (OMERS) — No API Key ✅ IMPLEMENTED
* [ ] British Columbia Investment Management Corporation (BCI) — No API Key
* [x] Caisse de dépôt et placement du Québec (CDPQ) — No API Key ✅ IMPLEMENTED
* [ ] Public Sector Pension Investment Board (PSP Investments) — No API Key

### **European Public / Sovereign Funds**

* [x] Norges Bank Investment Management (NBIM / Norway GPFG) — No API Key ✅ IMPLEMENTED
* [ ] AP Funds (AP1, AP2, AP3, AP4, AP6, AP7 – Sweden) — No API Key
* [x] Dutch ABP (via APG) — No API Key ✅ IMPLEMENTED
* [ ] PFZW (via PGGM) — No API Key
* [ ] UK USS (Universities Superannuation Scheme) — No API Key
* [ ] Irish Strategic Investment Fund (ISIF) — No API Key
* [ ] Finland Varma — No API Key
* [ ] Finland Ilmarinen — No API Key
* [ ] Denmark ATP — No API Key

### **Asia-Pacific Public Funds**

* [x] AustralianSuper — No API Key ✅ IMPLEMENTED
* [x] Future Fund (Australia SWF) — No API Key ✅ IMPLEMENTED
* [x] New Zealand Super Fund — No API Key ✅ IMPLEMENTED
* [ ] GPIF Japan — No API Key
* [x] GIC Singapore — No API Key (annual & policy docs are public enough to parse) ✅ IMPLEMENTED
* [ ] Temasek — No API Key

### **Middle East Sovereign Wealth Funds**

*(publishing is limited but strategy PDFs exist)*

* [x] ADIA (Abu Dhabi Investment Authority) — No API Key ✅ IMPLEMENTED
* [ ] Mubadala — No API Key
* [ ] QIA (Qatar Investment Authority) — No API Key
* [ ] PIF Saudi Arabia — No API Key

### **Latin America Public Funds**

* [ ] Chile Pension Funds (AFP system) — No API Key
* [ ] Mexico AFORES — No API Key

### **Quarterly Strategy Extraction**

* [ ] Extract Q3 2025 Strategy Summaries Across All LPs — No API Key
* [ ] Extract Target Allocation Tables — No API Key
* [ ] Extract Commitment & Pacing Plans — No API Key
* [ ] Extract Thematic Focus (AI, Energy Transition, Climate, etc.) — No API Key

Absolutely — here is a **clean, explicit, checklist-style expansion** for **major global family offices**, using the **exact same format** as your LP lists.


## 15. Family Office Strategy Documents

### Implementation Status: ✅ **SEC Form ADV Ingestion**

**Data Source:** SEC Investment Adviser Public Disclosure (IAPD) + Form ADV  
**Status:** Implemented  
**Endpoint:** `/api/v1/sec/form-adv/ingest/family-offices`

**What We Can Get:**
- ✅ Business addresses (street, city, state, zip)
- ✅ Business phone numbers
- ✅ Business email addresses
- ✅ Website URLs
- ✅ Key personnel names and titles
- ✅ Assets under management
- ✅ Registration status and dates

**Important Limitations:**
- ⚠️ Only for **registered investment advisers** (many family offices are exempt)
- ⚠️ Business contact info only (not personal PII)
- ⚠️ Not all family offices are required to register with SEC
- ⚠️ Some may be registered only with state regulators

**How to Use:**
```bash
# Test the ingestion
python test_formadv_ingestion.py

# Or via API:
curl -X POST http://localhost:8000/api/v1/sec/form-adv/ingest/family-offices \
  -H "Content-Type: application/json" \
  -d '{
    "family_office_names": ["Soros Fund Management", "Pritzker Group"],
    "max_concurrency": 1,
    "max_requests_per_second": 2.0
  }'
```

**Database Tables:**
- `sec_form_adv` - Main adviser information
- `sec_form_adv_personnel` - Key personnel details

### **U.S. Large Family Offices**

* [x] Soros Fund Management — SEC Form ADV (if registered)
* [x] Cohen Private Ventures (Steve Cohen) — SEC Form ADV (if registered)
* [x] MSD Capital / MSD Partners (Michael Dell) — SEC Form ADV (if registered)
* [x] Cascade Investment (Bill Gates) — SEC Form ADV (if registered)
* [x] Walton Family Office — SEC Form ADV (if registered)
* [x] Bezos Expeditions — SEC Form ADV (if registered)
* [x] Emerson Collective (Laurene Powell Jobs) — SEC Form ADV (if registered)
* [x] Shad Khan Family Office — SEC Form ADV (if registered)
* [x] Perot Investments — SEC Form ADV (if registered)
* [x] Pritzker Group — SEC Form ADV (if registered)
* [x] Ballmer Group — SEC Form ADV (if registered)
* [x] Arnold Ventures — SEC Form ADV (if registered)
* [x] Hewlett Foundation — SEC Form ADV (if registered)
* [x] Packard Foundation — SEC Form ADV (if registered)
* [x] Raine Group — SEC Form ADV (if registered)

### **Europe Family Offices**

* [ ] Cevian Capital / Cevian Family Office — No API Key
* [ ] LGT Group (Liechtenstein Royal Family) — No API Key
* [ ] Bertelsmann / Mohn Family Office — No API Key
* [ ] Reimann Family (JAB Holding Company) — No API Key
* [ ] Kyocera Family Office (Yamamura) — No API Key
* [ ] Agnelli Family (Exor) — No API Key
* [ ] BMW Quandt Family Office — No API Key
* [ ] Ferrero Family Office — No API Key
* [ ] Heineken Family Office — No API Key
* [ ] Hermès Family Office (Axile) — No API Key

### **Middle East & Asian Family Offices**

* [ ] Alwaleed Bin Talal Kingdom Holding — No API Key
* [ ] Olayan Group — No API Key
* [ ] Al-Futtaim Family Office — No API Key
* [ ] Mitsubishi Kinzoku / industrial family holdings — No API Key
* [ ] Tata Group Family Holdings — No API Key
* [ ] Cheng Family (New World / Chow Tai Fook) — No API Key
* [ ] Lee Family (Samsung) — No API Key
* [ ] Kuok Group — No API Key
* [ ] Temasek-adjacent private family entities (public docs only) — No API Key

### **Latin America Family Offices**

* [ ] Safra Family Office — No API Key
* [ ] Lemann Family (3G Capital adjacent) — No API Key
* [ ] Marinho Family (Globo) — No API Key
* [ ] Santo Domingo Family Office — No API Key
* [ ] Paulmann Family (Cencosud) — No API Key
* [ ] Luksic Family Office — No API Key

### **Common Document Types These Offices Publish**

* [ ] Annual investment letters
* [ ] Stewardship reports
* [ ] ESG / sustainability updates
* [ ] Public speeches and interviews from CIO / principal
* [ ] 13F filings (for U.S.-registered FO investment advisors)
* [ ] Regulatory filings (where applicable)

### **Strategy Extraction Tasks**

* [ ] Extract sector tilts (AI, healthcare, industrials, energy transition)
* [ ] Extract geographical preferences (US, EU, EM, APAC)
* [ ] Extract private vs public allocations (if disclosed)
* [ ] Extract forward-looking commentary (macro outlook, positioning)
* [ ] Extract key themes (AI, early-stage tech, climate infra, digital assets)

