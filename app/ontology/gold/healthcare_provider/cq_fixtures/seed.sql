-- SPEC_162 gold fixtures: synthetic seed rows for the gold SQL (no real NPI, person or facility).
-- Loaded by tests into a throwaway schema (search_path set by the test), never into public.
-- Column types follow the live tables (post-alembic 0022 for cms_medicare_utilization).
-- HCPCS values are Level II codes only (no AMA CPT content); NUCC rows are two public codes.

CREATE TABLE nppes_providers (
    npi TEXT PRIMARY KEY,
    entity_type TEXT,
    legal_name TEXT,
    first_name TEXT,
    last_name TEXT,
    credential TEXT,
    dba_name TEXT,
    gender TEXT,
    practice_address_line1 TEXT,
    practice_address_line2 TEXT,
    practice_city TEXT,
    practice_state TEXT,
    practice_zip TEXT,
    practice_phone TEXT,
    practice_fax TEXT,
    mailing_address_line1 TEXT,
    mailing_address_line2 TEXT,
    mailing_city TEXT,
    mailing_state TEXT,
    mailing_zip TEXT,
    taxonomy_code TEXT,
    taxonomy_description TEXT,
    taxonomy_license TEXT,
    taxonomy_state TEXT,
    enumeration_date DATE,
    last_updated DATE,
    status TEXT,
    sole_proprietor TEXT,
    organization_subpart TEXT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cms_medicare_utilization (
    id SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_year INTEGER NOT NULL,
    rndrng_npi TEXT,
    rndrng_prvdr_last_org_name TEXT,
    rndrng_prvdr_type TEXT,
    rndrng_prvdr_state_abrvtn TEXT,
    rndrng_prvdr_state_fips TEXT,
    rndrng_prvdr_zip5 TEXT,
    rndrng_prvdr_ruca TEXT,
    rndrng_prvdr_mdcr_prtcptg_ind TEXT,
    hcpcs_cd TEXT,
    place_of_srvc TEXT,
    tot_benes INTEGER,
    tot_srvcs NUMERIC,
    avg_mdcr_alowd_amt NUMERIC,
    avg_mdcr_pymt_amt NUMERIC,
    CONSTRAINT uq_gold_fixture_utilization_key UNIQUE (data_year, rndrng_npi, hcpcs_cd, place_of_srvc)
);

CREATE TABLE cms_hospitals (
    id SERIAL PRIMARY KEY,
    facility_id TEXT NOT NULL UNIQUE,
    facility_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    hospital_type TEXT,
    ownership TEXT,
    emergency_services BOOLEAN,
    overall_rating INTEGER,
    mortality_rating TEXT,
    readmission_rating TEXT,
    patient_experience_rating TEXT,
    effectiveness_rating TEXT,
    timeliness_rating TEXT,
    imaging_rating TEXT,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cms_hospital_cost_reports (
    id SERIAL PRIMARY KEY,
    rpt_rec_num TEXT,
    prvdr_num TEXT,
    npi TEXT,
    bed_cnt INTEGER,
    tot_costs NUMERIC,
    net_income NUMERIC
);

CREATE TABLE pe_portfolio_companies (
    id INTEGER PRIMARY KEY,
    name TEXT,
    legal_name TEXT,
    current_pe_owner TEXT
);

CREATE TABLE nucc_taxonomy (
    code TEXT PRIMARY KEY,
    grouping TEXT,
    classification TEXT,
    specialization TEXT
);

INSERT INTO nppes_providers (npi, entity_type, legal_name, first_name, last_name, credential, dba_name, gender,
    practice_address_line1, practice_city, practice_state, practice_zip, practice_phone,
    taxonomy_code, taxonomy_description, taxonomy_license, taxonomy_state,
    enumeration_date, last_updated, status, sole_proprietor, organization_subpart) VALUES
 ('1000000001', '1', 'TESTA ALPHA', 'TESTA', 'ALPHA', 'MD', NULL, 'F',
  '100 MAIN ST', 'SPRINGFIELD', 'IL', '627010001', '2175551000',
  '207Q00000X', 'Family Medicine', 'IL-0001', 'IL', '2010-01-04', '2024-05-01', 'A', 'NO', NULL),
 ('1000000002', '1', 'TESTB BETA', 'TESTB', 'BETA', 'DO', NULL, 'M',
  '200 OAK AVE', 'SPRINGFIELD', 'IL', '62702', '2175552000',
  '207Q00000X', 'Family Medicine', 'IL-0002', 'IL', '2012-03-15', '2023-11-20', 'A', 'YES', NULL),
 ('1000000003', '1', 'TESTC GAMMA', 'TESTC', 'GAMMA', 'NP', NULL, 'F',
  '100 Main St ', 'SPRINGFIELD', 'IL', '62701', '2175559999',
  '363L00000X', 'Nurse Practitioner', NULL, NULL, '2015-06-30', '2025-01-10', 'A', 'NO', NULL),
 ('1000000004', '1', 'TESTD DELTA', 'TESTD', 'DELTA', 'PA-C', NULL, 'M',
  '9 ELM RD', 'SPRINGFIELD', 'IL', '62703', '2175551000',
  '363A00000X', 'Physician Assistant', 'IL-0004', 'IL', '2018-08-01', '2024-02-02', 'A', 'NO', NULL),
 ('1000000010', '2', 'TEST CLINIC GROUP LLC', NULL, NULL, NULL, 'TEST CLINIC', NULL,
  '100 MAIN ST', 'SPRINGFIELD', 'IL', '62701', '2175551000',
  '261QP2300X', 'Clinic/Center, Primary Care', NULL, NULL, '2009-09-09', '2024-06-06', 'A', NULL, 'NO'),
 ('1000000011', '2', 'TEST HEALTH SYSTEM CAMPUS', NULL, NULL, NULL, NULL, NULL,
  '500 LAKE DR', 'PEORIA', 'IL', '61602', '3095550500',
  '282N00000X', 'General Acute Care Hospital', NULL, NULL, '2007-02-02', '2022-12-12', 'A', NULL, 'YES'),
 ('1000000012', '2', 'ACME DERMATOLOGY PARTNERS', NULL, NULL, NULL, NULL, NULL,
  '1 COMMERCE ST', 'DALLAS', 'TX', '752010001', '2145550100',
  '207N00000X', 'Dermatology', NULL, NULL, '2016-04-04', '2025-03-03', 'A', NULL, 'NO'),
 ('1000000013', '2', 'Acme Dermatology Partners ', NULL, NULL, NULL, NULL, NULL,
  '2 MAIN ST', 'HOUSTON', 'TX', '77002', '7135550200',
  '207N00000X', 'Dermatology', NULL, NULL, '2019-07-07', '2025-03-04', 'A', NULL, 'NO');

INSERT INTO cms_medicare_utilization (data_year, rndrng_npi, rndrng_prvdr_last_org_name, rndrng_prvdr_type,
    rndrng_prvdr_state_abrvtn, rndrng_prvdr_state_fips, rndrng_prvdr_zip5, rndrng_prvdr_ruca,
    rndrng_prvdr_mdcr_prtcptg_ind, hcpcs_cd, place_of_srvc, tot_benes, tot_srvcs,
    avg_mdcr_alowd_amt, avg_mdcr_pymt_amt) VALUES
 (2023, '1000000001', 'ALPHA', 'Family Practice', 'IL', '17', '62701', '1', 'Y', 'G0438', 'O', 20, 20, 170.50, 133.20),
 (2024, '1000000001', 'ALPHA', 'Family Practice', 'IL', '17', '62701', '1', 'Y', 'G0438', 'O', 25, 25, 175.00, 137.00),
 (2024, '1000000001', 'ALPHA', 'Family Practice', 'IL', '17', '62701', '1', 'Y', 'G0439', 'O', 30, 30, 120.00, 94.00),
 (2024, '1000000002', 'BETA', 'Family Practice', 'IL', '17', '62702', '1', 'Y', 'G0439', 'O', 40, 41, 121.00, 95.00),
 (2024, '1000000002', 'BETA', 'Family Practice', 'IL', '17', '62702', '1', 'Y', 'G0438', 'F', 5, 5, 150.00, 118.00),
 (2024, '1000000005', 'EPSILON', 'Family Practice', 'IL', '17', '62704', '2', 'N', 'G0439', 'O', 10, 10, 119.00, 93.00),
 (2024, '1000000006', 'ZETA', 'Internal Medicine', 'IL', '17', '62701', '1', 'Y', 'G0439', 'O', 3, 3, 118.00, 92.00),
 (2024, '1000000007', 'ETA', 'Family Practice', 'WY', '56', '82601', '4', 'Y', 'G0438', 'O', 12, 12, 180.00, 141.00);

INSERT INTO cms_hospitals (facility_id, facility_name, address, city, state, zip_code, county, hospital_type,
    ownership, emergency_services, overall_rating, mortality_rating, readmission_rating,
    patient_experience_rating, effectiveness_rating, timeliness_rating, imaging_rating) VALUES
 ('140001', 'TEST GENERAL HOSPITAL', '800 FIRST ST', 'SPRINGFIELD', 'IL', '62701', 'SANGAMON',
  'Acute Care Hospitals', 'Voluntary non-profit - Private', true, 4,
  'Same as the national average', 'Below the national average', 'Above the national average',
  'Same as the national average', 'Same as the national average', 'Not Available'),
 ('14001F', 'TEST VA MEDICAL CENTER', '900 SECOND ST', NULL, 'IL', '60612', NULL,
  'Acute Care - Veterans Administration', 'Veterans Health Administration', false, NULL,
  NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO cms_hospital_cost_reports (rpt_rec_num, prvdr_num, npi, bed_cnt, tot_costs, net_income) VALUES
 ('900001', NULL, NULL, NULL, NULL, NULL);

INSERT INTO pe_portfolio_companies (id, name, legal_name, current_pe_owner) VALUES
 (1, 'Acme Dermatology Partners', NULL, 'Test Capital Partners'),
 (2, 'Other Co', 'OTHER CO LLC', 'Test Capital Partners'),
 (3, 'Unrelated Practice', NULL, 'Other PE Fund');

INSERT INTO nucc_taxonomy (code, grouping, classification, specialization) VALUES
 ('207Q00000X', 'Allopathic & Osteopathic Physicians', 'Family Medicine', NULL),
 ('363L00000X', 'Physician Assistants & Advanced Practice Nursing Providers', 'Nurse Practitioner', NULL);
