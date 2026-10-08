-- SPEC_162 gold SQL: the reference answer for each of the 13 groundable competency questions
-- (PLAN_100 §5.3: HOLD + PARTIAL after SPEC_163). R2a checks which columns these read; R2b
-- (SPEC_169) compares translator answers with these results.
--
-- Rules: read-only; bind parameters only (:name), never interpolated text; deterministic
-- ORDER BY on every multi-row answer; written against the post-0022 schema (data_year exists).
-- CQ04 reads nucc_taxonomy(code, grouping, classification, specialization), the NUCC table
-- SPEC_163 vendors (reconcile the name there). Expected results on the seed rows live in
-- cq_fixtures/expected.json; tests/test_spec_162_* runs every block on cq_fixtures/seed.sql.

-- name: CQ01
-- Given an NPI: individual or organization, active or deactivated, enumeration and last-update dates, DBA name.
SELECT npi,
       CASE entity_type WHEN '1' THEN 'individual' WHEN '2' THEN 'organization' END AS entity_kind,
       status,
       (status = 'A') AS active,
       enumeration_date,
       last_updated,
       dba_name
  FROM nppes_providers
 WHERE npi = :npi;

-- name: CQ02
-- A practitioner's credentials, primary NUCC specialty, and licence state for that taxonomy.
SELECT npi,
       credential,
       taxonomy_code,
       taxonomy_description,
       taxonomy_license,
       taxonomy_state AS license_state
  FROM nppes_providers
 WHERE npi = :npi
   AND entity_type = '1';

-- name: CQ04
-- NUCC grouping, classification and specialization a provider's taxonomy code rolls up to.
SELECT p.npi,
       p.taxonomy_code,
       n.grouping,
       n.classification,
       n.specialization
  FROM nppes_providers p
  JOIN nucc_taxonomy n ON n.code = upper(trim(p.taxonomy_code))
 WHERE p.npi = :npi;

-- name: CQ05
-- Individual NPIs sharing a practice address (line 1 + ZIP5) or phone with an organization NPI.
SELECT o.npi AS org_npi,
       i.npi AS individual_npi,
       CASE
         WHEN upper(trim(i.practice_address_line1)) = upper(trim(o.practice_address_line1))
              AND left(i.practice_zip, 5) = left(o.practice_zip, 5)
              AND i.practice_phone = o.practice_phone THEN 'address+phone'
         WHEN upper(trim(i.practice_address_line1)) = upper(trim(o.practice_address_line1))
              AND left(i.practice_zip, 5) = left(o.practice_zip, 5) THEN 'address'
         ELSE 'phone'
       END AS shared_on
  FROM nppes_providers o
  JOIN nppes_providers i
    ON i.entity_type = '1'
   AND ((upper(trim(i.practice_address_line1)) = upper(trim(o.practice_address_line1))
         AND left(i.practice_zip, 5) = left(o.practice_zip, 5))
        OR i.practice_phone = o.practice_phone)
 WHERE o.npi = :org_npi
   AND o.entity_type = '2'
 ORDER BY i.npi;

-- name: CQ08
-- Whether an organization NPI is a subpart (the parent itself is post-NEED: NPPES V.2 parent LBN).
SELECT npi,
       organization_subpart,
       (upper(organization_subpart) IN ('Y', 'YES')) AS is_subpart
  FROM nppes_providers
 WHERE npi = :npi
   AND entity_type = '2';

-- name: CQ11
-- Where a provider practises: address, ZIP5, state; state FIPS and RUCA from the latest utilization year.
-- County FIPS is post-NEED (HUD ZIP-county crosswalk).
SELECT p.npi,
       p.practice_address_line1,
       p.practice_city,
       p.practice_state,
       left(p.practice_zip, 5) AS zip5,
       u.rndrng_prvdr_state_fips AS state_fips,
       u.rndrng_prvdr_ruca AS ruca
  FROM nppes_providers p
  LEFT JOIN LATERAL (
        SELECT rndrng_prvdr_state_fips, rndrng_prvdr_ruca
          FROM cms_medicare_utilization
         WHERE rndrng_npi = p.npi
         ORDER BY data_year DESC, hcpcs_cd, place_of_srvc
         LIMIT 1) u ON true
 WHERE p.npi = :npi;

-- name: CQ12
-- Providers of taxonomy X per ZIP5 (per-capita needs Census population: post-NEED).
SELECT taxonomy_code,
       left(practice_zip, 5) AS zip5,
       count(*) AS providers
  FROM nppes_providers
 WHERE taxonomy_code = :taxonomy_code
 GROUP BY 1, 2
 ORDER BY zip5;

-- name: CQ14
-- A hospital's type, emergency capability and county.
SELECT facility_id AS ccn,
       hospital_type,
       emergency_services,
       county
  FROM cms_hospitals
 WHERE facility_id = :ccn;

-- name: CQ15
-- Whether a provider takes Medicare assignment, per data year.
SELECT rndrng_npi AS npi,
       data_year,
       bool_or(rndrng_prvdr_mdcr_prtcptg_ind = 'Y') AS accepts_assignment
  FROM cms_medicare_utilization
 WHERE rndrng_npi = :npi
 GROUP BY 1, 2
 ORDER BY data_year;

-- name: CQ18
-- For an NPI: HCPCS billed, volumes, average allowed and paid, per data year.
SELECT data_year,
       hcpcs_cd,
       place_of_srvc,
       tot_srvcs,
       tot_benes,
       avg_mdcr_alowd_amt,
       avg_mdcr_pymt_amt
  FROM cms_medicare_utilization
 WHERE rndrng_npi = :npi
 ORDER BY data_year, hcpcs_cd, place_of_srvc;

-- name: CQ19
-- Top HCPCS by place of service for a specialty (CMS provider type) in a state, one data year.
SELECT hcpcs_cd,
       place_of_srvc,
       sum(tot_srvcs) AS services,
       count(DISTINCT rndrng_npi) AS providers
  FROM cms_medicare_utilization
 WHERE rndrng_prvdr_type = :provider_type
   AND rndrng_prvdr_state_abrvtn = :state
   AND data_year = :data_year
 GROUP BY 1, 2
 ORDER BY services DESC, hcpcs_cd, place_of_srvc
 LIMIT 10;

-- name: CQ23
-- Practices that are PE portfolio companies (exact legal-name match to a type-2 NPI; low
-- confidence until SPEC_168) and their NPI / location footprint for one PE owner.
SELECT c.id AS portfolio_company_id,
       c.name AS company_name,
       p.npi,
       p.practice_state,
       left(p.practice_zip, 5) AS zip5
  FROM pe_portfolio_companies c
  JOIN nppes_providers p
    ON p.entity_type = '2'
   AND upper(trim(p.legal_name)) = upper(trim(coalesce(c.legal_name, c.name)))
 WHERE c.current_pe_owner = :pe_owner
 ORDER BY c.id, p.npi;

-- name: CQ25
-- A hospital's overall and domain star ratings.
SELECT facility_id AS ccn,
       overall_rating,
       mortality_rating,
       readmission_rating,
       patient_experience_rating,
       effectiveness_rating,
       timeliness_rating,
       imaging_rating
  FROM cms_hospitals
 WHERE facility_id = :ccn;
