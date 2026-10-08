-- SPEC_162: one read-only probe per competency question (PLAN_100 Appendix B).
-- Each block returns ONE row of named metrics; probes.py turns it into a label
-- (HOLD / PARTIAL / NEED / SCHEMA) with a fixed rule. Works on PostgreSQL 14,
-- before and after alembic 0022 (no probe reads data_year). Never writes.
-- Existence probes read information_schema only, so a missing table is a 0, not an error.
-- Schema filter for existence probes: public and core (workbench, demo, quarantine excluded).

-- name: CQ01
SELECT count(*) AS n,
       count(entity_type) AS has_type,
       count(enumeration_date) AS has_enum_date,
       count(last_updated) AS has_last_updated,
       count(dba_name) AS has_dba,
       count(*) FILTER (WHERE status IS DISTINCT FROM 'A') AS non_active
  FROM nppes_providers;

-- name: CQ02
SELECT count(*) FILTER (WHERE entity_type = '1') AS n_individual,
       count(*) FILTER (WHERE entity_type = '1' AND taxonomy_code IS NOT NULL) AS has_taxonomy,
       count(*) FILTER (WHERE entity_type = '1' AND credential IS NOT NULL) AS has_credential,
       count(*) FILTER (WHERE entity_type = '1' AND taxonomy_state IS NOT NULL) AS has_license_state
  FROM nppes_providers;

-- name: CQ03
SELECT count(DISTINCT c.table_schema || '.' || c.table_name) AS multi_taxonomy_tables
  FROM information_schema.columns c
 WHERE c.table_schema IN ('public', 'core')
   AND c.column_name ~ 'taxonomy'
   AND EXISTS (SELECT 1 FROM information_schema.columns d
                WHERE d.table_schema = c.table_schema AND d.table_name = c.table_name
                  AND d.column_name ~ '(primary_taxonomy|taxonomy_switch|is_primary|taxonomy_slot)');

-- name: CQ04
SELECT count(*) AS nucc_tables
  FROM (SELECT table_schema, table_name
          FROM information_schema.columns
         WHERE table_schema IN ('public', 'core', 'ref')
           AND column_name IN ('grouping', 'classification', 'specialization')
         GROUP BY 1, 2
        HAVING count(DISTINCT column_name) = 3) t;

-- name: CQ05
WITH org_addr AS (
    SELECT DISTINCT upper(trim(practice_address_line1)) AS a, left(practice_zip, 5) AS z
      FROM nppes_providers
     WHERE entity_type = '2' AND practice_address_line1 IS NOT NULL),
org_phone AS (
    SELECT DISTINCT practice_phone AS p
      FROM nppes_providers
     WHERE entity_type = '2' AND practice_phone IS NOT NULL),
ind AS (
    SELECT npi, upper(trim(practice_address_line1)) AS a, left(practice_zip, 5) AS z, practice_phone AS p
      FROM nppes_providers
     WHERE entity_type = '1')
-- two hash joins and a UNION, not an OR of correlated EXISTS (that timed out live)
SELECT (SELECT count(*) FROM ind) AS n_individual,
       (SELECT count(*) FROM (
            SELECT ind.npi FROM ind JOIN org_addr o ON o.a = ind.a AND o.z = ind.z
            UNION
            SELECT ind.npi FROM ind JOIN org_phone o ON o.p = ind.p) s) AS sharing_individuals;

-- name: CQ06
SELECT count(*) AS reassignment_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(reassign|pecos|dac_)';

-- name: CQ07
SELECT count(*) AS affiliation_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(affiliat|dac_)';

-- name: CQ08
SELECT count(*) FILTER (WHERE entity_type = '2') AS n_org,
       count(*) FILTER (WHERE entity_type = '2' AND organization_subpart IS NOT NULL) AS has_subpart_flag,
       (SELECT count(*) FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = 'nppes_providers'
           AND column_name ~ 'parent') AS parent_columns
  FROM nppes_providers;

-- name: CQ09
SELECT count(DISTINCT c.table_name) AS npi_tables_with_tin
  FROM information_schema.columns c
 WHERE c.table_schema = 'public'
   AND c.column_name IN ('ein', 'tin', 'parent_organization_tin', 'parent_organization_ein')
   AND EXISTS (SELECT 1 FROM information_schema.columns d
                WHERE d.table_schema = c.table_schema AND d.table_name = c.table_name
                  AND d.column_name ~ '(^|_)npi$');

-- name: CQ10
SELECT count(*) AS crosswalk_rows
  FROM cms_hospital_cost_reports
 WHERE npi IS NOT NULL AND prvdr_num IS NOT NULL;

-- name: CQ11
SELECT count(*) AS n,
       count(practice_zip) AS has_zip,
       count(practice_state) AS has_state,
       count(r.npi) AS has_ruca,
       (SELECT count(*) FROM information_schema.tables
         WHERE table_schema IN ('public', 'core') AND table_name ~ '(zip.*county|county.*zip|hud_.*crosswalk|zip_crosswalk)') AS crosswalk_tables
  FROM nppes_providers p
  LEFT JOIN (SELECT DISTINCT rndrng_npi AS npi
               FROM cms_medicare_utilization
              WHERE rndrng_prvdr_ruca IS NOT NULL) r ON r.npi = p.npi;

-- name: CQ12
SELECT count(*) AS n,
       count(DISTINCT taxonomy_code) AS taxonomies,
       count(DISTINCT left(practice_zip, 5)) AS zips
  FROM nppes_providers;

-- name: CQ13
SELECT count(*) AS practice_location_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(practice_location|secondary_location|npi_location)';

-- name: CQ14
SELECT count(*) AS n,
       count(hospital_type) AS has_type,
       count(emergency_services) AS has_emergency,
       count(county) AS has_county
  FROM cms_hospitals;

-- name: CQ15
SELECT count(DISTINCT rndrng_npi) AS npis,
       count(DISTINCT rndrng_npi) FILTER (WHERE rndrng_prvdr_mdcr_prtcptg_ind IN ('Y', 'N')) AS has_assignment_flag
  FROM cms_medicare_utilization;

-- name: CQ16
SELECT count(*) AS network_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(plan_net|insurance_plan|^tic_|negotiated_rate|in_network)';

-- name: CQ17
SELECT count(*) AS medicaid_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ 'medicaid';

-- name: CQ18
SELECT count(*) AS n,
       count(DISTINCT rndrng_npi) AS npis,
       count(avg_mdcr_alowd_amt) AS has_allowed,
       count(avg_mdcr_pymt_amt) AS has_paid,
       (SELECT count(*) FROM pg_constraint WHERE conname = 'uq_cms_medicare_utilization_year_key') AS has_year_key
  FROM cms_medicare_utilization;

-- name: CQ19
SELECT count(DISTINCT rndrng_npi) AS npis,
       count(DISTINCT rndrng_prvdr_type) AS provider_types,
       count(DISTINCT rndrng_prvdr_state_abrvtn) AS states,
       count(DISTINCT place_of_srvc) AS places
  FROM cms_medicare_utilization;

-- name: CQ20
SELECT count(*) AS n,
       count(bed_cnt) AS has_beds,
       count(tot_costs) AS has_costs,
       count(net_income) AS has_net_income
  FROM cms_hospital_cost_reports;

-- name: CQ21
SELECT count(*) AS prescriber_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(prescriber|part_d_prov|partd_prov)';

-- name: CQ22
SELECT count(*) AS owner_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(^cms_.*owner|all_owners|hospital_owner|snf_owner|hha_owner|hospice_owner)';

-- name: CQ23
SELECT (SELECT count(*) FROM pe_portfolio_companies) AS portfolio_companies,
       (SELECT count(*) FROM information_schema.columns
         WHERE table_schema IN ('public', 'core') AND table_name ~ 'portfolio' AND column_name ~ '(^|_)npi$') AS npi_link_columns,
       count(DISTINCT c.id) AS name_matched_companies
  FROM pe_portfolio_companies c
  JOIN nppes_providers p
    ON p.entity_type = '2'
   AND upper(trim(p.legal_name)) = upper(trim(coalesce(c.legal_name, c.name)));

-- name: CQ24
SELECT count(*) AS change_history_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(chow|change_of_ownership|nppes_.*snapshot|provider_history)';

-- name: CQ25
SELECT count(*) AS n,
       count(overall_rating) AS has_overall,
       count(mortality_rating) AS has_domain
  FROM cms_hospitals;

-- name: CQ26
SELECT count(*) AS exclusion_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(leie|exclusion|debar)';

-- name: CQ27
SELECT count(*) AS payment_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '(open_payment|sunshine|general_payment)';

-- name: CQ28
SELECT count(*) AS patient_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '^(patient|person|persons)$';

-- name: CQ29
SELECT count(*) AS encounter_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '^(encounter|encounters|visit_occurrence|condition_occurrence)$';

-- name: CQ30
SELECT count(*) AS coverage_tables
  FROM information_schema.tables
 WHERE table_schema IN ('public', 'core') AND table_name ~ '^(coverage|payer_plan_period)$';
