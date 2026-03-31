"""
MetroProfileIngestor — orchestrates all four data sources and upserts into metro_profiles.

Order of operations:
  1. Seed metro_reference from static CBSA list
  2. Fetch Census BPS permit data (last 3 years, use most recent available)
  3. Fetch FHFA HPI (single download, all MSAs)
  4. Fetch Census ACS (CBSA level, most recent 5-year vintage)
  5. Fetch BLS LAUS unemployment (metros with known area codes)
  6. Join all sources by cbsa_code, compute derived scores
  7. null_preserving_upsert into metro_profiles
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.batch_operations import bulk_upsert
from app.core.models import MetroProfile, MetroReference
from app.services.metro_profile_service import MetroProfileService
from app.sources.metro.cbsa_reference import get_cbsa_list
from app.sources.metro.client import MetroDataClient

logger = logging.getLogger(__name__)


class MetroProfileIngestor:
    """Orchestrates ingestion of metro development characteristics from 4 federal sources."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self.client = MetroDataClient()
        self.service = MetroProfileService()

    async def run(self, vintage: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the full metro profiles ingestion pipeline.

        Args:
            vintage: Data vintage label, e.g. "2024". Defaults to current year.

        Returns:
            Summary dict with counts per source and total upserted.
        """
        if vintage is None:
            vintage = str(datetime.utcnow().year - 1)  # use prior year (data lags)

        summary: Dict[str, Any] = {
            "vintage": vintage,
            "metros_seeded": 0,
            "bps_records": 0,
            "fhfa_records": 0,
            "acs_records": 0,
            "laus_records": 0,
            "profiles_upserted": 0,
            "errors": [],
        }

        try:
            # Step 1: Seed metro_reference
            summary["metros_seeded"] = self._seed_metro_reference()
            logger.info(f"Metro reference: {summary['metros_seeded']} CBSAs seeded")

            # Step 2: Census BPS permits
            bps_data = await self._fetch_bps(int(vintage))
            summary["bps_records"] = len(bps_data)
            logger.info(f"BPS: {len(bps_data)} metro permit records")

            # Step 3: FHFA HPI
            fhfa_data = await self._fetch_fhfa()
            summary["fhfa_records"] = len(fhfa_data)
            logger.info(f"FHFA: {len(fhfa_data)} MSA HPI records")

            # Step 4: Census ACS
            acs_data = await self._fetch_acs()
            summary["acs_records"] = len(acs_data)
            logger.info(f"ACS: {len(acs_data)} CBSA records")

            # Step 5: BLS LAUS
            all_cbsa_codes = [r["cbsa_code"] for r in get_cbsa_list()]
            laus_data = await self._fetch_laus(all_cbsa_codes)
            summary["laus_records"] = len(laus_data)
            logger.info(f"LAUS: {len(laus_data)} metro unemployment records")

            # Step 6: Build joined profiles per CBSA
            profiles = self._join_sources(
                vintage=vintage,
                bps=bps_data,
                fhfa=fhfa_data,
                acs=acs_data,
                laus=laus_data,
            )
            logger.info(f"Joined {len(profiles)} metro profiles")

            # Step 7: Compute derived scores across all profiles
            profiles = self.service.compute_scores(profiles)

            # Step 8: Upsert
            upserted = self._upsert_profiles(profiles)
            summary["profiles_upserted"] = upserted
            logger.info(f"Upserted {upserted} metro profiles (vintage={vintage})")

        except Exception as e:
            logger.error(f"Metro profile ingest failed: {e}", exc_info=True)
            summary["errors"].append(str(e))

        finally:
            await self.client.close()

        return summary

    # -------------------------------------------------------------------------
    # Private helpers
    # -------------------------------------------------------------------------

    def _seed_metro_reference(self) -> int:
        """Upsert CBSA reference records. Returns count of records processed."""
        now = datetime.utcnow()
        cbsas = get_cbsa_list()
        rows = [
            {
                "cbsa_code": r["cbsa_code"],
                "cbsa_name": r["cbsa_name"],
                "metro_type": r["metro_type"],
                "state_abbr": r["state_abbr"],
                "population_rank": r["population_rank"],
                "created_at": now,
            }
            for r in cbsas
        ]
        value_cols = ["cbsa_name", "metro_type", "state_abbr", "population_rank", "created_at"]
        bulk_upsert(
            db=self.db,
            table_name="metro_reference",
            rows=rows,
            key_columns=["cbsa_code"],
            value_columns=value_cols,
        )
        self.db.commit()
        return len(rows)

    async def _fetch_bps(self, year: int) -> Dict[str, Dict[str, Any]]:
        """Try current year and two prior years; use first that returns data."""
        for y in [year, year - 1, year - 2]:
            try:
                records = await self.client.fetch_bps_metro(y)
                if records:
                    # Index by cbsa_code
                    return {r["cbsa_code"]: r for r in records}
            except Exception as e:
                logger.warning(f"BPS fetch failed for year {y}: {e}")
        return {}

    async def _fetch_fhfa(self) -> Dict[str, Dict[str, Any]]:
        try:
            return await self.client.fetch_fhfa_msa()
        except Exception as e:
            logger.error(f"FHFA fetch failed: {e}")
            return {}

    async def _fetch_acs(self) -> Dict[str, Dict[str, Any]]:
        for year in [2023, 2022, 2021]:
            try:
                data = await self.client.fetch_acs_cbsa(year=year)
                if data:
                    return data
            except Exception as e:
                logger.warning(f"ACS fetch failed for year {year}: {e}")
        return {}

    async def _fetch_laus(self, cbsa_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        try:
            return await self.client.fetch_bls_laus_metro(cbsa_codes)
        except Exception as e:
            logger.error(f"BLS LAUS fetch failed: {e}")
            return {}

    def _join_sources(
        self,
        vintage: str,
        bps: Dict[str, Dict[str, Any]],
        fhfa: Dict[str, Dict[str, Any]],
        acs: Dict[str, Dict[str, Any]],
        laus: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Join all four sources by cbsa_code into a list of profile dicts.
        Missing sources result in NULL columns for that source's fields.
        """
        # Only include CBSAs that are in metro_reference — FK constraint enforces this.
        # ACS/FHFA return many more codes; restrict to our curated reference list.
        all_codes = set(r["cbsa_code"] for r in get_cbsa_list())

        profiles = []
        for cbsa_code in sorted(all_codes):
            bps_rec = bps.get(cbsa_code, {})
            fhfa_rec = fhfa.get(cbsa_code, {})
            acs_rec = acs.get(cbsa_code, {})
            laus_rec = laus.get(cbsa_code, {})

            # Track which sources contributed
            sources_available = []
            if bps_rec:
                sources_available.append("bps")
            if fhfa_rec:
                sources_available.append("fhfa")
            if acs_rec:
                sources_available.append("acs")
            if laus_rec:
                sources_available.append("laus")

            completeness = round(len(sources_available) / 4 * 100, 1)

            # Compute permits_per_1000_units and multifamily_share_pct
            housing_units = acs_rec.get("housing_units_total")
            permits_total = bps_rec.get("permits_total")
            permits_5plus = bps_rec.get("permits_5plus")

            permits_per_1000: Optional[float] = None
            if permits_total is not None and housing_units and housing_units > 0:
                permits_per_1000 = round(permits_total / housing_units * 1000, 2)

            mf_share: Optional[float] = None
            if permits_5plus is not None and permits_total and permits_total > 0:
                mf_share = round(permits_5plus / permits_total * 100, 2)

            profiles.append({
                "cbsa_code": cbsa_code,
                "data_vintage": vintage,
                # BPS
                "permits_total": permits_total,
                "permits_1unit": bps_rec.get("permits_1unit"),
                "permits_2to4": bps_rec.get("permits_3to4"),
                "permits_5plus": permits_5plus,
                "permits_per_1000_units": permits_per_1000,
                "multifamily_share_pct": mf_share,
                # FHFA
                "hpi_current": fhfa_rec.get("hpi_current"),
                "hpi_yoy_pct": fhfa_rec.get("hpi_yoy_pct"),
                "hpi_5yr_pct": fhfa_rec.get("hpi_5yr_pct"),
                # ACS
                "population": acs_rec.get("population"),
                "median_hh_income": acs_rec.get("median_hh_income"),
                "housing_units_total": housing_units,
                "cost_burden_severe_pct": acs_rec.get("cost_burden_severe_pct"),
                # LAUS
                "unemployment_rate": laus_rec.get("unemployment_rate"),
                "labor_force_size": laus_rec.get("labor_force_size"),
                # Scores filled in by service
                "permit_velocity_score": None,
                "multifamily_score": None,
                "supply_elasticity_score": None,
                "build_hostility_score": None,
                "build_hostility_grade": None,
                # Metadata
                "sources_available": sources_available,
                "data_completeness_pct": completeness,
            })

        return profiles

    def _upsert_profiles(self, profiles: List[Dict[str, Any]]) -> int:
        """Upsert all profiles into metro_profiles table."""
        if not profiles:
            return 0

        now = datetime.utcnow()
        key_cols = ["cbsa_code", "data_vintage"]
        value_cols = [
            "permits_total", "permits_1unit", "permits_2to4", "permits_5plus",
            "permits_per_1000_units", "multifamily_share_pct",
            "hpi_current", "hpi_yoy_pct", "hpi_5yr_pct",
            "population", "median_hh_income", "housing_units_total", "cost_burden_severe_pct",
            "unemployment_rate", "labor_force_size",
            "permit_velocity_score", "multifamily_score", "supply_elasticity_score",
            "build_hostility_score", "build_hostility_grade",
            "sources_available", "data_completeness_pct", "updated_at",
        ]
        all_cols = key_cols + value_cols

        # Keep only known columns, fill missing with None; inject updated_at
        # sources_available must be JSON string (column is JSON type; bulk_upsert uses raw SQL)
        clean = []
        for p in profiles:
            row = {c: p.get(c) for c in all_cols}
            row["updated_at"] = now
            sa = row.get("sources_available")
            row["sources_available"] = json.dumps(sa) if sa is not None else None
            clean.append(row)

        bulk_upsert(
            db=self.db,
            table_name="metro_profiles",
            rows=clean,
            key_columns=key_cols,
            value_columns=value_cols,
        )
        self.db.commit()
        return len(clean)
