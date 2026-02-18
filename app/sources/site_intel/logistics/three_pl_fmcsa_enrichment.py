"""
3PL Company Enrichment Collector - Phase 3: FMCSA Cross-Reference.

Matches 3PL companies to FMCSA motor carrier records to enrich:
- HQ city/state (from physical address)
- Asset-based flag (from power_units count)
- Cold chain flag (from cargo_carried containing "Refrigerated")
- Hazmat flag (from cargo_carried containing "Hazardous")

First checks local motor_carrier table, then falls back to FMCSA web API.
"""

import logging
import re
from datetime import datetime
from typing import Optional, Dict, Any, List


from app.core.models_site_intel import ThreePLCompany, MotorCarrier
from app.sources.site_intel.base_collector import BaseCollector
from app.sources.site_intel.types import (
    SiteIntelDomain,
    SiteIntelSource,
    CollectionConfig,
    CollectionResult,
    CollectionStatus,
)
from app.sources.site_intel.runner import register_collector

logger = logging.getLogger(__name__)

# Words to strip from company names for fuzzy matching
STRIP_WORDS = {
    "inc",
    "llc",
    "corp",
    "corporation",
    "company",
    "co",
    "ltd",
    "worldwide",
    "international",
    "intl",
    "group",
    "holdings",
    "services",
    "logistics",
    "transport",
    "transportation",
    "freight",
    "express",
    "lines",
    "system",
    "systems",
    "the",
    "of",
    "and",
}


def normalize_name(name: str) -> str:
    """Normalize company name for matching."""
    name = name.lower()
    name = re.sub(r"[^\w\s]", " ", name)  # Remove punctuation
    tokens = name.split()
    tokens = [t for t in tokens if t not in STRIP_WORDS]
    return " ".join(tokens).strip()


def name_tokens(name: str) -> set:
    """Get token set from a name."""
    return set(normalize_name(name).split())


def jaccard_similarity(set_a: set, set_b: set) -> float:
    """Calculate Jaccard similarity between two sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0


@register_collector(SiteIntelSource.THREE_PL_FMCSA)
class ThreePLFMCSAEnrichmentCollector(BaseCollector):
    """
    Enrichment collector that cross-references 3PL companies with FMCSA data.

    Matching strategy:
    1. Exact normalized name match in local motor_carrier table
    2. Jaccard token overlap > 0.6
    3. Fallback to FMCSA web API for companies not found locally
    """

    domain = SiteIntelDomain.LOGISTICS
    source = SiteIntelSource.THREE_PL_FMCSA

    rate_limit_delay = 0.5  # FMCSA API rate limit

    def get_default_base_url(self) -> str:
        return "https://mobile.fmcsa.dot.gov"

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "Nexdata-SiteIntel/1.0",
            "Accept": "application/json",
        }

    async def collect(self, config: CollectionConfig) -> CollectionResult:
        """Cross-reference 3PL companies with FMCSA motor carrier data."""
        try:
            # Load all 3PL companies from DB
            three_pl_companies = self.db.query(ThreePLCompany).all()
            if not three_pl_companies:
                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=0,
                    error_message="No 3PL companies in database to enrich",
                )

            # Load all motor carriers from local DB
            motor_carriers = (
                self.db.query(MotorCarrier).filter(MotorCarrier.is_active == True).all()
            )

            logger.info(
                f"Cross-referencing {len(three_pl_companies)} 3PLs "
                f"against {len(motor_carriers)} FMCSA carriers"
            )

            # Build carrier lookup by normalized name
            carrier_by_name: Dict[str, MotorCarrier] = {}
            carrier_tokens: List[tuple] = []
            for mc in motor_carriers:
                norm = normalize_name(mc.legal_name)
                carrier_by_name[norm] = mc
                carrier_tokens.append((name_tokens(mc.legal_name), mc))
                if mc.dba_name:
                    dba_norm = normalize_name(mc.dba_name)
                    carrier_by_name[dba_norm] = mc
                    carrier_tokens.append((name_tokens(mc.dba_name), mc))

            records = []
            errors = []
            matched_count = 0

            for company in three_pl_companies:
                try:
                    # Try local DB match first
                    match = self._find_local_match(
                        company.company_name, carrier_by_name, carrier_tokens
                    )

                    # Fallback to FMCSA API if no local match
                    if not match:
                        match = await self._query_fmcsa_api(company.company_name)

                    if match:
                        record = self._build_enrichment_record(
                            company.company_name, match
                        )
                        if record:
                            records.append(record)
                            matched_count += 1

                except Exception as e:
                    logger.debug(f"FMCSA match failed for {company.company_name}: {e}")
                    errors.append(
                        {
                            "company": company.company_name,
                            "error": str(e),
                        }
                    )

            logger.info(
                f"Matched {matched_count}/{len(three_pl_companies)} companies to FMCSA records"
            )

            if records:
                inserted, updated = self.null_preserving_upsert(
                    ThreePLCompany,
                    records,
                    unique_columns=["company_name"],
                    update_columns=[
                        "headquarters_city",
                        "headquarters_state",
                        "is_asset_based",
                        "has_cold_chain",
                        "has_hazmat",
                        "source",
                        "collected_at",
                    ],
                )

                return self.create_result(
                    status=CollectionStatus.SUCCESS,
                    total=len(three_pl_companies),
                    processed=len(three_pl_companies),
                    inserted=inserted,
                    updated=updated,
                    failed=len(errors),
                    errors=errors if errors else None,
                    sample=records[:3],
                )

            return self.create_result(
                status=CollectionStatus.SUCCESS,
                total=len(three_pl_companies),
                processed=len(three_pl_companies),
                inserted=0,
            )

        except Exception as e:
            logger.error(f"FMCSA enrichment failed: {e}", exc_info=True)
            return self.create_result(
                status=CollectionStatus.FAILED,
                error_message=str(e),
            )
        finally:
            await self.close_client()

    def _find_local_match(
        self,
        company_name: str,
        carrier_by_name: Dict[str, "MotorCarrier"],
        carrier_tokens: List[tuple],
    ) -> Optional[Dict[str, Any]]:
        """Find a matching motor carrier in the local DB."""
        norm_name = normalize_name(company_name)

        # Exact normalized name match
        if norm_name in carrier_by_name:
            mc = carrier_by_name[norm_name]
            return self._carrier_to_dict(mc)

        # Jaccard token overlap
        company_tokens = name_tokens(company_name)
        best_score = 0.0
        best_match = None

        for tokens, mc in carrier_tokens:
            score = jaccard_similarity(company_tokens, tokens)
            if score > best_score:
                best_score = score
                best_match = mc

        if best_score >= 0.6 and best_match:
            logger.debug(
                f"Jaccard match: {company_name} -> {best_match.legal_name} "
                f"(score={best_score:.2f})"
            )
            return self._carrier_to_dict(best_match)

        return None

    async def _query_fmcsa_api(self, company_name: str) -> Optional[Dict[str, Any]]:
        """Query the FMCSA web API for a company name."""
        try:
            # URL-encode the company name
            import urllib.parse

            encoded_name = urllib.parse.quote(company_name)

            await self.apply_rate_limit()
            client = await self.get_client()

            response = await client.get(
                f"/qc/services/carriers/name/{encoded_name}",
                params={"webKey": ""},  # Public API, no key needed
            )

            if response.status_code != 200:
                return None

            data = response.json()
            content = data.get("content", [])

            if not content:
                return None

            # Take the first active result
            for carrier in content:
                carrier_data = carrier.get("carrier", {})
                if carrier_data.get("allowedToOperate") == "Y":
                    return {
                        "physical_city": carrier_data.get("phyCity"),
                        "physical_state": carrier_data.get("phyState"),
                        "power_units": carrier_data.get("totalPowerUnits"),
                        "drivers": carrier_data.get("totalDrivers"),
                        "cargo_carried": self._parse_api_cargo(carrier_data),
                    }

            return None

        except Exception as e:
            logger.debug(f"FMCSA API query failed for {company_name}: {e}")
            return None

    def _parse_api_cargo(self, carrier_data: Dict[str, Any]) -> List[str]:
        """Parse cargo types from FMCSA API response."""
        cargo = []
        cargo_fields = {
            "cargoGeneralFreight": "General Freight",
            "cargoHouseholdGoods": "Household Goods",
            "cargoMetalSheets": "Metal/Sheets/Coils",
            "cargoMotorVehicles": "Motor Vehicles",
            "cargoDriveTowAway": "Drive/Tow Away",
            "cargoLogs": "Logs/Poles/Lumber",
            "cargoBuildingMaterials": "Building Materials",
            "cargoMobileHomes": "Mobile Homes",
            "cargoMachinery": "Machinery/Large Objects",
            "cargoFreshProduce": "Fresh Produce",
            "cargoLiquids": "Liquids/Gases",
            "cargoIntermodal": "Intermodal Containers",
            "cargoPassengers": "Passengers",
            "cargoOilfield": "Oilfield Equipment",
            "cargoLivestock": "Livestock",
            "cargoGrain": "Grain/Feed/Hay",
            "cargoCoal": "Coal/Coke",
            "cargoMeat": "Meat",
            "cargoGarbage": "Garbage/Refuse",
            "cargoUSMail": "US Mail",
            "cargoChemicals": "Chemicals",
            "cargoCommodities": "Commodities Dry Bulk",
            "cargoRefrigerated": "Refrigerated Food",
            "cargoBeverages": "Beverages",
            "cargoPaper": "Paper Products",
            "cargoUtilities": "Utilities",
            "cargoFarmSupplies": "Agricultural/Farm Supplies",
            "cargoConstruction": "Construction",
            "cargoWaterWell": "Water Well",
        }

        for field, label in cargo_fields.items():
            if carrier_data.get(field) == "Y":
                cargo.append(label)

        return cargo

    def _carrier_to_dict(self, mc: "MotorCarrier") -> Dict[str, Any]:
        """Convert a MotorCarrier ORM object to a dict."""
        return {
            "physical_city": mc.physical_city,
            "physical_state": mc.physical_state,
            "power_units": mc.power_units,
            "drivers": mc.drivers,
            "cargo_carried": mc.cargo_carried if mc.cargo_carried else [],
        }

    def _build_enrichment_record(
        self, company_name: str, carrier_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Build an enrichment record from FMCSA carrier data."""
        record = {"company_name": company_name}

        city = carrier_data.get("physical_city")
        state = carrier_data.get("physical_state")
        if city:
            record["headquarters_city"] = city.title() if city else None
        if state:
            record["headquarters_state"] = state.upper() if state else None

        # Asset-based if they have 100+ power units
        power_units = carrier_data.get("power_units")
        if power_units and int(power_units) >= 100:
            record["is_asset_based"] = True

        # Check cargo carried for specializations
        cargo = carrier_data.get("cargo_carried", [])
        if isinstance(cargo, list):
            cargo_text = " ".join(str(c) for c in cargo).lower()

            if "refrigerat" in cargo_text or "fresh produce" in cargo_text:
                record["has_cold_chain"] = True

            if "hazardous" in cargo_text or "chemicals" in cargo_text:
                record["has_hazmat"] = True

        # Only return if we got meaningful data
        if len(record) > 1:
            record["source"] = "fmcsa"
            record["collected_at"] = datetime.utcnow()
            return record

        return None
