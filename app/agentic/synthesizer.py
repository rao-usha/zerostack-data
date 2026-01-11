"""
Data Synthesizer for merging and deduplicating portfolio findings.

Combines findings from multiple sources, deduplicates companies,
and merges records to create high-quality, validated data.
"""
import logging
import re
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# Source priority for merging (higher = more authoritative)
SOURCE_PRIORITY = {
    "sec_13f": 5,        # Regulatory filing - highest confidence
    "annual_report": 4,  # Official publication
    "portfolio_company_website": 4,  # Company confirms relationship
    "website": 3,        # Official investor website
    "press_release": 2,  # Public announcement
    "news": 1,           # News coverage
}


class DataSynthesizer:
    """
    Synthesizes portfolio findings from multiple sources.
    
    Key functions:
    - Deduplicate companies across sources
    - Merge records from multiple sources
    - Normalize company names
    - Calculate confidence based on source agreement
    """
    
    def __init__(self):
        """Initialize the synthesizer."""
        self._name_cache: Dict[str, str] = {}  # Normalized name cache
    
    def synthesize_findings(
        self, 
        all_findings: List[Dict[str, Any]],
        investor_id: int,
        investor_type: str
    ) -> List[Dict[str, Any]]:
        """
        Combine findings from multiple sources, deduplicate, and merge.
        
        Args:
            all_findings: List of company records from various strategies
            investor_id: The investor ID to associate with records
            investor_type: 'lp' or 'family_office'
            
        Returns:
            List of deduplicated, merged company records
        """
        logger.info(f"Synthesizing {len(all_findings)} findings")
        
        if not all_findings:
            return []
        
        # Group by normalized company name
        seen_companies: Dict[str, Dict[str, Any]] = {}
        
        for finding in all_findings:
            company_name = finding.get("company_name", "")
            if not company_name:
                continue
            
            # Normalize the company name
            company_key = self.normalize_company_name(company_name)
            
            if company_key in seen_companies:
                # Merge with existing record
                existing = seen_companies[company_key]
                merged = self.merge_records(existing, finding)
                seen_companies[company_key] = merged
            else:
                # New company - add investor context
                finding["investor_id"] = investor_id
                finding["investor_type"] = investor_type
                seen_companies[company_key] = finding
        
        result = list(seen_companies.values())
        logger.info(f"Synthesized to {len(result)} unique companies")
        
        return result
    
    def normalize_company_name(self, name: str) -> str:
        """
        Normalize a company name for comparison/deduplication.
        
        Args:
            name: Company name to normalize
            
        Returns:
            Normalized company name key
        """
        if name in self._name_cache:
            return self._name_cache[name]
        
        normalized = name.lower().strip()
        
        # Remove common suffixes
        suffixes = [
            r',?\s*(inc\.?|incorporated)$',
            r',?\s*(llc|l\.l\.c\.)$',
            r',?\s*(ltd\.?|limited)$',
            r',?\s*(corp\.?|corporation)$',
            r',?\s*(co\.?|company)$',
            r',?\s*(plc|p\.l\.c\.)$',
            r',?\s*(s\.a\.?|sa)$',
            r',?\s*(n\.v\.?|nv)$',
            r',?\s*(ag)$',
            r',?\s*(gmbh)$',
        ]
        
        for suffix in suffixes:
            normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)
        
        # Remove punctuation
        normalized = re.sub(r'[^\w\s]', '', normalized)
        
        # Normalize whitespace
        normalized = ' '.join(normalized.split())
        
        # Cache the result
        self._name_cache[name] = normalized
        
        return normalized
    
    def merge_records(
        self, 
        record1: Dict[str, Any], 
        record2: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Merge two records about the same company from different sources.
        
        Uses source priority to determine which data to keep.
        
        Args:
            record1: First record
            record2: Second record
            
        Returns:
            Merged record with best data from both sources
        """
        source1 = record1.get("source_type", "unknown")
        source2 = record2.get("source_type", "unknown")
        
        priority1 = SOURCE_PRIORITY.get(source1, 0)
        priority2 = SOURCE_PRIORITY.get(source2, 0)
        
        # Use data from higher priority source as primary
        if priority1 >= priority2:
            primary = record1
            secondary = record2
        else:
            primary = record2
            secondary = record1
        
        # Start with primary record
        merged = dict(primary)
        
        # Fill missing fields from secondary
        fill_fields = [
            "investment_date",
            "investment_amount_usd",
            "company_industry",
            "company_website",
            "company_stage",
            "company_location",
            "shares_held",
            "market_value_usd",
            "ownership_percentage",
        ]
        
        for field in fill_fields:
            if not merged.get(field) and secondary.get(field):
                merged[field] = secondary[field]
        
        # Combine source URLs for provenance
        source_urls = []
        if primary.get("source_url"):
            source_urls.append(primary["source_url"])
        if secondary.get("source_url") and secondary["source_url"] not in source_urls:
            source_urls.append(secondary["source_url"])
        
        if len(source_urls) > 1:
            merged["source_urls"] = source_urls
        
        # Upgrade confidence if multiple sources agree
        if source1 != source2 and priority1 > 0 and priority2 > 0:
            merged["confidence_level"] = "high"
            merged["agent_reasoning"] = f"Confirmed by multiple sources: {source1}, {source2}"
        
        return merged
    
    def extract_co_investors(
        self,
        all_findings: List[Dict[str, Any]],
        primary_investor_id: int,
        primary_investor_type: str
    ) -> List[Dict[str, Any]]:
        """
        Extract co-investor relationships from findings.
        
        Args:
            all_findings: List of company records that may contain co-investor info
            primary_investor_id: The main investor's ID
            primary_investor_type: 'lp' or 'family_office'
            
        Returns:
            List of co-investment records
        """
        co_investors: Dict[str, Dict[str, Any]] = {}
        
        for finding in all_findings:
            # Check for co_investors field
            finding_co_investors = finding.get("co_investors", [])
            deal_name = finding.get("company_name")
            deal_date = finding.get("investment_date")
            
            for co_investor in finding_co_investors:
                if isinstance(co_investor, str):
                    co_name = co_investor
                elif isinstance(co_investor, dict):
                    co_name = co_investor.get("name", "")
                else:
                    continue
                
                if not co_name:
                    continue
                
                # Normalize co-investor name
                co_key = self.normalize_company_name(co_name)
                
                if co_key in co_investors:
                    # Increment count
                    co_investors[co_key]["co_investment_count"] += 1
                else:
                    # New co-investor
                    co_investors[co_key] = {
                        "primary_investor_id": primary_investor_id,
                        "primary_investor_type": primary_investor_type,
                        "co_investor_name": co_name,
                        "deal_name": deal_name,
                        "deal_date": deal_date,
                        "co_investment_count": 1,
                        "source_type": finding.get("source_type"),
                        "source_url": finding.get("source_url"),
                    }
        
        return list(co_investors.values())
    
    def classify_investment_themes(
        self,
        companies: List[Dict[str, Any]],
        investor_id: int,
        investor_type: str
    ) -> List[Dict[str, Any]]:
        """
        Classify investment themes based on portfolio companies.
        
        Args:
            companies: List of portfolio company records
            investor_id: The investor's ID
            investor_type: 'lp' or 'family_office'
            
        Returns:
            List of theme classification records
        """
        themes: Dict[str, Dict[str, Any]] = {}
        
        # Count industries
        industry_counts: Dict[str, int] = {}
        stage_counts: Dict[str, int] = {}
        location_counts: Dict[str, int] = {}
        
        for company in companies:
            industry = company.get("company_industry")
            if industry:
                industry = self._normalize_industry(industry)
                industry_counts[industry] = industry_counts.get(industry, 0) + 1
            
            stage = company.get("company_stage")
            if stage:
                stage = stage.lower()
                stage_counts[stage] = stage_counts.get(stage, 0) + 1
            
            location = company.get("company_location")
            if location:
                region = self._extract_region(location)
                if region:
                    location_counts[region] = location_counts.get(region, 0) + 1
        
        total_companies = len(companies) if companies else 1
        
        # Create theme records for sectors
        for industry, count in industry_counts.items():
            pct = (count / total_companies) * 100
            if count >= 2 or pct >= 10:  # Threshold for significance
                theme_key = f"sector:{industry}"
                themes[theme_key] = {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "theme_category": "sector",
                    "theme_value": industry,
                    "investment_count": count,
                    "percentage_of_portfolio": f"{pct:.1f}",
                    "confidence_level": "high" if count >= 5 else "medium",
                }
        
        # Create theme records for stages
        for stage, count in stage_counts.items():
            pct = (count / total_companies) * 100
            if count >= 2 or pct >= 10:
                theme_key = f"stage:{stage}"
                themes[theme_key] = {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "theme_category": "stage",
                    "theme_value": stage,
                    "investment_count": count,
                    "percentage_of_portfolio": f"{pct:.1f}",
                    "confidence_level": "medium",
                }
        
        # Create theme records for geography
        for region, count in location_counts.items():
            pct = (count / total_companies) * 100
            if count >= 2 or pct >= 10:
                theme_key = f"geography:{region}"
                themes[theme_key] = {
                    "investor_id": investor_id,
                    "investor_type": investor_type,
                    "theme_category": "geography",
                    "theme_value": region,
                    "investment_count": count,
                    "percentage_of_portfolio": f"{pct:.1f}",
                    "confidence_level": "medium",
                }
        
        return list(themes.values())
    
    def _normalize_industry(self, industry: str) -> str:
        """Normalize industry name to standard categories."""
        industry_lower = industry.lower()
        
        # Map to standard categories
        mappings = {
            "tech": "technology",
            "software": "technology",
            "saas": "technology",
            "ai": "artificial_intelligence",
            "machine learning": "artificial_intelligence",
            "ml": "artificial_intelligence",
            "healthcare": "healthcare",
            "health": "healthcare",
            "biotech": "healthcare",
            "pharma": "healthcare",
            "fintech": "financial_services",
            "finance": "financial_services",
            "financial": "financial_services",
            "banking": "financial_services",
            "real estate": "real_estate",
            "property": "real_estate",
            "consumer": "consumer",
            "retail": "consumer",
            "ecommerce": "consumer",
            "e-commerce": "consumer",
            "energy": "energy",
            "cleantech": "climate_tech",
            "climate": "climate_tech",
            "sustainability": "climate_tech",
            "infrastructure": "infrastructure",
            "industrial": "industrials",
            "manufacturing": "industrials",
            "media": "media_entertainment",
            "entertainment": "media_entertainment",
        }
        
        for key, value in mappings.items():
            if key in industry_lower:
                return value
        
        return industry_lower.replace(" ", "_")
    
    def _extract_region(self, location: str) -> Optional[str]:
        """Extract geographic region from location string."""
        location_lower = location.lower()
        
        # US states - map to "us"
        us_indicators = [
            "united states", "usa", "us", "america",
            "california", "new york", "texas", "florida", "illinois",
            "massachusetts", "washington", "colorado", "georgia", "virginia",
        ]
        
        for indicator in us_indicators:
            if indicator in location_lower:
                return "us"
        
        # European countries
        europe_indicators = [
            "uk", "united kingdom", "england", "london",
            "germany", "france", "spain", "italy", "netherlands",
            "switzerland", "sweden", "denmark", "norway", "finland",
            "ireland", "belgium", "austria", "portugal", "europe",
        ]
        
        for indicator in europe_indicators:
            if indicator in location_lower:
                return "europe"
        
        # Asia
        asia_indicators = [
            "china", "japan", "korea", "singapore", "hong kong",
            "india", "taiwan", "asia", "vietnam", "indonesia",
            "malaysia", "thailand", "philippines",
        ]
        
        for indicator in asia_indicators:
            if indicator in location_lower:
                return "asia"
        
        # Other regions
        if any(x in location_lower for x in ["canada", "toronto", "vancouver"]):
            return "canada"
        
        if any(x in location_lower for x in ["israel", "tel aviv"]):
            return "israel"
        
        if any(x in location_lower for x in ["australia", "sydney", "melbourne"]):
            return "australia"
        
        if any(x in location_lower for x in ["brazil", "mexico", "latin america"]):
            return "latin_america"
        
        return None
