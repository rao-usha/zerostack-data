"""
Normalization and extraction utilities for LP strategy documents.

This module extracts structured data from raw text sections of LP investment
documents (investment committee presentations, quarterly reports, policy
statements, pacing plans).

Approach:
- Regex/keyword matching for deterministic fields (allocations, risk, themes)
- LLM (via app.agentic.llm_client) for complex extraction (summaries, managers, projections)
- Graceful degradation: returns partial results if LLM is unavailable
"""
import asyncio
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import date

from app.sources.public_lp_strategies.types import (
    StrategyExtractionResult,
    StrategySnapshotInput,
    AssetClassAllocationInput,
    AssetClassProjectionInput,
    ThematicTagInput,
    DocumentTextSectionInput,
)

logger = logging.getLogger(__name__)


# =============================================================================
# ASSET CLASS NAME NORMALIZATION
# =============================================================================

# Map common text representations to canonical asset class names
_ASSET_CLASS_MAP: Dict[str, str] = {
    # public_equity
    "public equity": "public_equity",
    "public equities": "public_equity",
    "global equity": "public_equity",
    "global equities": "public_equity",
    "domestic equity": "public_equity",
    "international equity": "public_equity",
    "us equity": "public_equity",
    "equity": "public_equity",
    "equities": "public_equity",
    "stocks": "public_equity",
    # private_equity
    "private equity": "private_equity",
    "buyout": "private_equity",
    "venture capital": "private_equity",
    "growth equity": "private_equity",
    "private markets": "private_equity",
    # real_estate
    "real estate": "real_estate",
    "real assets": "real_estate",
    "property": "real_estate",
    "reit": "real_estate",
    "reits": "real_estate",
    # fixed_income
    "fixed income": "fixed_income",
    "bonds": "fixed_income",
    "debt": "fixed_income",
    "credit": "fixed_income",
    "investment grade": "fixed_income",
    "high yield": "fixed_income",
    "treasuries": "fixed_income",
    "government bonds": "fixed_income",
    "sovereign debt": "fixed_income",
    # infrastructure
    "infrastructure": "infrastructure",
    "infra": "infrastructure",
    # cash
    "cash": "cash",
    "cash equivalents": "cash",
    "cash & equivalents": "cash",
    "money market": "cash",
    "short-term": "cash",
    "liquidity": "cash",
    # hedge_funds
    "hedge funds": "hedge_funds",
    "hedge fund": "hedge_funds",
    "absolute return": "hedge_funds",
    "alternatives": "hedge_funds",
    "alternative investments": "hedge_funds",
    "opportunistic": "hedge_funds",
}


def _normalize_asset_class(name: str) -> Optional[str]:
    """Map a text asset class name to a canonical config value."""
    key = name.strip().lower()
    if key in _ASSET_CLASS_MAP:
        return _ASSET_CLASS_MAP[key]
    # Substring match as fallback
    for pattern, canonical in _ASSET_CLASS_MAP.items():
        if pattern in key or key in pattern:
            return canonical
    return "other"


# =============================================================================
# THEME DETECTION KEYWORDS
# =============================================================================

_THEME_KEYWORDS: Dict[str, List[str]] = {
    "ai": [
        "artificial intelligence", "machine learning", "deep learning",
        "generative ai", "large language model", "neural network",
        "ai-driven", "ai-powered", "data science", "automation",
    ],
    "energy_transition": [
        "energy transition", "renewable energy", "clean energy",
        "solar", "wind power", "decarbonization", "net zero",
        "green energy", "electrification", "hydrogen",
        "carbon neutral", "emissions reduction",
    ],
    "climate_resilience": [
        "climate resilience", "climate risk", "climate adaptation",
        "climate change", "esg", "environmental", "sustainability",
        "sustainable investing", "impact investing", "responsible investment",
    ],
    "reshoring": [
        "reshoring", "nearshoring", "onshoring", "supply chain resilience",
        "domestic manufacturing", "friend-shoring", "de-globalization",
        "supply chain diversification",
    ],
    "healthcare": [
        "healthcare", "health care", "biotech", "biotechnology",
        "pharmaceutical", "life sciences", "medical device",
        "digital health", "genomics", "precision medicine",
    ],
    "technology": [
        "technology", "digital infrastructure", "cybersecurity",
        "cloud computing", "saas", "fintech", "software",
        "semiconductor", "data center", "5g", "digital transformation",
    ],
    "sustainability": [
        "sustainability", "sustainable", "esg integration",
        "social responsibility", "governance", "dei",
        "diversity", "inclusion", "stakeholder",
    ],
    "infrastructure": [
        "infrastructure investment", "core infrastructure",
        "transportation infrastructure", "utilities",
        "public-private partnership", "p3", "toll road",
        "airport", "port", "water infrastructure",
    ],
}

# =============================================================================
# RISK POSITIONING KEYWORDS
# =============================================================================

_RISK_ON_KEYWORDS = [
    "overweight equities", "increasing equity allocation",
    "risk-on", "aggressive positioning", "overweight risk assets",
    "increasing private equity", "adding to growth",
    "tilting toward equities", "pro-cyclical",
    "increasing exposure", "overweight",
    "adding risk", "above target",
]

_RISK_OFF_KEYWORDS = [
    "underweight equities", "defensive posture", "risk-off",
    "de-risking", "reducing equity", "increasing fixed income",
    "increasing cash", "flight to quality", "capital preservation",
    "reducing risk", "below target equity",
    "underweight risk assets", "protective positioning",
    "hedging", "downside protection",
]

_NEUTRAL_KEYWORDS = [
    "balanced positioning", "neutral stance", "at target",
    "benchmark weight", "strategic allocation",
    "maintaining current", "no significant changes",
    "stable allocation", "within range",
]

# =============================================================================
# LIQUIDITY KEYWORDS
# =============================================================================

_HIGH_LIQUIDITY_KEYWORDS = [
    "high liquidity", "ample liquidity", "liquid portfolio",
    "increasing cash", "cash reserves", "above target cash",
    "strong liquidity position", "excess liquidity",
]

_LOW_LIQUIDITY_KEYWORDS = [
    "illiquid", "illiquidity", "liquidity constraints",
    "liquidity risk", "over-committed", "capital calls",
    "denominator effect", "locked up", "limited redemption",
    "long lock-up", "liquidity challenge",
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def parse_percentage(text: str) -> Optional[float]:
    """Parse percentage from text (e.g., '35%', '35.5 percent', '0.35')."""
    # Pattern: "35%", "35.5%", "35 %"
    match = re.search(r'(\d+\.?\d*)\s*%', text)
    if match:
        return float(match.group(1))
    # Pattern: "35 percent", "35.5 percent"
    match = re.search(r'(\d+\.?\d*)\s*percent', text, re.IGNORECASE)
    if match:
        return float(match.group(1))
    # Pattern: "0.35" (decimal, only if looks like a weight)
    match = re.search(r'\b0\.(\d{2,})\b', text)
    if match:
        return float(f"0.{match.group(1)}") * 100
    return None


def parse_percentage_range(text: str) -> tuple[Optional[float], Optional[float]]:
    """Parse a percentage range like '30-40%' or '30% - 40%'."""
    match = re.search(r'(\d+\.?\d*)\s*%?\s*[-–—to]+\s*(\d+\.?\d*)\s*%', text)
    if match:
        return float(match.group(1)), float(match.group(2))
    return None, None


def parse_currency_amount(text: str) -> Optional[float]:
    """Parse currency amount from text (e.g., '$5B', '5 billion', '$5,000,000')."""
    multipliers = {
        'T': 1_000_000_000_000, 'TRILLION': 1_000_000_000_000,
        'B': 1_000_000_000, 'BN': 1_000_000_000, 'BILLION': 1_000_000_000,
        'M': 1_000_000, 'MM': 1_000_000, 'MN': 1_000_000, 'MILLION': 1_000_000,
        'K': 1_000, 'THOUSAND': 1_000,
    }
    # Pattern: "$5B", "$5.2B", "$5.2 billion"
    match = re.search(
        r'\$\s*([\d,]+\.?\d*)\s*(trillion|billion|million|thousand|[TBMK]N?)',
        text, re.IGNORECASE,
    )
    if match:
        value = float(match.group(1).replace(',', ''))
        unit = match.group(2).upper().rstrip('S')
        return value * multipliers.get(unit, 1)
    # Pattern: "$5,000,000" (raw number)
    match = re.search(r'\$\s*([\d,]+\.?\d*)', text)
    if match:
        value = float(match.group(1).replace(',', ''))
        if value >= 1000:
            return value
    return None


# =============================================================================
# CORE EXTRACTION FUNCTIONS
# =============================================================================


def extract_asset_class_allocations(
    text: str,
    section_id: Optional[int] = None
) -> List[AssetClassAllocationInput]:
    """
    Extract asset class allocation table from text using regex pattern matching.

    Handles formats:
    - "Public Equity: Target 35%, Current 37%, Range 30-40%"
    - "Public Equity  35%  37%  30-40%"
    - Tabular rows with asset class name followed by percentages
    """
    allocations = []
    lines = text.split('\n')

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Try to find an asset class name in the line
        matched_class = None
        matched_pos = -1
        for name_variant, canonical in _ASSET_CLASS_MAP.items():
            idx = line_stripped.lower().find(name_variant)
            if idx != -1:
                if matched_class is None or len(name_variant) > matched_pos:
                    matched_class = canonical
                    matched_pos = len(name_variant)

        if matched_class is None:
            continue

        # Extract all percentages from the line
        pct_matches = re.findall(r'(\d+\.?\d*)\s*%', line_stripped)
        if not pct_matches:
            continue

        pcts = [float(p) for p in pct_matches]
        # Filter out obviously wrong values (> 100%)
        pcts = [p for p in pcts if p <= 100]
        if not pcts:
            continue

        target = None
        current = None
        min_w = None
        max_w = None
        benchmark = None

        lower_line = line_stripped.lower()

        # Try labeled values first: "target 35%", "current 37%"
        target_match = re.search(r'target\s*:?\s*(\d+\.?\d*)\s*%', lower_line)
        current_match = re.search(r'(?:current|actual)\s*:?\s*(\d+\.?\d*)\s*%', lower_line)
        benchmark_match = re.search(r'benchmark\s*:?\s*(\d+\.?\d*)\s*%', lower_line)

        if target_match:
            target = float(target_match.group(1))
        if current_match:
            current = float(current_match.group(1))
        if benchmark_match:
            benchmark = float(benchmark_match.group(1))

        # Try range: "30-40%", "30% - 40%"
        min_w, max_w = parse_percentage_range(line_stripped)

        # If no labeled values, interpret positionally
        if target is None and current is None:
            if len(pcts) >= 2:
                target = pcts[0]
                current = pcts[1]
            elif len(pcts) == 1:
                target = pcts[0]

        # Skip if we have duplicate asset class already
        if any(a.asset_class == matched_class for a in allocations):
            continue

        allocations.append(AssetClassAllocationInput(
            asset_class=matched_class,
            target_weight_pct=target,
            current_weight_pct=current,
            min_weight_pct=min_w,
            max_weight_pct=max_w,
            benchmark_weight_pct=benchmark,
            source_section_id=section_id,
        ))

    if allocations:
        logger.info(f"Extracted {len(allocations)} asset class allocations from text")

    return allocations


def extract_asset_class_projections(
    text: str,
    section_id: Optional[int] = None
) -> List[AssetClassProjectionInput]:
    """
    Extract asset class projections/pacing plans from text.

    Looks for patterns combining asset class names with:
    - Expected returns: "expected return 8.5%"
    - Volatility: "volatility 12%", "risk 15%"
    - Commitment amounts: "$5B commitment", "commit $2.3 billion"
    - Horizons: "3-year", "5-year", "10-year"
    """
    projections = []
    lines = text.split('\n')

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Find asset class (longest match wins to avoid "equity" matching before "private equity")
        matched_class = None
        matched_len = -1
        for name_variant, canonical in _ASSET_CLASS_MAP.items():
            if name_variant in line_stripped.lower():
                if len(name_variant) > matched_len:
                    matched_class = canonical
                    matched_len = len(name_variant)
        if matched_class is None:
            continue

        lower_line = line_stripped.lower()

        # Detect horizon
        horizon = None
        horizon_match = re.search(r'(\d+)\s*[-–]?\s*year', lower_line)
        if horizon_match:
            years = int(horizon_match.group(1))
            horizon_map = {1: "1_year", 3: "3_year", 5: "5_year", 10: "10_year"}
            horizon = horizon_map.get(years)
        if horizon is None:
            horizon = "1_year"

        # Extract expected return
        expected_return = None
        ret_match = re.search(
            r'(?:expected|projected|target|forecast)\s*(?:return|yield)\s*:?\s*(\d+\.?\d*)\s*%',
            lower_line,
        )
        if ret_match:
            expected_return = float(ret_match.group(1))

        # Extract volatility
        volatility = None
        vol_match = re.search(
            r'(?:volatility|risk|std dev|standard deviation)\s*:?\s*(\d+\.?\d*)\s*%',
            lower_line,
        )
        if vol_match:
            volatility = float(vol_match.group(1))

        # Extract commitment amount
        commitment = None
        commit_match = re.search(
            r'(?:commitment?|commit|pacing|deploy|allocat)\w*\s*:?\s*\$?\s*([\d,.]+)\s*(billion|million|[BM])',
            lower_line,
        )
        if commit_match:
            commitment = parse_currency_amount(
                f"${commit_match.group(1)} {commit_match.group(2)}"
            )

        # Extract net flow
        net_flow = None
        flow_match = re.search(
            r'(?:net flow|net cash flow|distribution)\s*:?\s*\$?\s*([\d,.]+)\s*(billion|million|[BM])',
            lower_line,
        )
        if flow_match:
            net_flow = parse_currency_amount(
                f"${flow_match.group(1)} {flow_match.group(2)}"
            )

        if expected_return is None and volatility is None and commitment is None and net_flow is None:
            continue

        # Skip duplicates
        if any(
            p.asset_class == matched_class and p.projection_horizon == horizon
            for p in projections
        ):
            continue

        projections.append(AssetClassProjectionInput(
            asset_class=matched_class,
            projection_horizon=horizon,
            expected_return_pct=expected_return,
            expected_volatility_pct=volatility,
            commitment_plan_amount=commitment,
            net_flow_projection_amount=net_flow,
            source_section_id=section_id,
        ))

    if projections:
        logger.info(f"Extracted {len(projections)} asset class projections from text")

    return projections


def extract_thematic_tags(
    text: str,
    section_id: Optional[int] = None
) -> List[ThematicTagInput]:
    """
    Extract thematic investment tags from text using keyword matching.

    Scores relevance based on keyword frequency and prominence (mentions
    in the first third of text score higher).
    """
    text_lower = text.lower()
    word_count = max(len(text_lower.split()), 1)
    first_third = text_lower[:len(text_lower) // 3]
    tags = []

    for theme, keywords in _THEME_KEYWORDS.items():
        total_hits = 0
        early_hits = 0

        for kw in keywords:
            count = text_lower.count(kw)
            total_hits += count
            if kw in first_third:
                early_hits += 1

        if total_hits == 0:
            continue

        # Score: base from density, bonus for early mentions
        density = total_hits / (word_count / 100)  # hits per 100 words
        base_score = min(density / 5.0, 0.8)  # cap at 0.8
        early_bonus = min(early_hits * 0.1, 0.2)
        relevance = min(base_score + early_bonus, 1.0)
        relevance = round(relevance, 2)

        if relevance < 0.05:
            continue

        tags.append(ThematicTagInput(
            theme=theme,
            relevance_score=relevance,
            source_section_id=section_id,
        ))

    # Sort by relevance descending
    tags.sort(key=lambda t: t.relevance_score or 0, reverse=True)

    if tags:
        logger.info(
            f"Extracted {len(tags)} thematic tags: "
            f"{', '.join(t.theme for t in tags[:5])}"
        )

    return tags


def detect_risk_positioning(text: str) -> Optional[str]:
    """
    Detect risk positioning from text using keyword scoring.

    Returns 'risk_on', 'risk_off', or 'neutral'.
    """
    text_lower = text.lower()

    risk_on_score = sum(1 for kw in _RISK_ON_KEYWORDS if kw in text_lower)
    risk_off_score = sum(1 for kw in _RISK_OFF_KEYWORDS if kw in text_lower)
    neutral_score = sum(1 for kw in _NEUTRAL_KEYWORDS if kw in text_lower)

    total = risk_on_score + risk_off_score + neutral_score
    if total == 0:
        return None

    if risk_on_score > risk_off_score and risk_on_score > neutral_score:
        return "risk_on"
    elif risk_off_score > risk_on_score and risk_off_score > neutral_score:
        return "risk_off"
    elif neutral_score > 0:
        return "neutral"

    return None


def extract_liquidity_profile(text: str) -> Optional[str]:
    """
    Extract liquidity profile description from text.

    Returns a category: 'high_liquidity', 'moderate_liquidity', or 'illiquid_heavy'.
    """
    text_lower = text.lower()

    high_score = sum(1 for kw in _HIGH_LIQUIDITY_KEYWORDS if kw in text_lower)
    low_score = sum(1 for kw in _LOW_LIQUIDITY_KEYWORDS if kw in text_lower)

    if high_score == 0 and low_score == 0:
        return None

    if high_score > low_score:
        return "high_liquidity"
    elif low_score > high_score:
        return "illiquid_heavy"
    else:
        return "moderate_liquidity"


# =============================================================================
# LLM-POWERED EXTRACTION
# =============================================================================

_SUMMARY_SYSTEM_PROMPT = (
    "You are a financial analyst specializing in institutional investor strategy. "
    "Extract key information from LP strategy documents concisely and accurately. "
    "Focus on investment allocation changes, forward guidance, and notable themes."
)

_SUMMARY_PROMPT_TEMPLATE = """Summarize this LP strategy document in 2-4 sentences.
Focus on: current allocation stance, key changes from prior period, forward guidance, and notable themes.

Document text:
{text}

Respond with ONLY the summary text, no JSON or formatting."""

_MANAGER_PROMPT_TEMPLATE = """Extract manager and investment vehicle names from this text.
For each manager, extract:
- manager_name: The firm/manager name
- vehicle_name: Fund or vehicle name (if mentioned)
- asset_class: The asset class (public_equity, private_equity, real_estate, fixed_income, infrastructure, cash, hedge_funds, other)
- commitment_amount: Dollar amount if mentioned (as a number, no symbols)
- status: "existing", "new", or "exiting"

Document text:
{text}

Respond with a JSON array of objects. If no managers found, respond with [].
Example: [{{"manager_name": "Blackstone", "vehicle_name": "Blackstone Real Estate Fund IX", "asset_class": "real_estate", "commitment_amount": 500000000, "status": "new"}}]"""


async def generate_strategy_summary(
    sections: List[DocumentTextSectionInput],
) -> Optional[str]:
    """
    Generate high-level strategy summary from document sections using LLM.

    Falls back to extracting the first substantial section if LLM is unavailable.
    """
    if not sections:
        return None

    combined_text = "\n\n".join(s.text for s in sections)
    # Truncate to ~8K chars for LLM context
    if len(combined_text) > 8000:
        combined_text = combined_text[:8000] + "\n[...truncated]"

    try:
        from app.agentic.llm_client import get_llm_client
        client = get_llm_client()
        if client is None:
            logger.info("No LLM client available, falling back to heuristic summary")
            return _heuristic_summary(sections)

        response = await client.complete(
            prompt=_SUMMARY_PROMPT_TEMPLATE.format(text=combined_text),
            system_prompt=_SUMMARY_SYSTEM_PROMPT,
        )
        summary = response.content.strip()
        if summary and len(summary) > 20:
            logger.info(f"Generated LLM strategy summary ({len(summary)} chars)")
            return summary
    except Exception as e:
        logger.warning(f"LLM summary generation failed: {e}")

    return _heuristic_summary(sections)


def _heuristic_summary(sections: List[DocumentTextSectionInput]) -> Optional[str]:
    """Fallback: extract first substantial paragraph as summary."""
    for section in sections:
        # Look for executive summary or overview sections
        name = (section.section_name or "").lower()
        if any(kw in name for kw in ["summary", "overview", "highlights", "executive"]):
            text = section.text.strip()
            # Take first 500 chars
            if len(text) > 50:
                summary = text[:500]
                if len(text) > 500:
                    # Cut at last sentence boundary
                    last_period = summary.rfind('.')
                    if last_period > 100:
                        summary = summary[:last_period + 1]
                return summary

    # Fallback: first section with enough text
    for section in sections:
        if len(section.text.strip()) > 100:
            text = section.text.strip()[:300]
            last_period = text.rfind('.')
            if last_period > 50:
                text = text[:last_period + 1]
            return text

    return None


async def extract_manager_exposures(
    text: str,
    section_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Extract manager/vehicle exposure information from text using LLM.

    Falls back to regex-based extraction if LLM is unavailable.
    """
    if not text or len(text.strip()) < 50:
        return []

    # Truncate for LLM
    input_text = text[:6000] if len(text) > 6000 else text

    try:
        from app.agentic.llm_client import get_llm_client
        client = get_llm_client(model="gpt-4o-mini")
        if client is None:
            return _regex_manager_extraction(text)

        response = await client.complete(
            prompt=_MANAGER_PROMPT_TEMPLATE.format(text=input_text),
            system_prompt=_SUMMARY_SYSTEM_PROMPT,
            json_mode=True,
        )
        data = response.parse_json()
        if isinstance(data, list):
            logger.info(f"LLM extracted {len(data)} manager exposures")
            return data
        if isinstance(data, dict) and "managers" in data:
            return data["managers"]
    except Exception as e:
        logger.warning(f"LLM manager extraction failed: {e}")

    return _regex_manager_extraction(text)


def _regex_manager_extraction(text: str) -> List[Dict[str, Any]]:
    """Fallback regex extraction for manager names."""
    managers = []
    # Pattern: "Firm Name Fund IX" or "Firm Name Capital"
    patterns = [
        r'([A-Z][a-zA-Z&\s]{2,30}(?:Capital|Partners|Group|Management|Advisors|Investment[s]?))\s+(?:Fund\s+[IVX\d]+|LP)',
        r'(?:committed?|allocated?|invested?)\s+(?:to|in|with)\s+([A-Z][a-zA-Z&\s]{2,40})',
    ]
    seen = set()
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            name = match.group(1).strip()
            if name not in seen and len(name) > 3:
                seen.add(name)
                managers.append({
                    "manager_name": name,
                    "vehicle_name": None,
                    "asset_class": "other",
                    "commitment_amount": None,
                    "status": "existing",
                })
    return managers


# =============================================================================
# MAIN ORCHESTRATOR
# =============================================================================


async def extract_strategy_from_text_sections(
    sections: List[DocumentTextSectionInput],
    lp_id: int,
    program: str,
    fiscal_year: int,
    fiscal_quarter: str,
) -> StrategyExtractionResult:
    """
    Extract structured strategy data from document text sections.

    This is the main entry point for converting raw text into structured data.
    Combines regex-based extraction for allocations/themes/risk with LLM-based
    extraction for summaries and manager exposures.

    Args:
        sections: List of parsed text sections
        lp_id: LP fund ID
        program: Program name
        fiscal_year: Fiscal year
        fiscal_quarter: Fiscal quarter

    Returns:
        StrategyExtractionResult with extracted data
    """
    if not sections:
        logger.warning("No sections provided for extraction")
        return _empty_result(lp_id, program, fiscal_year, fiscal_quarter)

    combined_text = "\n\n".join(s.text for s in sections)

    # Run regex-based extractions (deterministic)
    all_allocations = []
    all_projections = []
    all_tags = []
    all_managers = []

    for section in sections:
        sid = None  # section_id would come from DB
        allocs = extract_asset_class_allocations(section.text, sid)
        projs = extract_asset_class_projections(section.text, sid)
        tags = extract_thematic_tags(section.text, sid)
        all_allocations.extend(allocs)
        all_projections.extend(projs)
        all_tags.extend(tags)

    # Deduplicate allocations by asset class (keep first occurrence)
    seen_classes = set()
    deduped_allocs = []
    for a in all_allocations:
        if a.asset_class not in seen_classes:
            seen_classes.add(a.asset_class)
            deduped_allocs.append(a)
    all_allocations = deduped_allocs

    # Deduplicate projections by (asset_class, horizon)
    seen_projs = set()
    deduped_projs = []
    for p in all_projections:
        key = (p.asset_class, p.projection_horizon)
        if key not in seen_projs:
            seen_projs.add(key)
            deduped_projs.append(p)
    all_projections = deduped_projs

    # Deduplicate tags by theme (keep highest relevance)
    tag_map: Dict[str, ThematicTagInput] = {}
    for t in all_tags:
        existing = tag_map.get(t.theme)
        if existing is None or (t.relevance_score or 0) > (existing.relevance_score or 0):
            tag_map[t.theme] = t
    all_tags = sorted(tag_map.values(), key=lambda t: t.relevance_score or 0, reverse=True)

    # Risk and liquidity from combined text
    risk_positioning = detect_risk_positioning(combined_text)
    liquidity_profile = extract_liquidity_profile(combined_text)

    # LLM-powered extractions (async)
    summary = await generate_strategy_summary(sections)
    manager_text = _find_manager_sections(sections, combined_text)
    all_managers = await extract_manager_exposures(manager_text)

    strategy = StrategySnapshotInput(
        lp_id=lp_id,
        program=program,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        strategy_date=None,
        primary_document_id=None,
        summary_text=summary,
        risk_positioning=risk_positioning,
        liquidity_profile=liquidity_profile,
        tilt_description=None,
    )

    logger.info(
        f"Extraction complete for lp_id={lp_id} FY{fiscal_year}-{fiscal_quarter}: "
        f"{len(all_allocations)} allocations, {len(all_projections)} projections, "
        f"{len(all_tags)} themes, {len(all_managers)} managers, "
        f"risk={risk_positioning}, liquidity={liquidity_profile}"
    )

    return StrategyExtractionResult(
        strategy=strategy,
        allocations=all_allocations,
        projections=all_projections,
        thematic_tags=all_tags,
        manager_exposures=all_managers,
    )


def _find_manager_sections(
    sections: List[DocumentTextSectionInput],
    fallback_text: str,
) -> str:
    """Find sections most likely to contain manager/vehicle info."""
    manager_keywords = ["manager", "holding", "commitment", "vehicle", "fund", "exposure"]
    for section in sections:
        name = (section.section_name or "").lower()
        if any(kw in name for kw in manager_keywords):
            return section.text

    # Fallback: use combined text, truncated
    return fallback_text[:6000]


def _empty_result(
    lp_id: int, program: str, fiscal_year: int, fiscal_quarter: str,
) -> StrategyExtractionResult:
    """Return an empty extraction result."""
    return StrategyExtractionResult(
        strategy=StrategySnapshotInput(
            lp_id=lp_id,
            program=program,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
        ),
        allocations=[],
        projections=[],
        thematic_tags=[],
        manager_exposures=[],
    )
