"""
CAFR (Comprehensive Annual Financial Report) Parser with LLM extraction.

Downloads CAFR PDFs and uses LLM to extract structured data:
- Asset allocation (current and target)
- Performance returns (1/3/5/10 year)
- External managers list
- Governance information
"""

import asyncio
import io
import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

from app.sources.lp_collection.base_collector import BaseCollector
from app.sources.lp_collection.types import (
    CollectionResult,
    CollectedItem,
    LpCollectionSource,
)
from app.agentic.llm_client import get_llm_client, LLMClient

logger = logging.getLogger(__name__)


# Try to import PDF libraries
try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    pdfplumber = None

try:
    from PyPDF2 import PdfReader

    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    PdfReader = None


# Extraction prompt templates
ALLOCATION_EXTRACTION_PROMPT = """Extract the asset allocation information from this CAFR text.

Return a JSON object with:
{{
    "total_fund_value_usd": "123456789000",  // Total fund value in USD (no commas)
    "fiscal_year": 2024,
    "allocations": [
        {{
            "asset_class": "Public Equity",  // Use standard names
            "current_pct": "35.2",
            "target_pct": "36.0",
            "min_pct": "30.0",   // If available
            "max_pct": "40.0"    // If available
        }}
    ],
    "benchmark_name": "60/40 Policy Benchmark"  // If mentioned
}}

Standard asset class names:
- Public Equity
- Fixed Income
- Private Equity
- Real Estate
- Real Assets / Infrastructure
- Hedge Funds / Alternatives
- Cash / Short Term
- Other

If a field is not found, use null.

TEXT TO ANALYZE:
{text}
"""

PERFORMANCE_EXTRACTION_PROMPT = """Extract investment performance returns from this CAFR text.

Return a JSON object with:
{{
    "fiscal_year": 2024,
    "returns": {{
        "one_year_pct": "12.5",
        "three_year_pct": "8.3",
        "five_year_pct": "9.1",
        "ten_year_pct": "7.8",
        "since_inception_pct": "8.2"
    }},
    "benchmark_returns": {{
        "benchmark_name": "Policy Benchmark",
        "one_year_pct": "11.2",
        "three_year_pct": "7.5",
        "five_year_pct": "8.8"
    }},
    "value_added_bps": {{
        "one_year": "130",  // Basis points above benchmark
        "five_year": "30"
    }}
}}

Extract all return figures mentioned. If not found, use null.

TEXT TO ANALYZE:
{text}
"""

MANAGERS_EXTRACTION_PROMPT = """Extract the list of external investment managers from this CAFR text.

Return a JSON object with:
{{
    "managers": [
        {{
            "name": "BlackRock",
            "asset_class": "Public Equity",  // Primary asset class
            "mandate_type": "passive",  // passive, active, co-investment
            "commitment_usd": "500000000"  // If mentioned
        }}
    ]
}}

Only include managers that are clearly external investment managers/advisors.
Do NOT include trustees, board members, consultants, or custodians.

TEXT TO ANALYZE:
{text}
"""


class CafrParser(BaseCollector):
    """
    Enhanced CAFR collector with LLM-powered data extraction.

    Process:
    1. Download CAFR PDF from LP website
    2. Extract text using pdfplumber or PyPDF2
    3. Use LLM to extract structured data
    4. Return normalized CollectedItems
    """

    MAX_PAGES_TO_EXTRACT = 100  # Limit for large PDFs
    TEXT_CHUNK_SIZE = 15000  # Characters per LLM call

    @property
    def source_type(self) -> LpCollectionSource:
        return LpCollectionSource.CAFR

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._llm_client: Optional[LLMClient] = None

    def _get_llm_client(self) -> Optional[LLMClient]:
        """Get or create LLM client."""
        if self._llm_client is None:
            self._llm_client = get_llm_client()
        return self._llm_client

    async def collect(
        self,
        lp_id: int,
        lp_name: str,
        website_url: Optional[str] = None,
        cafr_url: Optional[str] = None,
        **kwargs,
    ) -> CollectionResult:
        """
        Collect and parse CAFR data for an LP.

        Args:
            lp_id: LP fund ID
            lp_name: LP fund name
            website_url: LP website URL (to find CAFR)
            cafr_url: Direct URL to CAFR PDF (if known)

        Returns:
            CollectionResult with extracted data items
        """
        self.reset_tracking()
        started_at = datetime.utcnow()
        items: List[CollectedItem] = []
        warnings: List[str] = []

        logger.info(f"Parsing CAFR for {lp_name}")

        # Check prerequisites
        if not PDFPLUMBER_AVAILABLE and not PYPDF2_AVAILABLE:
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message="No PDF library available (install pdfplumber or PyPDF2)",
                started_at=started_at,
            )

        llm = self._get_llm_client()
        if not llm:
            warnings.append("LLM client not available; using regex-only extraction")

        try:
            # Find CAFR URL if not provided
            if not cafr_url and website_url:
                cafr_url = await self._find_cafr_url(website_url)

            if not cafr_url:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not find CAFR PDF URL",
                    warnings=["No CAFR URL provided and could not find on website"],
                    started_at=started_at,
                )

            # Download PDF
            pdf_content = await self._download_pdf(cafr_url)
            if not pdf_content:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not download CAFR PDF",
                    started_at=started_at,
                )

            # Extract text
            text, extraction_warnings = self._extract_pdf_text(pdf_content)
            warnings.extend(extraction_warnings)

            if not text:
                return self._create_result(
                    lp_id=lp_id,
                    lp_name=lp_name,
                    success=False,
                    error_message="Could not extract text from CAFR PDF",
                    warnings=warnings,
                    started_at=started_at,
                )

            logger.info(f"Extracted {len(text)} characters from CAFR")

            # Detect fiscal year from text
            fiscal_year = self._detect_fiscal_year(text)

            # Extract data using LLM
            if llm:
                # Extract allocations
                allocation_data = await self._extract_allocations_llm(
                    llm, text, lp_id, lp_name, cafr_url, fiscal_year
                )
                if allocation_data:
                    items.extend(allocation_data)

                # Extract performance
                performance_data = await self._extract_performance_llm(
                    llm, text, lp_id, lp_name, cafr_url, fiscal_year
                )
                if performance_data:
                    items.extend(performance_data)

                # Extract managers
                manager_data = await self._extract_managers_llm(
                    llm, text, lp_id, lp_name, cafr_url
                )
                if manager_data:
                    items.extend(manager_data)

            else:
                # Fallback: regex-based extraction
                regex_items = self._extract_with_regex(
                    text, lp_id, lp_name, cafr_url, fiscal_year
                )
                items.extend(regex_items)

            # Add document item
            items.append(
                CollectedItem(
                    item_type="document_link",
                    data={
                        "lp_id": lp_id,
                        "url": cafr_url,
                        "title": f"CAFR {fiscal_year}" if fiscal_year else "CAFR",
                        "document_type": "cafr",
                        "file_format": "pdf",
                        "fiscal_year": fiscal_year,
                        "text_length": len(text),
                        "source_type": "cafr",
                    },
                    source_url=cafr_url,
                    confidence="high",
                )
            )

            success = len(items) > 0

            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=success,
                items=items,
                warnings=warnings,
                started_at=started_at,
            )

        except Exception as e:
            logger.error(f"Error parsing CAFR for {lp_name}: {e}")
            return self._create_result(
                lp_id=lp_id,
                lp_name=lp_name,
                success=False,
                error_message=str(e),
                warnings=warnings,
                started_at=started_at,
            )

    async def _find_cafr_url(self, website_url: str) -> Optional[str]:
        """Find CAFR PDF URL from LP website."""
        # Patterns for CAFR pages
        page_patterns = [
            "/cafr",
            "/acfr",
            "/annual-report",
            "/comprehensive-annual-financial-report",
            "/financial-reports",
            "/publications",
            "/reports",
        ]

        cafr_pattern = re.compile(
            r"cafr|acfr|comprehensive.*annual.*financial|annual.*report", re.IGNORECASE
        )

        # Try main website first
        response = await self._fetch_url(website_url)
        if response and response.status_code == 200:
            pdf_url = self._find_cafr_pdf_link(response.text, website_url, cafr_pattern)
            if pdf_url:
                return pdf_url

        # Try common CAFR pages
        for pattern in page_patterns:
            page_url = website_url.rstrip("/") + pattern
            response = await self._fetch_url(page_url)

            if response and response.status_code == 200:
                pdf_url = self._find_cafr_pdf_link(
                    response.text, page_url, cafr_pattern
                )
                if pdf_url:
                    return pdf_url

        return None

    def _find_cafr_pdf_link(
        self, html: str, base_url: str, cafr_pattern: re.Pattern
    ) -> Optional[str]:
        """Find CAFR PDF link in HTML."""
        from urllib.parse import urljoin

        pdf_pattern = re.compile(
            r'href=["\']([^"\']*\.pdf)["\'][^>]*>([^<]*)', re.IGNORECASE
        )

        best_match = None
        best_year = 0

        for match in pdf_pattern.finditer(html):
            href = match.group(1)
            link_text = match.group(2).strip()
            combined = f"{href} {link_text}"

            if cafr_pattern.search(combined):
                # Extract year
                year_match = re.search(r"20\d{2}", combined)
                year = int(year_match.group()) if year_match else 0

                if year > best_year:
                    best_year = year
                    best_match = href

        if best_match:
            return urljoin(base_url, best_match)

        return None

    async def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF from URL."""
        headers = {"Accept": "application/pdf"}
        response = await self._fetch_url(url, headers=headers)

        if response and response.status_code == 200:
            return response.content

        return None

    def _extract_pdf_text(self, pdf_content: bytes) -> Tuple[str, List[str]]:
        """
        Extract text from PDF content.

        Returns:
            Tuple of (extracted_text, warnings)
        """
        warnings = []
        text_parts = []

        # Try pdfplumber first (better text extraction)
        if PDFPLUMBER_AVAILABLE:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    for i, page in enumerate(pdf.pages[: self.MAX_PAGES_TO_EXTRACT]):
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)

                    if len(pdf.pages) > self.MAX_PAGES_TO_EXTRACT:
                        warnings.append(
                            f"PDF has {len(pdf.pages)} pages; "
                            f"only extracted first {self.MAX_PAGES_TO_EXTRACT}"
                        )

                if text_parts:
                    return "\n\n".join(text_parts), warnings

            except Exception as e:
                warnings.append(f"pdfplumber extraction failed: {e}")

        # Fallback to PyPDF2
        if PYPDF2_AVAILABLE:
            try:
                reader = PdfReader(io.BytesIO(pdf_content))
                for i, page in enumerate(reader.pages[: self.MAX_PAGES_TO_EXTRACT]):
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

                if text_parts:
                    return "\n\n".join(text_parts), warnings

            except Exception as e:
                warnings.append(f"PyPDF2 extraction failed: {e}")

        return "", warnings

    def _detect_fiscal_year(self, text: str) -> Optional[int]:
        """Detect fiscal year from CAFR text."""
        # Look for explicit fiscal year mentions
        fy_pattern = re.compile(
            r"fiscal\s+year\s+(?:ended?\s+)?(?:june\s+30,?\s+)?(\d{4})", re.IGNORECASE
        )
        match = fy_pattern.search(text)
        if match:
            return int(match.group(1))

        # Look for year range (e.g., "2023-2024")
        range_pattern = re.compile(r"20\d{2}\s*[-–]\s*(20\d{2})")
        match = range_pattern.search(text)
        if match:
            return int(match.group(1))

        # Find most common recent year
        years = re.findall(r"20\d{2}", text)
        if years:
            from collections import Counter

            year_counts = Counter(int(y) for y in years if 2015 <= int(y) <= 2030)
            if year_counts:
                return year_counts.most_common(1)[0][0]

        return None

    async def _extract_allocations_llm(
        self,
        llm: LLMClient,
        text: str,
        lp_id: int,
        lp_name: str,
        source_url: str,
        fiscal_year: Optional[int],
    ) -> List[CollectedItem]:
        """Extract asset allocation using LLM."""
        items = []

        # Find relevant text chunks (look for allocation-related content)
        allocation_keywords = [
            "asset allocation",
            "investment policy",
            "target allocation",
            "actual allocation",
            "strategic allocation",
            "policy benchmark",
        ]

        chunk = self._find_relevant_chunk(text, allocation_keywords)
        if not chunk:
            chunk = text[: self.TEXT_CHUNK_SIZE]

        try:
            prompt = ALLOCATION_EXTRACTION_PROMPT.format(text=chunk)
            response = await llm.complete(
                prompt=prompt,
                system_prompt="You extract structured financial data from pension fund documents. Return valid JSON only.",
                json_mode=True if llm.provider == "openai" else False,
            )

            data = response.parse_json()
            if data:
                # Create allocation items
                allocations = data.get("allocations", [])
                fy = data.get("fiscal_year") or fiscal_year

                for alloc in allocations:
                    items.append(
                        CollectedItem(
                            item_type="allocation",
                            data={
                                "lp_id": lp_id,
                                "lp_name": lp_name,
                                "fiscal_year": fy,
                                "asset_class": alloc.get("asset_class"),
                                "current_pct": alloc.get("current_pct"),
                                "target_pct": alloc.get("target_pct"),
                                "min_pct": alloc.get("min_pct"),
                                "max_pct": alloc.get("max_pct"),
                                "source_type": "cafr",
                            },
                            source_url=source_url,
                            confidence="high",
                        )
                    )

                # Add total fund value if extracted
                if data.get("total_fund_value_usd"):
                    items.append(
                        CollectedItem(
                            item_type="strategy_info",
                            data={
                                "lp_id": lp_id,
                                "fiscal_year": fy,
                                "total_fund_value_usd": data.get(
                                    "total_fund_value_usd"
                                ),
                                "benchmark_name": data.get("benchmark_name"),
                                "source_type": "cafr",
                            },
                            source_url=source_url,
                            confidence="high",
                        )
                    )

        except Exception as e:
            logger.warning(f"LLM allocation extraction failed: {e}")

        return items

    async def _extract_performance_llm(
        self,
        llm: LLMClient,
        text: str,
        lp_id: int,
        lp_name: str,
        source_url: str,
        fiscal_year: Optional[int],
    ) -> List[CollectedItem]:
        """Extract performance returns using LLM."""
        items = []

        # Find relevant text chunks
        perf_keywords = [
            "investment performance",
            "rate of return",
            "annualized return",
            "total fund return",
            "benchmark",
            "value added",
        ]

        chunk = self._find_relevant_chunk(text, perf_keywords)
        if not chunk:
            chunk = text[: self.TEXT_CHUNK_SIZE]

        try:
            prompt = PERFORMANCE_EXTRACTION_PROMPT.format(text=chunk)
            response = await llm.complete(
                prompt=prompt,
                system_prompt="You extract structured financial data from pension fund documents. Return valid JSON only.",
            )

            data = response.parse_json()
            if data:
                fy = data.get("fiscal_year") or fiscal_year
                returns = data.get("returns", {})
                benchmark = data.get("benchmark_returns", {})

                if any(returns.values()):
                    items.append(
                        CollectedItem(
                            item_type="performance_return",
                            data={
                                "lp_id": lp_id,
                                "lp_name": lp_name,
                                "fiscal_year": fy,
                                "one_year_return_pct": returns.get("one_year_pct"),
                                "three_year_return_pct": returns.get("three_year_pct"),
                                "five_year_return_pct": returns.get("five_year_pct"),
                                "ten_year_return_pct": returns.get("ten_year_pct"),
                                "since_inception_return_pct": returns.get(
                                    "since_inception_pct"
                                ),
                                "benchmark_name": benchmark.get("benchmark_name"),
                                "benchmark_one_year_pct": benchmark.get("one_year_pct"),
                                "benchmark_three_year_pct": benchmark.get(
                                    "three_year_pct"
                                ),
                                "benchmark_five_year_pct": benchmark.get(
                                    "five_year_pct"
                                ),
                                "source_type": "cafr",
                            },
                            source_url=source_url,
                            confidence="high",
                        )
                    )

        except Exception as e:
            logger.warning(f"LLM performance extraction failed: {e}")

        return items

    async def _extract_managers_llm(
        self,
        llm: LLMClient,
        text: str,
        lp_id: int,
        lp_name: str,
        source_url: str,
    ) -> List[CollectedItem]:
        """Extract external managers using LLM."""
        items = []

        # Find relevant text chunks
        manager_keywords = [
            "investment manager",
            "external manager",
            "investment adviser",
            "mandate",
            "portfolio manager",
            "sub-adviser",
        ]

        chunk = self._find_relevant_chunk(text, manager_keywords)
        if not chunk:
            return items  # Skip if no manager section found

        try:
            prompt = MANAGERS_EXTRACTION_PROMPT.format(text=chunk)
            response = await llm.complete(
                prompt=prompt,
                system_prompt="You extract structured financial data from pension fund documents. Return valid JSON only.",
            )

            data = response.parse_json()
            if data:
                managers = data.get("managers", [])

                for mgr in managers[:50]:  # Limit
                    items.append(
                        CollectedItem(
                            item_type="manager_relationship",
                            data={
                                "lp_id": lp_id,
                                "lp_name": lp_name,
                                "manager_name": mgr.get("name"),
                                "asset_class": mgr.get("asset_class"),
                                "mandate_type": mgr.get("mandate_type"),
                                "commitment_usd": mgr.get("commitment_usd"),
                                "source_type": "cafr",
                            },
                            source_url=source_url,
                            confidence="medium",  # LLM extraction is less certain
                        )
                    )

        except Exception as e:
            logger.warning(f"LLM manager extraction failed: {e}")

        return items

    def _find_relevant_chunk(
        self,
        text: str,
        keywords: List[str],
        chunk_size: int = None,
    ) -> Optional[str]:
        """Find text chunk containing relevant keywords."""
        chunk_size = chunk_size or self.TEXT_CHUNK_SIZE
        text_lower = text.lower()

        # Find positions of keyword matches
        positions = []
        for keyword in keywords:
            for match in re.finditer(re.escape(keyword.lower()), text_lower):
                positions.append(match.start())

        if not positions:
            return None

        # Find best starting position (most keywords nearby)
        positions.sort()
        best_start = positions[0]
        best_count = 1

        for start_pos in positions:
            end_pos = start_pos + chunk_size
            count = sum(1 for p in positions if start_pos <= p < end_pos)
            if count > best_count:
                best_count = count
                best_start = start_pos

        # Extract chunk with some context before
        chunk_start = max(0, best_start - 500)
        chunk_end = min(len(text), chunk_start + chunk_size)

        return text[chunk_start:chunk_end]

    def _extract_with_regex(
        self,
        text: str,
        lp_id: int,
        lp_name: str,
        source_url: str,
        fiscal_year: Optional[int],
    ) -> List[CollectedItem]:
        """Fallback regex-based extraction when LLM not available."""
        items = []

        # Extract returns using regex patterns
        return_patterns = [
            (r"1[- ]?year.*?(-?\d{1,2}\.?\d*)%", "one_year_return_pct"),
            (r"3[- ]?year.*?(-?\d{1,2}\.?\d*)%", "three_year_return_pct"),
            (r"5[- ]?year.*?(-?\d{1,2}\.?\d*)%", "five_year_return_pct"),
            (r"10[- ]?year.*?(-?\d{1,2}\.?\d*)%", "ten_year_return_pct"),
        ]

        returns = {"lp_id": lp_id, "fiscal_year": fiscal_year, "source_type": "cafr"}

        for pattern, field in return_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                returns[field] = match.group(1)

        if any(v for k, v in returns.items() if k.endswith("_pct")):
            items.append(
                CollectedItem(
                    item_type="performance_return",
                    data=returns,
                    source_url=source_url,
                    confidence="medium",  # Regex is less reliable
                )
            )

        return items
