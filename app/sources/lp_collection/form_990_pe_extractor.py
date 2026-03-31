"""
Form 990 PE Extractor — extracts PE fund investments from IRS Form 990 filings.

Targets university endowments and large foundations that file 990s and hold PE funds.
Schedule D Part VIII covers investments in other entities (including PE funds).

Data source: ProPublica Nonprofit Explorer API (free, no key required)
  https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json
  Response includes pdf_url pointing to downloadable IRS PDF filing.
"""
import asyncio
import logging
import re
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Known endowments/foundations with PE portfolios and their EINs
ENDOWMENT_TARGETS = [
    {"name": "Harvard Management Company", "ein": "04-2103580", "short": "Harvard"},
    {"name": "Yale University", "ein": "06-0646973", "short": "Yale"},
    {"name": "Stanford University", "ein": "94-1156365", "short": "Stanford"},
    {"name": "MIT Investment Management Company", "ein": "04-2103594", "short": "MIT"},
    {"name": "Princeton University", "ein": "21-0634501", "short": "Princeton"},
    {"name": "Duke University", "ein": "56-0532129", "short": "Duke"},
    {"name": "University of Michigan", "ein": "38-6006309", "short": "UMich"},
    {"name": "University of Virginia Investment Management Company", "ein": "54-0506458", "short": "UVA"},
    {"name": "MacArthur Foundation", "ein": "23-7093598", "short": "MacArthur"},
    {"name": "Ford Foundation", "ein": "13-1684331", "short": "Ford"},
    {"name": "Rockefeller Foundation", "ein": "13-1659629", "short": "Rockefeller"},
    {"name": "Wellcome Trust", "ein": "13-3948776", "short": "Wellcome"},
]

HEADERS = {
    "User-Agent": "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)",
}

# ProPublica Nonprofit Explorer API — free, no key, returns pdf_url per filing
PROPUBLICA_ORG_API = (
    "https://projects.propublica.org/nonprofits/api/v2/organizations/{ein}.json"
)


class Form990PEExtractor:
    """Extracts PE investments from IRS Form 990 filings via ProPublica Nonprofit Explorer."""

    async def search_990_filing(self, target: dict) -> Optional[str]:
        """
        Find the most recent Form 990 PDF URL for an organization via ProPublica.

        Returns the pdf_url from the most recent filing_with_data, or None.
        """
        ein_numeric = target["ein"].replace("-", "")
        url = PROPUBLICA_ORG_API.format(ein=ein_numeric)
        try:
            async with httpx.AsyncClient(timeout=20, headers=HEADERS) as client:
                resp = await client.get(url)
                if resp.status_code != 200:
                    logger.warning(
                        f"[{target['short']}] ProPublica returned {resp.status_code} for EIN {ein_numeric}"
                    )
                    return None

                data = resp.json()
                filings = data.get("filings_with_data", [])
                if not filings:
                    logger.info(f"[{target['short']}] No filings_with_data from ProPublica")
                    return None

                # Most recent filing first; prefer pdf_url over pdfUrl
                for filing in filings[:3]:
                    pdf_url = filing.get("pdf_url") or filing.get("pdfUrl")
                    if pdf_url:
                        logger.info(
                            f"[{target['short']}] ProPublica PDF: {pdf_url}"
                        )
                        return pdf_url

                logger.info(f"[{target['short']}] Filings found but no pdf_url present")
                return None
        except Exception as e:
            logger.error(f"[{target['short']}] ProPublica search error: {e}")
            return None

    def _extract_pe_investments_from_text(self, text: str, org_name: str) -> list:
        """
        Extract PE fund investments from Form 990 text.
        Delegates to form_990_html_parser for HTML table parsing first.
        Falls back to legacy regex if HTML parsing returns nothing.
        """
        # Try HTML-aware parser first
        try:
            from app.sources.lp_collection.form_990_html_parser import parse_form_990_schedule_d
            records = parse_form_990_schedule_d(text, org_name)
            if records:
                logger.info(f"  {org_name}: HTML parser found {len(records)} PE records")
                return records
        except ImportError:
            logger.debug("form_990_html_parser not available, using legacy regex")
        except Exception as e:
            logger.warning(f"HTML parser error for {org_name}: {e}")

        # Legacy regex fallback
        results = []
        lines = text.split('\n')
        in_pe_section = False
        for line in lines:
            line_clean = line.strip()
            if any(kw in line_clean.lower() for kw in [
                'private equity', 'venture capital', 'alternative investment',
                'schedule d', 'investments - other', 'other investments',
                'partnership interests'
            ]):
                in_pe_section = True
                continue
            if in_pe_section and (not line_clean or line_clean.startswith('Part ')):
                in_pe_section = False
                continue
            if not in_pe_section:
                continue
            m = re.match(
                r'^([A-Z][A-Za-z0-9\s,\.\-&]+(?:Fund|Partners|Capital|Ventures|LP|LLC|L\.P\.)[^$\d]*)'
                r'\s+([\d,]+(?:\.\d+)?)\s*$',
                line_clean
            )
            if m:
                fund_name = m.group(1).strip()
                book_value_str = m.group(2).replace(',', '')
                try:
                    book_value = float(book_value_str)
                    if book_value > 1_000_000:
                        manager_match = re.match(
                            r'^((?:\w+\s+){1,3})(?:Fund|Partners|Capital)', fund_name
                        )
                        results.append({
                            "fund_name": fund_name,
                            "gp_name": manager_match.group(1).strip() if manager_match else fund_name.split()[0],
                            "fair_value_usd": book_value,
                            "lp_name": org_name,
                            "data_source": "form_990",
                        })
                except ValueError:
                    pass
        return results

    async def collect_for_endowment(self, target: dict) -> list:
        """
        Collect PE investments for a specific endowment.

        Flow:
          1. Fetch PDF URL from ProPublica Nonprofit Explorer
          2. Download the PDF
          3. Extract text (pdfplumber → OCR fallback for scanned PDFs)
          4. Parse PE investments from text
        """
        logger.info(f"Collecting Form 990 PE data for {target['short']}")

        pdf_url = await self.search_990_filing(target)
        if not pdf_url:
            logger.warning(f"No 990 PDF found for {target['name']}")
            return []

        try:
            async with httpx.AsyncClient(
                timeout=120, headers=HEADERS, follow_redirects=True
            ) as client:
                resp = await client.get(pdf_url)
                if resp.status_code != 200:
                    logger.warning(
                        f"  {target['short']}: PDF download returned {resp.status_code}"
                    )
                    return []

                pdf_bytes = resp.content
                logger.info(f"  {target['short']}: downloaded {len(pdf_bytes):,} bytes")

        except Exception as e:
            logger.error(f"Error downloading 990 PDF for {target['name']}: {e}")
            return []

        # Extract text with OCR fallback.
        # Form 990 Schedule D (investments) is in the FIRST half of the document,
        # not the back — use ocr_start_pct=0.1 so OCR covers pages ~10%-60%.
        from app.sources.lp_collection.pension_cafr_collector import extract_text_from_pdf
        text = extract_text_from_pdf(
            pdf_bytes,
            label=target["short"],
            ocr_start_pct=0.1,
            ocr_end_pct=0.7,
        )
        if not text:
            logger.warning(f"  {target['short']}: no text extracted from PDF")
            return []

        records = self._extract_pe_investments_from_text(text, target['short'])

        # LLM fallback: structured parsing (HTML + regex) works poorly on OCR text
        if not records:
            logger.info(f"  {target['short']}: structured parse found 0 — trying LLM extraction")
            from app.sources.lp_collection.pension_cafr_collector import _extract_pe_from_text
            raw = await _extract_pe_from_text(text, target['short'])
            for rec in raw:
                manager = rec.get("manager_name", "").strip()
                if manager:
                    records.append({
                        "fund_name": (rec.get("fund_name") or "").strip(),
                        "gp_name": manager,
                        "fair_value_usd": rec.get("fair_value_usd"),
                        "commitment_amount_usd": rec.get("commitment_amount_usd"),
                        "vintage_year": rec.get("vintage_year"),
                        "lp_name": target["short"],
                        "data_source": "form_990",
                    })

        logger.info(f"  {target['short']}: found {len(records)} PE investment records")
        return records

    async def collect_all(self) -> list:
        """Collect PE investments from all configured endowments."""
        all_records = []
        for target in ENDOWMENT_TARGETS:
            await asyncio.sleep(1.0)  # rate limit
            try:
                records = await self.collect_for_endowment(target)
                all_records.extend(records)
            except Exception as e:
                logger.error(f"Failed {target['short']}: {e}")
        return all_records
