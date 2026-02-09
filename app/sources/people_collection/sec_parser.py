"""
SEC filing parser for extracting leadership data.

Parses DEF 14A proxy statements and 8-K filings to extract:
- Named Executive Officers
- Board of Directors
- Compensation data
- Leadership changes
"""

import re
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import date
from dataclasses import dataclass, field

from bs4 import BeautifulSoup

from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    ExtractionConfidence,
    TitleLevel,
    ChangeType,
)
from app.sources.people_collection.llm_extractor import LLMExtractor

logger = logging.getLogger(__name__)


@dataclass
class CompensationData:
    """Executive compensation from proxy statement."""
    name: str
    title: str
    fiscal_year: int
    base_salary: Optional[float] = None
    bonus: Optional[float] = None
    stock_awards: Optional[float] = None
    option_awards: Optional[float] = None
    non_equity_incentive: Optional[float] = None
    pension_change: Optional[float] = None
    other_compensation: Optional[float] = None
    total: Optional[float] = None


@dataclass
class ProxyParseResult:
    """Result of parsing a DEF 14A proxy statement."""
    executives: List[ExtractedPerson] = field(default_factory=list)
    board_members: List[ExtractedPerson] = field(default_factory=list)
    compensation: List[CompensationData] = field(default_factory=list)
    company_name: str = ""
    fiscal_year_end: Optional[date] = None
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM
    extraction_notes: List[str] = field(default_factory=list)


@dataclass
class Form8KParseResult:
    """Result of parsing an 8-K filing."""
    changes: List[LeadershipChange] = field(default_factory=list)
    items_found: List[str] = field(default_factory=list)
    extraction_confidence: ExtractionConfidence = ExtractionConfidence.MEDIUM


class SECParser:
    """
    Parses SEC filings to extract leadership information.

    Uses a combination of:
    1. Pattern matching for structured sections
    2. HTML/table parsing for compensation data
    3. LLM extraction for unstructured text
    """

    def __init__(self):
        self.llm_extractor = LLMExtractor()

    def _clean_html(self, html: str) -> str:
        """Remove HTML tags and clean text."""
        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style elements
        for element in soup.find_all(['script', 'style']):
            element.decompose()

        text = soup.get_text(separator='\n')

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        return text.strip()

    def _extract_section(
        self,
        text: str,
        start_patterns: List[str],
        end_patterns: List[str],
        max_length: int = 50000,
    ) -> Optional[str]:
        """Extract a section of text between patterns."""
        text_lower = text.lower()

        # Find start
        start_pos = -1
        for pattern in start_patterns:
            pos = text_lower.find(pattern.lower())
            if pos >= 0:
                start_pos = pos
                break

        if start_pos < 0:
            return None

        # Find end
        end_pos = len(text)
        search_start = start_pos + 100  # Skip past the header

        for pattern in end_patterns:
            pos = text_lower.find(pattern.lower(), search_start)
            if pos > 0 and pos < end_pos:
                end_pos = pos

        section = text[start_pos:end_pos]

        # Limit length
        if len(section) > max_length:
            section = section[:max_length]

        return section

    async def parse_proxy_statement(
        self,
        content: str,
        company_name: str,
        cik: str,
    ) -> ProxyParseResult:
        """
        Parse a DEF 14A proxy statement.

        Extracts:
        - Named Executive Officers from compensation tables
        - Board of Directors from governance section
        - Compensation data from Summary Compensation Table
        """
        result = ProxyParseResult(company_name=company_name)

        # Clean HTML if present
        if '<html' in content.lower() or '<body' in content.lower():
            text = self._clean_html(content)
        else:
            text = content

        # Try to extract key sections
        sections_found = []

        # 1. Named Executive Officers section
        neo_section = self._extract_section(
            text,
            start_patterns=[
                "named executive officers",
                "executive officers",
                "our executive officers",
                "executive compensation",
            ],
            end_patterns=[
                "board of directors",
                "director compensation",
                "security ownership",
                "equity compensation",
            ],
            max_length=30000,
        )

        if neo_section:
            sections_found.append("NEO")
            # Extract executives from this section
            execs = await self._extract_executives_from_section(
                neo_section, company_name, is_board=False
            )
            result.executives.extend(execs)

        # 2. Board of Directors section
        board_section = self._extract_section(
            text,
            start_patterns=[
                "board of directors",
                "election of directors",
                "proposal 1",
                "our directors",
            ],
            end_patterns=[
                "executive compensation",
                "named executive officers",
                "audit committee",
                "security ownership",
            ],
            max_length=30000,
        )

        if board_section:
            sections_found.append("Board")
            # Extract board members
            board = await self._extract_executives_from_section(
                board_section, company_name, is_board=True
            )
            result.board_members.extend(board)

        # 3. Summary Compensation Table
        comp_section = self._extract_section(
            text,
            start_patterns=[
                "summary compensation table",
                "summary of compensation",
            ],
            end_patterns=[
                "grants of plan-based awards",
                "outstanding equity awards",
                "option exercises",
                "pension benefits",
            ],
            max_length=20000,
        )

        if comp_section:
            sections_found.append("Compensation")
            # Try to parse compensation table
            comp_data = self._parse_compensation_table(comp_section)
            result.compensation.extend(comp_data)

        # If structured extraction didn't find much, use LLM
        if len(result.executives) < 3 and len(result.board_members) < 3:
            logger.debug("Using LLM for proxy extraction")
            llm_result = await self.llm_extractor.extract_from_sec_proxy(
                text[:100000], company_name, cik
            )

            if llm_result:
                # Parse LLM results
                for exec_data in llm_result.get("executives", []):
                    person = self._person_from_dict(exec_data, company_name, is_board=False)
                    if person and person.full_name not in [e.full_name for e in result.executives]:
                        result.executives.append(person)

                for board_data in llm_result.get("board_members", []):
                    person = self._person_from_dict(board_data, company_name, is_board=True)
                    if person and person.full_name not in [b.full_name for b in result.board_members]:
                        result.board_members.append(person)

        result.extraction_notes = sections_found
        result.extraction_confidence = (
            ExtractionConfidence.HIGH if len(sections_found) >= 2
            else ExtractionConfidence.MEDIUM if sections_found
            else ExtractionConfidence.LOW
        )

        logger.info(
            f"Parsed proxy for {company_name}: "
            f"{len(result.executives)} executives, "
            f"{len(result.board_members)} board members"
        )

        return result

    async def _extract_executives_from_section(
        self,
        section: str,
        company_name: str,
        is_board: bool,
    ) -> List[ExtractedPerson]:
        """Extract executives or board members from a text section."""
        people = []

        # Common patterns for executive listings
        # Pattern: "NAME, AGE, TITLE" or "NAME - TITLE"
        patterns = [
            # "John Smith, 55, Chief Executive Officer"
            r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),?\s*(?:age\s*)?(\d{2,3})?,?\s*(?:has served as|serves as|is our|is the)?\s*([A-Z][^.]{10,100}?)(?:\.|since|\()',

            # "Mr. Smith has served as CEO"
            r'(?:Mr\.|Ms\.|Mrs\.)\s+([A-Z][a-z]+)\s+(?:has served|serves|is)\s+(?:as\s+)?(?:our\s+)?([A-Z][^.]{10,80}?)(?:\s+since|\s+from|\.)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, section)
            for match in matches[:20]:  # Limit to prevent noise
                try:
                    if len(match) >= 2:
                        name = match[0].strip()
                        title = match[-1].strip() if match[-1] else ""

                        # Validate name
                        if not self._is_valid_name(name):
                            continue

                        # Clean title
                        title = self._clean_title(title)
                        if not title or len(title) < 3:
                            continue

                        person = ExtractedPerson(
                            full_name=name,
                            title=title,
                            is_board_member=is_board,
                            is_executive=not is_board,
                            confidence=ExtractionConfidence.MEDIUM,
                            title_level=self._infer_title_level(title),
                        )

                        # Avoid duplicates
                        if name not in [p.full_name for p in people]:
                            people.append(person)

                except Exception as e:
                    logger.debug(f"Pattern match error: {e}")

        return people

    def _is_valid_name(self, name: str) -> bool:
        """Validate a potential person name."""
        if not name or len(name) < 4:
            return False

        words = name.split()
        if len(words) < 2 or len(words) > 5:
            return False

        # Should be mostly letters
        letters = sum(1 for c in name if c.isalpha())
        if letters < len(name) * 0.7:
            return False

        # Common false positives
        false_positives = [
            'fiscal year', 'annual report', 'form 10', 'item',
            'table of', 'part', 'section', 'pursuant',
        ]
        if any(fp in name.lower() for fp in false_positives):
            return False

        return True

    def _clean_title(self, title: str) -> str:
        """Clean and normalize a title string."""
        # Remove common noise
        noise = [
            'and has', 'he has', 'she has', 'who has',
            'and is', 'and was', 'and will', 'effective',
            'prior to', 'before', 'after', 'during',
        ]

        title_clean = title
        for n in noise:
            if n in title_clean.lower():
                idx = title_clean.lower().find(n)
                title_clean = title_clean[:idx]

        return title_clean.strip().rstrip(',').strip()

    def _infer_title_level(self, title: str) -> TitleLevel:
        """Infer title level from title string."""
        if not title:
            return TitleLevel.UNKNOWN

        title_lower = title.lower()
        title_spaced = f' {title_lower} '

        # C-Suite
        if any(kw in title_spaced for kw in [' chief ', ' ceo ', ' cfo ', ' coo ', ' cto ']):
            return TitleLevel.C_SUITE

        # EVP
        if 'executive vice president' in title_lower or ' evp ' in title_spaced:
            return TitleLevel.EVP

        # SVP
        if 'senior vice president' in title_lower or ' svp ' in title_spaced:
            return TitleLevel.SVP

        # VP
        if 'vice president' in title_lower:
            return TitleLevel.VP

        # President
        if 'president' in title_lower:
            return TitleLevel.PRESIDENT

        # Board
        if 'director' in title_lower or 'chairman' in title_lower:
            if 'director of' not in title_lower:
                return TitleLevel.BOARD

        return TitleLevel.UNKNOWN

    def _person_from_dict(
        self,
        data: Dict[str, Any],
        company_name: str,
        is_board: bool,
    ) -> Optional[ExtractedPerson]:
        """Create ExtractedPerson from dictionary (LLM output)."""
        name = data.get("full_name") or data.get("name")
        if not name:
            return None

        title = data.get("title", "")

        return ExtractedPerson(
            full_name=name,
            title=title,
            bio=data.get("bio"),
            is_board_member=is_board or data.get("is_board_member", False),
            is_executive=not is_board or data.get("is_executive", False),
            confidence=ExtractionConfidence.MEDIUM,
            title_level=self._infer_title_level(title),
        )

    def _parse_compensation_table(
        self,
        section: str,
    ) -> List[CompensationData]:
        """Parse a compensation table section."""
        compensation = []

        # Look for table rows with compensation data
        # Pattern: NAME | YEAR | SALARY | BONUS | STOCK | OPTIONS | TOTAL
        lines = section.split('\n')

        current_name = ""
        for line in lines:
            # Skip empty lines
            if not line.strip():
                continue

            # Check if line starts with a name (capitalized words)
            words = line.split()
            if not words:
                continue

            # Try to find name at start
            if words[0][0].isupper() and len(words[0]) > 2:
                potential_name = []
                for w in words[:4]:
                    if w[0].isupper() and w.replace('.', '').replace(',', '').isalpha():
                        potential_name.append(w)
                    else:
                        break

                if len(potential_name) >= 2:
                    current_name = ' '.join(potential_name)

            # Try to extract numbers (compensation values)
            if current_name:
                numbers = re.findall(r'\$?([\d,]+(?:\.\d{2})?)', line)
                if len(numbers) >= 3:
                    try:
                        # Assume order: salary, bonus, stock, options, other, total
                        comp = CompensationData(
                            name=current_name,
                            title="",
                            fiscal_year=date.today().year - 1,
                        )

                        values = [float(n.replace(',', '')) for n in numbers]

                        if len(values) >= 1:
                            comp.base_salary = values[0]
                        if len(values) >= 2:
                            comp.bonus = values[1]
                        if len(values) >= 3:
                            comp.stock_awards = values[2]
                        if len(values) >= 6:
                            comp.total = values[-1]

                        # Only add if salary is reasonable ($50K - $50M)
                        if comp.base_salary and 50000 <= comp.base_salary <= 50000000:
                            compensation.append(comp)
                            current_name = ""  # Reset for next person

                    except (ValueError, IndexError):
                        pass

        return compensation

    async def parse_10k_executives(
        self,
        content: str,
        company_name: str,
    ) -> List[ExtractedPerson]:
        """
        Parse executives from 10-K Item 10 (Directors, Executive Officers and Corporate Governance).

        This section typically lists ALL Section 16 officers (15-25 people),
        more than the 5-7 NEOs in the proxy statement.

        Args:
            content: Full 10-K filing text/HTML
            company_name: Company name for context

        Returns:
            List of extracted executive officers
        """
        # Clean HTML if present
        if '<html' in content.lower() or '<body' in content.lower():
            text = self._clean_html(content)
        else:
            text = content

        # Strategy 1: Look for the executive officers listing section
        # (often separate from the brief "Item 10" header which may say "incorporated by reference")
        text_lower = text.lower()
        item10_section = None

        # First, try to find the actual executive officers listing
        # This pattern appears when the 10-K includes the exec list directly
        exec_listing_patterns = [
            f"the executive officers of {company_name.lower().split()[0]}",
            "executive officers of the registrant",
            "executive officers of the company",
            "the following table sets forth information regarding",
            "set forth below is certain information concerning",
        ]

        for pattern in exec_listing_patterns:
            idx = text_lower.find(pattern)
            if idx >= 0:
                # Find the end of this section (next major heading or "Item 11")
                end_idx = len(text)
                for end_term in ["item 11", "part iv", "item 12", "signatures"]:
                    end_pos = text_lower.find(end_term, idx + 200)
                    if end_pos > 0 and end_pos < end_idx:
                        end_idx = end_pos
                item10_section = text[idx:min(idx + 50000, end_idx)]
                logger.info(f"[SECParser] Found exec listing via pattern '{pattern}' at position {idx}")
                break

        # Strategy 2: Fall back to standard Item 10 section extraction
        if not item10_section:
            item10_section = self._extract_section(
                text,
                start_patterns=[
                    "directors, executive officers and corporate governance",
                    "directors, executive officers",
                    "directors and executive officers",
                    "executive officers and directors",
                ],
                end_patterns=[
                    "item 11",
                    "executive compensation",
                    "security ownership",
                    "certain relationships",
                ],
                max_length=50000,
            )

        if not item10_section:
            logger.warning(f"[SECParser] No Item 10 section found in 10-K for {company_name}")
            return []

        logger.info(f"[SECParser] Found Item 10 section: {len(item10_section)} chars")

        # 10-K executive sections are typically tabular (Name | Age | Title)
        # which is hard for regex but reliable for LLM. Use LLM directly.
        people: List[ExtractedPerson] = []

        try:
            prompt = (
                f"Extract ALL executive officers listed in this 10-K filing section for {company_name}. "
                f"This is the 'Directors, Executive Officers and Corporate Governance' section. "
                f"Return a JSON object with an 'executives' array. Each executive should have: "
                f"'full_name' (the person's full legal name), "
                f"'title' (their exact corporate title), "
                f"'age' (integer, if listed), "
                f"'bio' (1-2 sentence summary if biographical info is available). "
                f"Include ALL officers listed, not just the top 5. "
                f"Do NOT include non-person entries like company names.\n\n"
                f"TEXT:\n{item10_section[:80000]}"
            )

            response = await self.llm_extractor._call_llm(prompt)
            if response:
                parsed = self.llm_extractor._parse_json_response(response)
                if parsed and isinstance(parsed, dict):
                    for exec_data in parsed.get("executives", []):
                        person = self._person_from_dict(exec_data, company_name, is_board=False)
                        if person:
                            person.extraction_notes = "From 10-K Item 10 (LLM)"
                            person.confidence = ExtractionConfidence.HIGH
                            people.append(person)
        except Exception as e:
            logger.warning(f"[SECParser] LLM extraction failed for 10-K: {e}")

        # Fallback to pattern extraction if LLM failed
        if not people:
            logger.info("[SECParser] LLM failed, falling back to pattern extraction")
            people = await self._extract_executives_from_section(
                item10_section, company_name, is_board=False
            )

        # Mark all with source info
        for person in people:
            if not person.extraction_notes:
                person.extraction_notes = "From 10-K Item 10"

        logger.info(f"[SECParser] 10-K extraction: {len(people)} executives for {company_name}")
        return people

    async def parse_8k_filing(
        self,
        content: str,
        company_name: str,
        filing_date: date,
    ) -> Form8KParseResult:
        """
        Parse an 8-K filing for leadership changes.

        Looks for Item 5.02 (Departure/Appointment of Directors/Officers).
        """
        result = Form8KParseResult()

        # Clean HTML if present
        if '<html' in content.lower():
            text = self._clean_html(content)
        else:
            text = content

        text_lower = text.lower()

        # Check for relevant items
        items_found = []
        if 'item 5.02' in text_lower:
            items_found.append('5.02')
        if 'item 5.03' in text_lower:
            items_found.append('5.03')
        if 'item 5.01' in text_lower:
            items_found.append('5.01')

        result.items_found = items_found

        if not items_found:
            result.extraction_confidence = ExtractionConfidence.LOW
            return result

        # Extract Item 5.02 section
        section_502 = self._extract_section(
            text,
            start_patterns=["item 5.02"],
            end_patterns=["item 5.03", "item 6", "item 7", "item 8", "item 9", "signature"],
            max_length=20000,
        )

        if section_502:
            changes = await self._extract_changes_from_8k(
                section_502, company_name, filing_date
            )
            result.changes.extend(changes)

        result.extraction_confidence = (
            ExtractionConfidence.HIGH if result.changes
            else ExtractionConfidence.MEDIUM if items_found
            else ExtractionConfidence.LOW
        )

        return result

    async def _extract_changes_from_8k(
        self,
        section: str,
        company_name: str,
        filing_date: date,
    ) -> List[LeadershipChange]:
        """Extract leadership changes from 8-K Item 5.02 section."""
        changes = []

        # Use LLM for extraction
        try:
            llm_changes = await self.llm_extractor.extract_changes_from_press_release(
                section, company_name, filing_date.isoformat()
            )
            for change in llm_changes:
                change.source_type = "8k_filing"
                change.announced_date = filing_date
                changes.append(change)
        except Exception as e:
            logger.warning(f"LLM extraction failed for 8-K: {e}")

        # Also try pattern matching for common announcements
        patterns = [
            # Resignation
            (r'(\w+\s+\w+(?:\s+\w+)?)\s+(?:has\s+)?resign(?:ed|ing)\s+(?:from\s+)?(?:the\s+)?(?:position\s+of\s+)?([^.]+)',
             ChangeType.DEPARTURE),

            # Appointment
            (r'(?:appointed|named)\s+(\w+\s+\w+(?:\s+\w+)?)\s+(?:as\s+)?(?:the\s+)?([^.]+)',
             ChangeType.HIRE),

            # Retirement
            (r'(\w+\s+\w+(?:\s+\w+)?)\s+(?:has\s+)?(?:announced|will)\s+(?:his|her|their)\s+retire',
             ChangeType.RETIREMENT),
        ]

        for pattern, change_type in patterns:
            matches = re.findall(pattern, section, re.IGNORECASE)
            for match in matches[:5]:
                try:
                    name = match[0].strip()
                    title = match[1].strip() if len(match) > 1 else ""

                    if not self._is_valid_name(name):
                        continue

                    # Check if already found by LLM
                    if any(c.person_name.lower() == name.lower() for c in changes):
                        continue

                    change = LeadershipChange(
                        person_name=name,
                        change_type=change_type,
                        new_title=title if change_type == ChangeType.HIRE else None,
                        old_title=title if change_type == ChangeType.DEPARTURE else None,
                        announced_date=filing_date,
                        source_type="8k_filing",
                        confidence=ExtractionConfidence.MEDIUM,
                    )
                    changes.append(change)

                except Exception as e:
                    logger.debug(f"Pattern match error: {e}")

        return changes
