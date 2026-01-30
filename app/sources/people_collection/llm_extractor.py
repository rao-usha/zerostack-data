"""
LLM-based data extraction for people collection.

Uses Claude or GPT-4 to extract structured leadership data from:
- HTML pages
- Press releases
- SEC filings
- Executive bios
"""

import json
import logging
import re
from typing import Optional, List, Dict, Any
from datetime import datetime

from app.sources.people_collection.types import (
    ExtractedPerson,
    LeadershipChange,
    LeadershipPageResult,
    ParsedBio,
    ExtractionConfidence,
    TitleLevel,
    ChangeType,
)
from app.sources.people_collection.config import (
    get_llm_config,
    LEADERSHIP_EXTRACTION_PROMPT,
    PRESS_RELEASE_EXTRACTION_PROMPT,
    BIO_PARSING_PROMPT,
    SEC_PROXY_EXTRACTION_PROMPT,
    TITLE_NORMALIZATIONS,
    TITLE_LEVEL_KEYWORDS,
)

logger = logging.getLogger(__name__)


class LLMExtractor:
    """
    Extracts structured data from text using LLMs.

    Supports both Anthropic (Claude) and OpenAI (GPT-4) backends.
    """

    def __init__(self):
        self.config = get_llm_config()
        self._client = None

    def _get_client(self):
        """Get or create the LLM client."""
        if self._client is None:
            if self.config.provider == "anthropic":
                import anthropic
                self._client = anthropic.Anthropic()
            else:
                import openai
                self._client = openai.OpenAI()
        return self._client

    async def _call_llm(self, prompt: str) -> Optional[str]:
        """
        Call the LLM with a prompt and return the response.

        Handles both sync clients in an async context.
        """
        import asyncio

        client = self._get_client()

        try:
            if self.config.provider == "anthropic":
                # Run sync client in thread pool
                response = await asyncio.to_thread(
                    client.messages.create,
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.content[0].text
            else:
                # OpenAI
                response = await asyncio.to_thread(
                    client.chat.completions.create,
                    model=self.config.model,
                    max_tokens=self.config.max_tokens,
                    temperature=self.config.temperature,
                    messages=[{"role": "user", "content": prompt}],
                )
                return response.choices[0].message.content

        except Exception as e:
            logger.error(f"LLM call failed: {e}")
            return None

    def _parse_json_response(self, response: str) -> Optional[Dict]:
        """Parse JSON from LLM response, handling common issues."""
        if not response:
            return None

        # Try to find JSON in the response
        # Sometimes LLMs wrap JSON in markdown code blocks
        json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response)
        if json_match:
            response = json_match.group(1)

        # Try direct parse
        try:
            return json.loads(response)
        except json.JSONDecodeError:
            pass

        # Try to find JSON object in response
        json_match = re.search(r'\{[\s\S]*\}', response)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass

        logger.warning(f"Could not parse JSON from response: {response[:200]}...")
        return None

    def _normalize_title(self, title: str) -> str:
        """Normalize a title to standard form."""
        if not title:
            return title

        title_lower = title.lower().strip()

        # Check exact matches
        if title_lower in TITLE_NORMALIZATIONS:
            return TITLE_NORMALIZATIONS[title_lower]

        # Check partial matches
        for pattern, normalized in TITLE_NORMALIZATIONS.items():
            if pattern in title_lower:
                return normalized

        # Return original with title case
        return title.strip()

    def _infer_title_level(self, title: str) -> TitleLevel:
        """Infer the hierarchy level from a title."""
        if not title:
            return TitleLevel.UNKNOWN

        title_lower = title.lower()

        for level, keywords in TITLE_LEVEL_KEYWORDS.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return TitleLevel(level)

        return TitleLevel.UNKNOWN

    def _clean_html_for_llm(self, html: str, max_length: int = 100000) -> str:
        """
        Clean HTML for LLM processing.

        Removes scripts, styles, and unnecessary elements while preserving
        the text content and structure.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style elements
        for element in soup.find_all(['script', 'style', 'noscript', 'iframe']):
            element.decompose()

        # Remove navigation and footer (often noisy)
        for element in soup.find_all(['nav', 'footer', 'header']):
            # Keep if it might contain leadership info
            text = element.get_text().lower()
            if 'leadership' not in text and 'team' not in text and 'management' not in text:
                element.decompose()

        # Get text with some structure preserved
        text = soup.get_text(separator='\n', strip=True)

        # Collapse multiple newlines
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Truncate if too long
        if len(text) > max_length:
            text = text[:max_length] + "\n... [truncated]"

        return text

    async def extract_leadership_from_html(
        self,
        html: str,
        company_name: str,
        page_url: str,
    ) -> LeadershipPageResult:
        """
        Extract leadership information from an HTML page.

        Args:
            html: Raw HTML content
            company_name: Name of the company
            page_url: URL of the page

        Returns:
            LeadershipPageResult with extracted people
        """
        # Clean HTML for LLM
        cleaned_html = self._clean_html_for_llm(html)

        # Build prompt
        prompt = LEADERSHIP_EXTRACTION_PROMPT.format(
            company_name=company_name,
            page_url=page_url,
            html_content=cleaned_html,
        )

        # Call LLM
        response = await self._call_llm(prompt)
        data = self._parse_json_response(response)

        if not data:
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type="unknown",
                people=[],
                extraction_confidence=ExtractionConfidence.LOW,
                extraction_notes="Failed to parse LLM response",
            )

        # Parse people
        people = []
        for p in data.get("people", []):
            try:
                # Normalize and enhance
                title = p.get("title", "")
                title_normalized = p.get("title_normalized") or self._normalize_title(title)
                title_level = p.get("title_level", "unknown")

                if title_level == "unknown":
                    title_level = self._infer_title_level(title).value

                person = ExtractedPerson(
                    full_name=p.get("full_name", ""),
                    title=title,
                    title_normalized=title_normalized,
                    title_level=TitleLevel(title_level) if title_level else TitleLevel.UNKNOWN,
                    department=p.get("department"),
                    bio=p.get("bio"),
                    linkedin_url=p.get("linkedin_url"),
                    email=p.get("email"),
                    photo_url=p.get("photo_url"),
                    reports_to=p.get("reports_to"),
                    is_board_member=p.get("is_board_member", False),
                    is_executive=p.get("is_executive", True),
                    confidence=ExtractionConfidence(data.get("extraction_confidence", "medium")),
                    source_url=page_url,
                )
                people.append(person)
            except Exception as e:
                logger.warning(f"Failed to parse person: {e}")

        return LeadershipPageResult(
            company_name=company_name,
            page_url=page_url,
            page_type=data.get("page_type", "leadership"),
            people=people,
            extraction_confidence=ExtractionConfidence(data.get("extraction_confidence", "medium")),
            extraction_notes=data.get("notes"),
        )

    async def extract_changes_from_press_release(
        self,
        text: str,
        company_name: str,
        release_date: Optional[str] = None,
    ) -> List[LeadershipChange]:
        """
        Extract leadership changes from a press release.

        Args:
            text: Press release text
            company_name: Name of the company
            release_date: Date of the release (YYYY-MM-DD)

        Returns:
            List of detected leadership changes
        """
        prompt = PRESS_RELEASE_EXTRACTION_PROMPT.format(
            company_name=company_name,
            date=release_date or "unknown",
            text=text[:50000],  # Limit length
        )

        response = await self._call_llm(prompt)
        data = self._parse_json_response(response)

        if not data:
            return []

        changes = []
        for c in data.get("changes", []):
            try:
                change_type = c.get("change_type", "hire")
                try:
                    change_type_enum = ChangeType(change_type)
                except ValueError:
                    change_type_enum = ChangeType.HIRE

                effective_date = None
                if c.get("effective_date"):
                    try:
                        from datetime import date
                        effective_date = date.fromisoformat(c["effective_date"])
                    except ValueError:
                        pass

                change = LeadershipChange(
                    person_name=c.get("person_name", ""),
                    change_type=change_type_enum,
                    old_title=c.get("old_title"),
                    new_title=c.get("new_title"),
                    old_company=c.get("old_company"),
                    effective_date=effective_date,
                    reason=c.get("reason"),
                    successor_name=c.get("successor_name"),
                    predecessor_name=c.get("predecessor_name"),
                    is_c_suite=c.get("is_c_suite", False),
                    is_board=c.get("is_board", False),
                    source_type="press_release",
                    confidence=ExtractionConfidence(data.get("extraction_confidence", "medium")),
                )
                changes.append(change)
            except Exception as e:
                logger.warning(f"Failed to parse change: {e}")

        return changes

    async def parse_bio(
        self,
        bio_text: str,
        person_name: str,
        company_name: str,
    ) -> ParsedBio:
        """
        Parse an executive bio into structured data.

        Args:
            bio_text: Raw biography text
            person_name: Name of the person
            company_name: Current company name

        Returns:
            ParsedBio with experience, education, etc.
        """
        prompt = BIO_PARSING_PROMPT.format(
            person_name=person_name,
            company_name=company_name,
            bio_text=bio_text[:10000],
        )

        response = await self._call_llm(prompt)
        data = self._parse_json_response(response)

        if not data:
            return ParsedBio()

        from app.sources.people_collection.types import ExtractedExperience, ExtractedEducation

        experience = []
        for exp in data.get("experience", []):
            try:
                experience.append(ExtractedExperience(
                    company_name=exp.get("company", ""),
                    title=exp.get("title", ""),
                    start_year=exp.get("start_year"),
                    end_year=exp.get("end_year"),
                    is_current=exp.get("is_current", False),
                    description=exp.get("description"),
                ))
            except Exception:
                pass

        education = []
        for edu in data.get("education", []):
            try:
                education.append(ExtractedEducation(
                    institution=edu.get("institution", ""),
                    degree=edu.get("degree"),
                    field_of_study=edu.get("field"),
                    graduation_year=edu.get("graduation_year"),
                ))
            except Exception:
                pass

        return ParsedBio(
            experience=experience,
            education=education,
            board_positions=data.get("board_positions", []),
            certifications=data.get("certifications", []),
            military_service=data.get("military_service"),
            notable_achievements=data.get("notable_achievements", []),
        )

    async def extract_from_sec_proxy(
        self,
        filing_text: str,
        company_name: str,
        cik: str,
    ) -> Dict[str, Any]:
        """
        Extract executive data from SEC DEF 14A proxy statement.

        Args:
            filing_text: Text of the proxy statement
            company_name: Company name
            cik: SEC CIK number

        Returns:
            Dict with executives, board_members, and compensation data
        """
        # Truncate to relevant sections if too long
        if len(filing_text) > 100000:
            # Try to find relevant sections
            sections = []
            for keyword in ["named executive", "compensation", "board of director", "executive officer"]:
                idx = filing_text.lower().find(keyword)
                if idx > 0:
                    sections.append(filing_text[max(0, idx - 1000):idx + 30000])

            if sections:
                filing_text = "\n\n---\n\n".join(sections)
            else:
                filing_text = filing_text[:100000]

        prompt = SEC_PROXY_EXTRACTION_PROMPT.format(
            company_name=company_name,
            cik=cik,
            filing_text=filing_text,
        )

        response = await self._call_llm(prompt)
        data = self._parse_json_response(response)

        if not data:
            return {"executives": [], "board_members": [], "extraction_confidence": "low"}

        return data
