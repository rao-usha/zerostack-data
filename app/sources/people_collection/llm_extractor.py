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

    async def _call_llm(self, prompt: str, retry_count: int = 0) -> Optional[str]:
        """
        Call the LLM with a prompt and return the response.

        Handles both sync clients in an async context.
        Includes retry logic for transient failures.
        """
        import asyncio

        client = self._get_client()
        max_retries = self.config.retry_attempts

        # Log prompt details for debugging
        logger.info(
            f"[LLMExtractor] Calling {self.config.provider}/{self.config.model} "
            f"with prompt length: {len(prompt)} chars"
        )
        logger.debug(f"[LLMExtractor] Prompt preview: {prompt[:500]}...")

        for attempt in range(max_retries):
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
                    result = response.content[0].text
                else:
                    # OpenAI
                    response = await asyncio.to_thread(
                        client.chat.completions.create,
                        model=self.config.model,
                        max_tokens=self.config.max_tokens,
                        temperature=self.config.temperature,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    result = response.choices[0].message.content

                logger.info(f"[LLMExtractor] LLM response received: {len(result) if result else 0} chars")
                logger.debug(f"[LLMExtractor] Response preview: {result[:500] if result else 'None'}...")
                return result

            except Exception as e:
                logger.warning(f"[LLMExtractor] LLM call attempt {attempt + 1}/{max_retries} failed: {e}")
                import traceback
                logger.debug(f"[LLMExtractor] Error traceback: {traceback.format_exc()}")
                if attempt < max_retries - 1:
                    wait_time = self.config.retry_delay_seconds * (2 ** attempt)
                    logger.info(f"[LLMExtractor] Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"[LLMExtractor] LLM call failed after {max_retries} attempts: {e}")
                    return None

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

        Uses a two-pass approach:
        1. First try with standard prompt
        2. If empty, retry with simplified/focused prompt

        Args:
            html: Raw HTML content (already cleaned text)
            company_name: Name of the company
            page_url: URL of the page

        Returns:
            LeadershipPageResult with extracted people
        """
        # The html parameter is actually already cleaned text from HTMLCleaner
        cleaned_text = html
        text_length = len(cleaned_text)

        logger.info(f"[LLMExtractor] Extracting from {page_url} ({text_length} chars)")

        # Build prompt
        prompt = LEADERSHIP_EXTRACTION_PROMPT.format(
            company_name=company_name,
            page_url=page_url,
            html_content=cleaned_text,
        )

        # Call LLM
        response = await self._call_llm(prompt)

        if not response:
            logger.warning(f"[LLMExtractor] No response from LLM for {page_url}")
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type="unknown",
                people=[],
                extraction_confidence=ExtractionConfidence.LOW,
                extraction_notes="LLM returned no response",
            )

        data = self._parse_json_response(response)

        if not data:
            logger.warning(
                f"[LLMExtractor] Failed to parse JSON from LLM response for {page_url}. "
                f"Full response: {response[:1000]}..."
            )
            return LeadershipPageResult(
                company_name=company_name,
                page_url=page_url,
                page_type="unknown",
                people=[],
                extraction_confidence=ExtractionConfidence.LOW,
                extraction_notes=f"Failed to parse LLM response as JSON. Response: {response[:200]}...",
            )

        # Log parsed data
        logger.info(
            f"[LLMExtractor] Parsed JSON for {page_url}: "
            f"people count={len(data.get('people', []))}, "
            f"confidence={data.get('extraction_confidence')}, "
            f"page_type={data.get('page_type')}"
        )
        if data.get('notes'):
            logger.info(f"[LLMExtractor] LLM notes: {data.get('notes')}")

        # Parse people
        people = self._parse_people_from_response(data, page_url)

        # If no people found, try with simplified prompt
        if len(people) == 0 and text_length > 100:
            logger.info(f"[LLMExtractor] No people found, trying simplified extraction for {page_url}")
            people = await self._try_simplified_extraction(cleaned_text, company_name, page_url)

        logger.info(f"[LLMExtractor] Extracted {len(people)} people from {page_url}")

        return LeadershipPageResult(
            company_name=company_name,
            page_url=page_url,
            page_type=data.get("page_type", "leadership"),
            people=people,
            extraction_confidence=ExtractionConfidence(data.get("extraction_confidence", "medium")),
            extraction_notes=data.get("notes"),
        )

    def _parse_people_from_response(
        self,
        data: Dict,
        page_url: str,
    ) -> List[ExtractedPerson]:
        """Parse people from LLM response data."""
        people = []
        raw_people = data.get("people", [])
        skipped_count = 0

        logger.debug(f"[LLMExtractor] Parsing {len(raw_people)} raw people from response")

        for i, p in enumerate(raw_people):
            try:
                # Normalize and enhance
                title = p.get("title", "")
                title_normalized = p.get("title_normalized") or self._normalize_title(title)
                title_level = p.get("title_level", "unknown")

                if title_level == "unknown":
                    title_level = self._infer_title_level(title).value

                full_name = p.get("full_name", "").strip()

                # Skip invalid names
                if not full_name or len(full_name) < 3:
                    logger.debug(f"[LLMExtractor] Skipping person {i}: invalid name '{full_name}'")
                    skipped_count += 1
                    continue

                person = ExtractedPerson(
                    full_name=full_name,
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
                logger.debug(f"[LLMExtractor] Parsed person: {full_name} - {title}")
            except Exception as e:
                logger.warning(f"[LLMExtractor] Failed to parse person {i}: {e}, data: {p}")
                skipped_count += 1

        logger.info(
            f"[LLMExtractor] Parsed {len(people)} valid people, skipped {skipped_count} "
            f"(from {len(raw_people)} raw)"
        )
        return people

    async def _try_simplified_extraction(
        self,
        text: str,
        company_name: str,
        page_url: str,
    ) -> List[ExtractedPerson]:
        """
        Try extraction with a simplified, more focused prompt.

        Used as fallback when standard extraction returns empty.
        """
        # Use a simplified prompt that's more direct
        simplified_prompt = f"""Extract all people with their job titles from this text.

Company: {company_name}

Return JSON format:
{{"people": [{{"full_name": "First Last", "title": "Job Title"}}]}}

Rules:
1. Only include people who work at {company_name}
2. Each person must have both a name and title
3. Return valid JSON only

Text:
{text[:50000]}"""

        response = await self._call_llm(simplified_prompt)
        if not response:
            return []

        data = self._parse_json_response(response)
        if not data:
            return []

        people = []
        for p in data.get("people", []):
            full_name = p.get("full_name", "").strip()
            title = p.get("title", "").strip()

            if full_name and len(full_name) >= 3 and title:
                people.append(ExtractedPerson(
                    full_name=full_name,
                    title=title,
                    title_level=self._infer_title_level(title),
                    confidence=ExtractionConfidence.LOW,
                    source_url=page_url,
                    extraction_notes="Extracted via simplified fallback prompt",
                ))

        logger.info(f"[LLMExtractor] Simplified extraction found {len(people)} people")
        return people

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
        # Always try to find and extract relevant sections
        # SEC proxy filings are very long, so we need to find the right sections
        sections = []
        filing_text_lower = filing_text.lower()

        # Strategy 1: Find "Director Since" markers which only appear in actual bios
        # This avoids TOC entries which contain keywords but no actual names
        director_since_positions = []
        search_start = 0
        while True:
            idx = filing_text_lower.find("director since", search_start)
            if idx < 0:
                break
            director_since_positions.append(idx)
            search_start = idx + 20
            if len(director_since_positions) >= 20:  # Limit search
                break

        if director_since_positions:
            # Found director bios - extract from first occurrence with large context
            first_bio = min(director_since_positions)
            # Go back to find the start of the bio section (look for name before "Director Since")
            start = max(0, first_bio - 2000)
            # Extract a large section covering multiple director bios
            end = min(len(filing_text), first_bio + 50000)
            section = filing_text[start:end]
            sections.append(f"--- Section: DIRECTOR BIOGRAPHIES ---\n{section}")
            logger.info(f"[LLMExtractor] Found {len(director_since_positions)} director bios starting at {first_bio}")

        # Strategy 2: Find multiple "AGE:" markers (indicates bio section even in complex HTML)
        if not sections:
            age_positions = []
            search_start = 0
            while True:
                idx = filing_text_lower.find("age:", search_start)
                if idx < 0:
                    break
                age_positions.append(idx)
                search_start = idx + 10
                if len(age_positions) >= 20:
                    break

            # If we find 3+ age markers, likely a bio section
            if len(age_positions) >= 3:
                first_age = min(age_positions)
                # Go back to find start of bio section
                start = max(0, first_age - 3000)
                end = min(len(filing_text), first_age + 60000)
                section = filing_text[start:end]
                sections.append(f"--- Section: DIRECTOR BIOGRAPHIES (AGE MARKERS) ---\n{section}")
                logger.info(f"[LLMExtractor] Found {len(age_positions)} AGE markers, extracting bio section from {first_age}")

        # Also look for executive officer sections using "Age:" pattern after exec keywords
        exec_keywords = ["named executive officers", "executive officers", "our executive officers"]
        for keyword in exec_keywords:
            idx = filing_text_lower.find(keyword)
            if idx >= 0:
                # Check if this section has actual content (look for "Age:" within 5000 chars)
                section_preview = filing_text_lower[idx:idx+5000]
                if "age:" in section_preview or "age " in section_preview:
                    start = max(0, idx - 200)
                    end = min(len(filing_text), idx + 30000)
                    section = filing_text[start:end]
                    sections.append(f"--- Section: {keyword.upper()} ---\n{section}")
                    logger.info(f"[LLMExtractor] Found executive section '{keyword}' at {idx} with bio content")
                    break  # Only need one exec section

        # Fallback: try original keywords but skip early TOC positions
        if not sections:
            section_keywords = [
                ("board of directors", 25000),
                ("election of directors", 20000),
                ("compensation of named", 15000),
            ]
            for keyword, section_len in section_keywords:
                # Find all occurrences and use the one most likely to be content (not TOC)
                idx = filing_text_lower.find(keyword)
                if idx >= 0 and idx > 40000:  # Skip TOC which is usually in first 40k
                    start = max(0, idx - 500)
                    end = min(len(filing_text), idx + section_len)
                    section = filing_text[start:end]
                    sections.append(f"--- Section: {keyword.upper()} ---\n{section}")
                    logger.debug(f"[LLMExtractor] Found section '{keyword}' at position {idx}")

        if sections:
            # Use the extracted sections
            filing_text = "\n\n".join(sections[:4])  # Use top 4 sections
            logger.info(f"[LLMExtractor] Extracted {len(sections)} relevant sections for {company_name}")
        else:
            # Fallback: use first portion of text
            filing_text = filing_text[:50000]
            logger.warning(f"[LLMExtractor] No sections found for {company_name}, using first 50k chars")

        prompt = SEC_PROXY_EXTRACTION_PROMPT.format(
            company_name=company_name,
            cik=cik,
            filing_text=filing_text,
        )

        logger.info(f"[LLMExtractor] Extracting from SEC proxy for {company_name} ({len(filing_text)} chars)")

        response = await self._call_llm(prompt)

        # Always log the response for proxy extraction debugging
        logger.info(
            f"[LLMExtractor] SEC proxy raw response for {company_name}: "
            f"{response[:500] if response else 'None'}"
        )

        data = self._parse_json_response(response)

        if not data:
            # Log the failure and try a simpler extraction
            logger.warning(
                f"[LLMExtractor] SEC proxy JSON parse failed for {company_name}. "
                f"Full response: {response}"
            )

            # Try simpler fallback prompt
            data = await self._try_simple_sec_extraction(filing_text, company_name)

        if not data:
            return {"executives": [], "board_members": [], "extraction_confidence": "low"}

        return data

    async def _try_simple_sec_extraction(
        self,
        filing_text: str,
        company_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Try a simpler extraction prompt for SEC filings."""
        # Use only a small portion of the text
        text_sample = filing_text[:20000]

        simple_prompt = f"""Extract names and titles of executives from this SEC filing for {company_name}.

Return JSON only:
{{"executives": [{{"full_name": "Name", "title": "Title"}}], "board_members": [{{"full_name": "Name", "title": "Title"}}]}}

Text:
{text_sample}"""

        logger.info(f"[LLMExtractor] Trying simple SEC extraction for {company_name}")
        response = await self._call_llm(simple_prompt)
        return self._parse_json_response(response)
