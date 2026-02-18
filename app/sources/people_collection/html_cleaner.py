"""
HTML cleaning and preprocessing for LLM extraction.

Removes noise, extracts relevant content, and prepares HTML
for efficient LLM processing.
"""

import re
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


@dataclass
class CleanedContent:
    """Result of cleaning HTML content."""

    text: str
    structured_data: Dict[str, Any]
    people_sections: List[Dict[str, str]]
    has_leadership_content: bool
    content_length: int
    original_length: int


class HTMLCleaner:
    """
    Cleans and preprocesses HTML for LLM extraction.

    Strategies:
    1. Remove noise (scripts, styles, ads, navigation)
    2. Extract structured data (JSON-LD, microdata)
    3. Identify people/leadership sections
    4. Preserve semantic structure
    """

    # Elements to completely remove
    REMOVE_ELEMENTS = [
        "script",
        "style",
        "noscript",
        "iframe",
        "svg",
        "canvas",
        "video",
        "audio",
        "object",
        "embed",
        "applet",
        "form",
        "input",
        "button",
        "select",
        "textarea",
        "template",
        "slot",
    ]

    # Elements that are likely noise
    NOISE_SELECTORS = [
        # Navigation
        "nav",
        "header nav",
        ".nav",
        ".navigation",
        ".menu",
        ".navbar",
        "#navigation",
        "#nav",
        "#menu",
        # Footer
        "footer",
        ".footer",
        "#footer",
        ".site-footer",
        # Sidebars
        "aside",
        ".sidebar",
        "#sidebar",
        ".widget",
        # Ads and popups
        ".ad",
        ".ads",
        ".advertisement",
        ".popup",
        ".modal",
        '[class*="cookie"]',
        '[class*="banner"]',
        '[class*="promo"]',
        # Social
        ".social",
        ".share",
        ".social-links",
        ".social-icons",
        # Comments
        ".comments",
        "#comments",
        ".comment-section",
    ]

    # Keywords indicating leadership content
    LEADERSHIP_KEYWORDS = [
        "leadership",
        "team",
        "executive",
        "management",
        "board",
        "director",
        "officer",
        "president",
        "chief",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "vp",
        "vice president",
        "founder",
        "partner",
    ]

    # Keywords indicating person bio
    BIO_KEYWORDS = [
        "joined",
        "appointed",
        "serves as",
        "responsible for",
        "leads",
        "oversees",
        "experience",
        "previously",
        "prior to",
        "before joining",
        "career",
        "background",
        "education",
        "degree",
        "mba",
        "university",
    ]

    def __init__(self, max_length: int = 100000):
        """
        Initialize cleaner.

        Args:
            max_length: Maximum output text length
        """
        self.max_length = max_length

    def clean(self, html: str, preserve_structure: bool = True) -> CleanedContent:
        """
        Clean HTML and extract relevant content.

        Args:
            html: Raw HTML string
            preserve_structure: Whether to preserve some HTML structure

        Returns:
            CleanedContent with processed text and metadata
        """
        original_length = len(html)

        try:
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.warning(f"HTML parsing failed: {e}")
            return CleanedContent(
                text=html[: self.max_length],
                structured_data={},
                people_sections=[],
                has_leadership_content=False,
                content_length=min(len(html), self.max_length),
                original_length=original_length,
            )

        # Extract structured data before cleaning
        structured_data = self._extract_structured_data(soup)

        # Remove unwanted elements
        self._remove_elements(soup)

        # Remove noise sections
        self._remove_noise(soup)

        # Find people/leadership sections
        people_sections = self._find_people_sections(soup)

        # Check for leadership content
        has_leadership = self._has_leadership_content(soup)

        # Extract text with optional structure
        if preserve_structure:
            text = self._extract_structured_text(soup)
        else:
            text = soup.get_text(separator="\n", strip=True)

        # Clean up whitespace
        text = self._clean_whitespace(text)

        # Truncate if needed
        if len(text) > self.max_length:
            text = text[: self.max_length] + "\n\n[Content truncated...]"

        return CleanedContent(
            text=text,
            structured_data=structured_data,
            people_sections=people_sections,
            has_leadership_content=has_leadership,
            content_length=len(text),
            original_length=original_length,
        )

    def _remove_elements(self, soup: BeautifulSoup) -> None:
        """Remove elements that should be completely removed."""
        for element_type in self.REMOVE_ELEMENTS:
            for element in soup.find_all(element_type):
                element.decompose()

    def _remove_noise(self, soup: BeautifulSoup) -> None:
        """Remove noise sections that don't contain leadership info."""
        for selector in self.NOISE_SELECTORS:
            try:
                for element in soup.select(selector):
                    # Check if it might contain leadership content
                    text = element.get_text().lower()
                    if not any(kw in text for kw in self.LEADERSHIP_KEYWORDS[:5]):
                        element.decompose()
            except Exception:
                # Invalid selector, skip
                pass

    def _extract_structured_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract JSON-LD and other structured data."""
        data = {}

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                import json

                content = script.string
                if content:
                    parsed = json.loads(content)
                    if isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, dict):
                                item_type = item.get("@type", "unknown")
                                if item_type not in data:
                                    data[item_type] = []
                                data[item_type].append(item)
                    elif isinstance(parsed, dict):
                        item_type = parsed.get("@type", "unknown")
                        if item_type not in data:
                            data[item_type] = []
                        data[item_type].append(parsed)
            except Exception:
                pass

        return data

    def _find_people_sections(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Find sections that appear to contain people/bios."""
        sections = []

        # Look for common person card patterns
        person_selectors = [
            ".team-member",
            ".person",
            ".leader",
            ".executive",
            ".bio",
            ".profile",
            ".staff-member",
            ".management-team",
            '[class*="team"]',
            '[class*="leader"]',
            '[class*="executive"]',
            '[class*="bio"]',
            '[class*="profile"]',
        ]

        for selector in person_selectors:
            try:
                for element in soup.select(selector):
                    text = element.get_text(strip=True)
                    if len(text) > 50:  # Has meaningful content
                        # Try to extract name and title
                        name = self._extract_name_from_element(element)
                        title = self._extract_title_from_element(element)

                        sections.append(
                            {
                                "selector": selector,
                                "text": text[:500],
                                "name": name,
                                "title": title,
                            }
                        )
            except Exception:
                pass

        # Also look for heading + content patterns
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            heading_text = heading.get_text().lower()
            if any(kw in heading_text for kw in self.LEADERSHIP_KEYWORDS):
                # Get following content
                content = []
                for sibling in heading.find_next_siblings():
                    if sibling.name in ["h1", "h2", "h3"]:
                        break
                    text = sibling.get_text(strip=True)
                    if text:
                        content.append(text)

                if content:
                    sections.append(
                        {
                            "selector": "heading_section",
                            "heading": heading_text,
                            "text": "\n".join(content)[:1000],
                        }
                    )

        return sections

    def _extract_name_from_element(self, element: Tag) -> Optional[str]:
        """Try to extract a person's name from an element."""
        # Look for common name patterns
        name_selectors = [
            ".name",
            ".person-name",
            ".team-name",
            ".title-name",
            "h3",
            "h4",
            "h5",
            ".heading",
            "strong",
        ]

        for selector in name_selectors:
            try:
                found = element.select_one(selector)
                if found:
                    name = found.get_text(strip=True)
                    # Basic validation - names usually have 2-4 words
                    words = name.split()
                    if 2 <= len(words) <= 5 and len(name) < 50:
                        return name
            except Exception:
                pass

        return None

    def _extract_title_from_element(self, element: Tag) -> Optional[str]:
        """Try to extract a person's title from an element."""
        title_selectors = [
            ".title",
            ".position",
            ".role",
            ".job-title",
            ".designation",
            "em",
            "span.title",
        ]

        for selector in title_selectors:
            try:
                found = element.select_one(selector)
                if found:
                    title = found.get_text(strip=True)
                    # Title keywords
                    if any(
                        kw in title.lower()
                        for kw in [
                            "chief",
                            "president",
                            "vp",
                            "director",
                            "manager",
                            "officer",
                        ]
                    ):
                        return title
            except Exception:
                pass

        return None

    def _has_leadership_content(self, soup: BeautifulSoup) -> bool:
        """Check if the page appears to have leadership content."""
        text = soup.get_text().lower()

        # Count leadership keywords
        keyword_count = sum(1 for kw in self.LEADERSHIP_KEYWORDS if kw in text)
        bio_count = sum(1 for kw in self.BIO_KEYWORDS if kw in text)

        # Need multiple indicators
        return keyword_count >= 3 or (keyword_count >= 2 and bio_count >= 2)

    def _extract_structured_text(self, soup: BeautifulSoup) -> str:
        """Extract text while preserving some structure."""
        lines = []

        # Process main content
        for element in soup.find_all(
            ["h1", "h2", "h3", "h4", "h5", "p", "li", "div", "article", "section"]
        ):
            # Skip if inside another tracked element
            if element.parent and element.parent.name in [
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "li",
            ]:
                continue

            text = element.get_text(separator=" ", strip=True)

            if not text or len(text) < 10:
                continue

            # Add heading markers
            if element.name in ["h1", "h2", "h3", "h4", "h5"]:
                level = int(element.name[1])
                prefix = "#" * level + " "
                lines.append(f"\n{prefix}{text}\n")
            else:
                lines.append(text)

        return "\n".join(lines)

    def _clean_whitespace(self, text: str) -> str:
        """Clean up excessive whitespace."""
        # Replace multiple spaces with single space
        text = re.sub(r"[ \t]+", " ", text)

        # Replace multiple newlines with double newline
        text = re.sub(r"\n{3,}", "\n\n", text)

        # Strip lines
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)

        return text.strip()


def clean_html_for_extraction(
    html: str,
    max_length: int = 100000,
) -> str:
    """
    Convenience function to clean HTML for LLM extraction.

    Args:
        html: Raw HTML
        max_length: Maximum output length

    Returns:
        Cleaned text ready for LLM
    """
    cleaner = HTMLCleaner(max_length=max_length)
    result = cleaner.clean(html)
    return result.text


def extract_people_cards(html: str) -> List[Dict[str, Any]]:
    """
    Extract potential person cards from HTML.

    Uses multiple strategies:
    1. Schema.org Person markup (most reliable)
    2. Common CSS class patterns for team cards
    3. Grid/flex container patterns
    4. Sequential heading + paragraph patterns

    Args:
        html: Raw HTML

    Returns:
        List of dicts with name, title, bio, image_url, linkedin_url
    """
    soup = BeautifulSoup(html, "html.parser")
    people = []
    seen_names = set()

    def add_person(person: Dict[str, Any]) -> bool:
        """Add person if valid and not seen."""
        if not person or not person.get("name"):
            return False
        name_key = person["name"].lower().strip()
        if name_key in seen_names or len(name_key) < 3:
            return False
        seen_names.add(name_key)
        people.append(person)
        logger.debug(
            f"[HTMLCleaner] Found person: {person.get('name')} - {person.get('title', 'No title')}"
        )
        return True

    # Strategy 1: Schema.org Person markup (JSON-LD)
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            import json

            data = json.loads(script.string)
            if isinstance(data, list):
                items = data
            else:
                items = [data]

            for item in items:
                if item.get("@type") == "Person":
                    person = {
                        "name": item.get("name"),
                        "title": item.get("jobTitle"),
                        "bio": item.get("description"),
                        "image_url": item.get("image"),
                        "linkedin_url": _extract_social_url(
                            item.get("sameAs", []), "linkedin"
                        ),
                    }
                    add_person(person)
        except Exception:
            pass

    # Strategy 2: Schema.org microdata (itemprop)
    for person_el in soup.find_all(
        attrs={"itemtype": re.compile(r"schema.org/Person", re.I)}
    ):
        person = {
            "name": _get_itemprop(person_el, "name"),
            "title": _get_itemprop(person_el, "jobTitle"),
            "bio": _get_itemprop(person_el, "description"),
            "image_url": _get_itemprop(person_el, "image", attr="src"),
        }
        add_person(person)

    # Strategy 3: Common card CSS selectors
    card_selectors = [
        # Team-specific classes
        ".team-member",
        ".team-card",
        ".team-item",
        ".team-block",
        ".person-card",
        ".person-item",
        ".person-block",
        ".leader-card",
        ".leader-item",
        ".leader-block",
        ".executive-card",
        ".executive-item",
        ".executive-block",
        ".bio-card",
        ".profile-card",
        ".staff-card",
        ".member-card",
        # Attribute selectors
        '[class*="team-member"]',
        '[class*="team-card"]',
        '[class*="person-card"]',
        '[class*="person-item"]',
        '[class*="leader"]',
        '[class*="executive"]',
        '[class*="bio-card"]',
        '[class*="profile-card"]',
        # Structural patterns
        "article.person",
        "div.person",
        "li.person",
        "article.team",
        "div.team",
        "li.team",
        ".leadership-grid > div",
        ".team-grid > div",
        ".leadership-list > li",
        ".team-list > li",
        # WordPress patterns
        ".wp-block-group.person",
        ".elementor-widget-container .person",
        # Generic card in team section
        ".about-team .card",
        ".our-team .card",
        ".leadership .card",
        ".executives .card",
    ]

    for selector in card_selectors:
        try:
            for card in soup.select(selector):
                person = _extract_person_from_card(card)
                add_person(person)
        except Exception:
            pass

    # Strategy 4: Grid/flex containers that look like team sections
    container_selectors = [
        ".team",
        ".leadership",
        ".executives",
        ".management",
        ".our-team",
        ".the-team",
        ".meet-the-team",
        '[class*="team-section"]',
        '[class*="leadership-section"]',
    ]

    for selector in container_selectors:
        try:
            for container in soup.select(selector):
                # Find child elements that could be person cards
                for child in container.find_all(
                    ["div", "article", "li"], recursive=False
                ):
                    # Skip if too small
                    if len(child.get_text(strip=True)) < 20:
                        continue
                    person = _extract_person_from_card(child)
                    add_person(person)
        except Exception:
            pass

    # Strategy 5: Image + name + title pattern (common layout)
    for img in soup.find_all("img"):
        parent = img.parent
        if not parent:
            continue

        # Look for text near image that looks like name + title
        container = parent.parent if parent.name == "a" else parent

        # Go up a few levels to find the card container
        for _ in range(3):
            if not container:
                break
            text = container.get_text(strip=True)
            if len(text) > 30 and len(text) < 1000:
                person = _extract_person_from_card(container)
                if add_person(person):
                    break
            container = container.parent

    # Strategy 6: Heading patterns (h3/h4 followed by title/bio)
    for heading in soup.find_all(["h3", "h4", "h5"]):
        name = heading.get_text(strip=True)

        # Skip if doesn't look like a name
        if not _looks_like_name(name):
            continue

        person = {"name": name}

        # Look for title in next sibling
        next_el = heading.find_next_sibling()
        if next_el:
            next_text = next_el.get_text(strip=True)
            if _looks_like_title(next_text):
                person["title"] = next_text

                # Look for bio after title
                bio_el = next_el.find_next_sibling()
                if bio_el and bio_el.name == "p":
                    bio = bio_el.get_text(strip=True)
                    if len(bio) > 50:
                        person["bio"] = bio[:500]

        # Check parent for LinkedIn link
        parent = heading.parent
        if parent:
            for a in parent.find_all("a", href=True):
                if "linkedin.com" in a["href"]:
                    person["linkedin_url"] = a["href"]
                    break

        add_person(person)

    logger.info(f"[HTMLCleaner] Extracted {len(people)} people via structured HTML")
    return people


def _extract_social_url(same_as: Any, platform: str) -> Optional[str]:
    """Extract a social URL from schema.org sameAs."""
    if isinstance(same_as, str):
        same_as = [same_as]
    if not isinstance(same_as, list):
        return None

    for url in same_as:
        if platform in str(url).lower():
            return url
    return None


def _get_itemprop(element: Tag, prop: str, attr: str = None) -> Optional[str]:
    """Get value of a schema.org itemprop."""
    el = element.find(attrs={"itemprop": prop})
    if not el:
        return None
    if attr:
        return el.get(attr)
    return el.get_text(strip=True)


def _strip_name_from_title(title: str, name: Optional[str]) -> str:
    """
    Strip person's name from the beginning of a title string.

    Handles cases where HTML extraction concatenates name and title,
    e.g., "Vicente ReynalChairman, President and CEO" -> "Chairman, President and CEO"
    """
    if not name or not title:
        return title

    # Check if title starts with the person's name (with or without separator)
    if title.startswith(name):
        remainder = title[len(name) :].lstrip(" ,;:-\t\n")
        if remainder and _looks_like_title(remainder):
            return remainder

    # Also check first name + last name without space (concatenation artifact)
    name_no_space = name.replace(" ", "")
    if title.replace(" ", "").startswith(name_no_space):
        # Find where the name part ends in the original title
        for i in range(len(name), len(title)):
            remainder = title[i:].lstrip(" ,;:-\t\n")
            if remainder and _looks_like_title(remainder):
                return remainder

    return title


def _looks_like_name(text: str) -> bool:
    """Check if text looks like a person's name."""
    if not text or len(text) < 3 or len(text) > 60:
        return False

    words = text.split()
    if len(words) < 2 or len(words) > 5:
        return False

    # Should be mostly letters
    alpha_count = sum(1 for c in text if c.isalpha())
    if alpha_count < len(text) * 0.7:
        return False

    # Should start with capital letter
    if not text[0].isupper():
        return False

    # Should not contain these
    bad_words = [
        "team",
        "leadership",
        "about",
        "contact",
        "learn",
        "read",
        "more",
        "view",
    ]
    if any(w in text.lower() for w in bad_words):
        return False

    return True


def _looks_like_title(text: str) -> bool:
    """Check if text looks like a job title."""
    if not text or len(text) < 3 or len(text) > 150:
        return False

    text_lower = text.lower()

    # Title keywords
    title_keywords = [
        "chief",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "cio",
        "cmo",
        "president",
        "vice president",
        "vp",
        "director",
        "manager",
        "head of",
        "founder",
        "co-founder",
        "partner",
        "officer",
        "executive",
        "lead",
        "senior",
    ]

    return any(kw in text_lower for kw in title_keywords)


def _extract_person_from_card(card: Tag) -> Optional[Dict[str, Any]]:
    """Extract person info from a card element."""
    person = {}

    # Name - try multiple selectors
    name_selectors = [
        ".name",
        ".person-name",
        ".team-name",
        ".member-name",
        ".leader-name",
        ".executive-name",
        ".profile-name",
        "h3",
        "h4",
        "h5",
        ".heading",
        "strong",
        '[class*="name"]',
    ]

    for sel in name_selectors:
        try:
            el = card.select_one(sel)
            if el:
                name = el.get_text(strip=True)
                if _looks_like_name(name):
                    person["name"] = name
                    break
        except Exception:
            pass

    # Title - try multiple selectors
    title_selectors = [
        ".title",
        ".position",
        ".role",
        ".job-title",
        ".designation",
        ".person-title",
        ".team-title",
        ".member-title",
        '[class*="title"]',
        '[class*="position"]',
        '[class*="role"]',
        "em",
        "span.title",
        "p.title",
    ]

    for sel in title_selectors:
        try:
            el = card.select_one(sel)
            if el and el != card.select_one(".name"):  # Don't pick name as title
                title = el.get_text(strip=True)
                if len(title) < 150 and title != person.get("name"):
                    # Strip person's name from title if concatenated
                    title = _strip_name_from_title(title, person.get("name"))
                    person["title"] = title
                    break
        except Exception:
            pass

    # If no title found, try to find text that looks like a title
    if not person.get("title"):
        for el in card.find_all(["span", "p", "div", "em"]):
            text = el.get_text(strip=True)
            if text != person.get("name") and _looks_like_title(text):
                # Strip person's name from title if concatenated
                text = _strip_name_from_title(text, person.get("name"))
                person["title"] = text
                break

    # Bio
    bio_selectors = [".bio", ".description", ".about", ".summary", ".excerpt", "p"]
    for sel in bio_selectors:
        try:
            el = card.select_one(sel)
            if el:
                bio = el.get_text(strip=True)
                # Don't use name or title as bio
                if (
                    len(bio) > 50
                    and bio != person.get("name")
                    and bio != person.get("title")
                ):
                    person["bio"] = bio[:500]
                    break
        except Exception:
            pass

    # Image
    img = card.select_one("img")
    if img:
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if src:
            person["image_url"] = src

    # LinkedIn - check all links
    for a in card.find_all("a", href=True):
        href = a["href"]
        if "linkedin.com" in href.lower():
            person["linkedin_url"] = href
            break

    # Email
    for a in card.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            person["email"] = href.replace("mailto:", "").split("?")[0]
            break

    return person if person.get("name") else None
