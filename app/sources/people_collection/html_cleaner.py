"""
HTML cleaning and preprocessing for LLM extraction.

Removes noise, extracts relevant content, and prepares HTML
for efficient LLM processing.
"""

import re
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag, NavigableString

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
        'script', 'style', 'noscript', 'iframe', 'svg', 'canvas',
        'video', 'audio', 'object', 'embed', 'applet',
        'form', 'input', 'button', 'select', 'textarea',
        'template', 'slot',
    ]

    # Elements that are likely noise
    NOISE_SELECTORS = [
        # Navigation
        'nav', 'header nav', '.nav', '.navigation', '.menu', '.navbar',
        '#navigation', '#nav', '#menu',

        # Footer
        'footer', '.footer', '#footer', '.site-footer',

        # Sidebars
        'aside', '.sidebar', '#sidebar', '.widget',

        # Ads and popups
        '.ad', '.ads', '.advertisement', '.popup', '.modal',
        '[class*="cookie"]', '[class*="banner"]', '[class*="promo"]',

        # Social
        '.social', '.share', '.social-links', '.social-icons',

        # Comments
        '.comments', '#comments', '.comment-section',
    ]

    # Keywords indicating leadership content
    LEADERSHIP_KEYWORDS = [
        'leadership', 'team', 'executive', 'management', 'board',
        'director', 'officer', 'president', 'chief', 'ceo', 'cfo',
        'coo', 'cto', 'vp', 'vice president', 'founder', 'partner',
    ]

    # Keywords indicating person bio
    BIO_KEYWORDS = [
        'joined', 'appointed', 'serves as', 'responsible for',
        'leads', 'oversees', 'experience', 'previously',
        'prior to', 'before joining', 'career', 'background',
        'education', 'degree', 'mba', 'university',
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
            soup = BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.warning(f"HTML parsing failed: {e}")
            return CleanedContent(
                text=html[:self.max_length],
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
            text = soup.get_text(separator='\n', strip=True)

        # Clean up whitespace
        text = self._clean_whitespace(text)

        # Truncate if needed
        if len(text) > self.max_length:
            text = text[:self.max_length] + "\n\n[Content truncated...]"

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
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                import json
                content = script.string
                if content:
                    parsed = json.loads(content)
                    if isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, dict):
                                item_type = item.get('@type', 'unknown')
                                if item_type not in data:
                                    data[item_type] = []
                                data[item_type].append(item)
                    elif isinstance(parsed, dict):
                        item_type = parsed.get('@type', 'unknown')
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
            '.team-member', '.person', '.leader', '.executive',
            '.bio', '.profile', '.staff-member', '.management-team',
            '[class*="team"]', '[class*="leader"]', '[class*="executive"]',
            '[class*="bio"]', '[class*="profile"]',
        ]

        for selector in person_selectors:
            try:
                for element in soup.select(selector):
                    text = element.get_text(strip=True)
                    if len(text) > 50:  # Has meaningful content
                        # Try to extract name and title
                        name = self._extract_name_from_element(element)
                        title = self._extract_title_from_element(element)

                        sections.append({
                            'selector': selector,
                            'text': text[:500],
                            'name': name,
                            'title': title,
                        })
            except Exception:
                pass

        # Also look for heading + content patterns
        for heading in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            heading_text = heading.get_text().lower()
            if any(kw in heading_text for kw in self.LEADERSHIP_KEYWORDS):
                # Get following content
                content = []
                for sibling in heading.find_next_siblings():
                    if sibling.name in ['h1', 'h2', 'h3']:
                        break
                    text = sibling.get_text(strip=True)
                    if text:
                        content.append(text)

                if content:
                    sections.append({
                        'selector': 'heading_section',
                        'heading': heading_text,
                        'text': '\n'.join(content)[:1000],
                    })

        return sections

    def _extract_name_from_element(self, element: Tag) -> Optional[str]:
        """Try to extract a person's name from an element."""
        # Look for common name patterns
        name_selectors = [
            '.name', '.person-name', '.team-name', '.title-name',
            'h3', 'h4', 'h5', '.heading', 'strong',
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
            '.title', '.position', '.role', '.job-title',
            '.designation', 'em', 'span.title',
        ]

        for selector in title_selectors:
            try:
                found = element.select_one(selector)
                if found:
                    title = found.get_text(strip=True)
                    # Title keywords
                    if any(kw in title.lower() for kw in ['chief', 'president', 'vp', 'director', 'manager', 'officer']):
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
        for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'p', 'li', 'div', 'article', 'section']):
            # Skip if inside another tracked element
            if element.parent and element.parent.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'li']:
                continue

            text = element.get_text(separator=' ', strip=True)

            if not text or len(text) < 10:
                continue

            # Add heading markers
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                level = int(element.name[1])
                prefix = '#' * level + ' '
                lines.append(f"\n{prefix}{text}\n")
            else:
                lines.append(text)

        return '\n'.join(lines)

    def _clean_whitespace(self, text: str) -> str:
        """Clean up excessive whitespace."""
        # Replace multiple spaces with single space
        text = re.sub(r'[ \t]+', ' ', text)

        # Replace multiple newlines with double newline
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

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

    Args:
        html: Raw HTML

    Returns:
        List of dicts with name, title, bio, image_url
    """
    soup = BeautifulSoup(html, 'html.parser')
    people = []

    # Common person card patterns
    card_selectors = [
        '.team-member', '.person-card', '.leader-card', '.executive-card',
        '.bio-card', '.profile-card', '.staff-card', '.member-card',
        '[class*="team-member"]', '[class*="person"]',
        'article.person', 'div.person', 'li.person',
    ]

    seen_names = set()

    for selector in card_selectors:
        try:
            for card in soup.select(selector):
                person = _extract_person_from_card(card)
                if person and person.get('name'):
                    name_key = person['name'].lower()
                    if name_key not in seen_names:
                        seen_names.add(name_key)
                        people.append(person)
        except Exception:
            pass

    return people


def _extract_person_from_card(card: Tag) -> Optional[Dict[str, Any]]:
    """Extract person info from a card element."""
    person = {}

    # Name
    name_selectors = ['.name', 'h3', 'h4', '.person-name', '.team-name', 'strong']
    for sel in name_selectors:
        el = card.select_one(sel)
        if el:
            name = el.get_text(strip=True)
            if 2 <= len(name.split()) <= 5 and len(name) < 60:
                person['name'] = name
                break

    # Title
    title_selectors = ['.title', '.position', '.role', '.job-title', 'em', '.designation']
    for sel in title_selectors:
        el = card.select_one(sel)
        if el:
            title = el.get_text(strip=True)
            if len(title) < 100:
                person['title'] = title
                break

    # Bio
    bio_selectors = ['.bio', '.description', '.about', 'p']
    for sel in bio_selectors:
        el = card.select_one(sel)
        if el:
            bio = el.get_text(strip=True)
            if len(bio) > 50:
                person['bio'] = bio[:500]
                break

    # Image
    img = card.select_one('img')
    if img and img.get('src'):
        person['image_url'] = img['src']

    # LinkedIn
    for a in card.find_all('a', href=True):
        href = a['href']
        if 'linkedin.com' in href:
            person['linkedin_url'] = href
            break

    return person if person.get('name') else None
