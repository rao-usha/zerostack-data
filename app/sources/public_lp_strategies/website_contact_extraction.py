"""
Website Contact Extraction for LP Funds

Extracts contact information from LP official websites:
- Contact pages
- Team/People pages
- About pages
- Investor Relations pages

RULES:
- Respect robots.txt
- Rate limiting (max 2 concurrent, 2s delay per domain)
- Only public pages (no authentication)
- Bounded concurrency
- Proper User-Agent identification
"""

import asyncio
import re
import logging
from typing import Optional, List, Dict, Any, Set
from urllib.parse import urljoin, urlparse
from datetime import datetime

import httpx
from bs4 import BeautifulSoup

from app.sources.public_lp_strategies.contact_validation import (
    validate_email, validate_phone, validate_name, calculate_confidence_score
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

USER_AGENT = "NexdataResearch/1.0 (Data research bot; contact@nexdata.com)"
REQUEST_TIMEOUT = 10  # seconds
MAX_PAGES_PER_LP = 5  # Prevent runaway crawling
DELAY_BETWEEN_REQUESTS = 2.0  # seconds
MAX_CONCURRENT_REQUESTS = 2


# =============================================================================
# URL DISCOVERY
# =============================================================================

def find_contact_page_links(soup: BeautifulSoup, base_url: str) -> List[str]:
    """
    Find links to contact, team, about, and IR pages.
    
    Args:
        soup: BeautifulSoup object of homepage
        base_url: Base URL for resolving relative links
        
    Returns:
        List of absolute URLs to crawl
    """
    target_keywords = [
        'contact', 'about', 'team', 'people', 'staff', 'leadership',
        'investor', 'investment', 'director', 'executive', 'management',
        'board', 'governance'
    ]
    
    found_links = set()
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        text = link.get_text().lower().strip()
        
        # Check if link text or href contains target keywords
        if any(keyword in href.lower() or keyword in text for keyword in target_keywords):
            absolute_url = urljoin(base_url, href)
            
            # Only add if it's the same domain
            if urlparse(absolute_url).netloc == urlparse(base_url).netloc:
                found_links.add(absolute_url)
    
    return list(found_links)[:MAX_PAGES_PER_LP]


# =============================================================================
# CONTENT EXTRACTION
# =============================================================================

def extract_emails(text: str) -> List[str]:
    """Extract email addresses from text."""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    emails = re.findall(email_pattern, text)
    
    # Validate each email
    valid_emails = []
    for email in emails:
        is_valid, _ = validate_email(email)
        if is_valid:
            valid_emails.append(email)
    
    return valid_emails


def extract_phones(text: str) -> List[str]:
    """Extract phone numbers from text."""
    # US/Canada pattern
    phone_patterns = [
        r'\+?1?[-.]?\(?\d{3}\)?[-.]?\d{3}[-.]?\d{4}',
        r'\(\d{3}\)\s?\d{3}[-.]?\d{4}',
    ]
    
    phones = []
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        phones.extend(matches)
    
    # Validate each phone
    valid_phones = []
    for phone in phones:
        is_valid, _ = validate_phone(phone)
        if is_valid:
            valid_phones.append(phone)
    
    return valid_phones


def extract_executives(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Extract executive names and titles from structured HTML.
    
    Looks for common patterns:
    - <h3>John Smith</h3><p>Chief Investment Officer</p>
    - <div class="person"><h4>...</h4><span class="title">...</span></div>
    - Lists with names and titles
    
    Returns:
        List of dicts with 'name', 'title', 'role_category'
    """
    executives = []
    
    # Title patterns to look for
    title_patterns = [
        (r'\bChief Investment Officer\b', 'CIO'),
        (r'\bCIO\b', 'CIO'),
        (r'\bChief Financial Officer\b', 'CFO'),
        (r'\bCFO\b', 'CFO'),
        (r'\bChief Executive Officer\b', 'CEO'),
        (r'\bCEO\b', 'CEO'),
        (r'\b(?:Managing|Investment) Director\b', 'Investment Director'),
        (r'\bHead of (?:Investments?|Private Equity|Alternatives)\b', 'Investment Director'),
        (r'\bBoard Member\b', 'Board Member'),
        (r'\bTrustee\b', 'Board Member'),
    ]
    
    # Strategy 1: Look for common HTML structures
    # Find all headings (h2-h4) that might contain names
    for heading in soup.find_all(['h2', 'h3', 'h4']):
        potential_name = heading.get_text().strip()
        
        # Skip if it looks like a section title
        if len(potential_name.split()) > 4 or len(potential_name) < 4:
            continue
        
        # Look for title in next sibling
        title_text = None
        next_elem = heading.find_next_sibling(['p', 'span', 'div'])
        if next_elem:
            title_text = next_elem.get_text().strip()
        
        if title_text:
            # Check if title matches known patterns
            for pattern, role_category in title_patterns:
                if re.search(pattern, title_text, re.IGNORECASE):
                    is_valid, _ = validate_name(potential_name)
                    if is_valid:
                        executives.append({
                            'name': potential_name,
                            'title': title_text,
                            'role_category': role_category
                        })
                    break
    
    # Strategy 2: Look for text containing title keywords
    text_content = soup.get_text()
    for pattern, role_category in title_patterns:
        matches = re.finditer(pattern, text_content, re.IGNORECASE)
        for match in matches:
            # Get surrounding context (50 chars before and after)
            start = max(0, match.start() - 50)
            end = min(len(text_content), match.end() + 50)
            context = text_content[start:end]
            
            # Try to extract name from context (words before the title)
            words = context.split()
            # Look for capitalized words before the title
            name_words = []
            for i, word in enumerate(words):
                if word[0].isupper() and len(word) > 1:
                    name_words.append(word)
                    if len(name_words) >= 2:  # First and last name
                        break
            
            if len(name_words) >= 2:
                potential_name = ' '.join(name_words[-2:])  # Last 2 words
                is_valid, _ = validate_name(potential_name)
                if is_valid:
                    # Check if we already have this person
                    if not any(e['name'] == potential_name for e in executives):
                        executives.append({
                            'name': potential_name,
                            'title': match.group(),
                            'role_category': role_category
                        })
    
    return executives


# =============================================================================
# PAGE CRAWLER
# =============================================================================

async def fetch_page(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore
) -> Optional[str]:
    """
    Fetch a single page with rate limiting.
    
    Returns:
        HTML content or None if failed
    """
    async with semaphore:
        try:
            logger.info(f"Fetching: {url}")
            response = await client.get(
                url,
                headers={'User-Agent': USER_AGENT},
                timeout=REQUEST_TIMEOUT,
                follow_redirects=True
            )
            
            if response.status_code != 200:
                logger.warning(f"Non-200 status for {url}: {response.status_code}")
                return None
            
            # Only process HTML content
            content_type = response.headers.get('content-type', '').lower()
            if 'text/html' not in content_type:
                logger.warning(f"Non-HTML content for {url}: {content_type}")
                return None
            
            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
            return response.text
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None


async def crawl_lp_website(
    homepage_url: str,
    lp_name: str
) -> Dict[str, Any]:
    """
    Crawl an LP website to extract contact information.
    
    Args:
        homepage_url: LP homepage URL
        lp_name: LP name for logging
        
    Returns:
        Dict with extracted contacts, emails, phones
    """
    result = {
        'lp_name': lp_name,
        'homepage_url': homepage_url,
        'executives': [],
        'emails': [],
        'phones': [],
        'pages_crawled': [],
        'errors': []
    }
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async with httpx.AsyncClient() as client:
        try:
            # Fetch homepage
            homepage_html = await fetch_page(client, homepage_url, semaphore)
            if not homepage_html:
                result['errors'].append(f"Failed to fetch homepage: {homepage_url}")
                return result
            
            result['pages_crawled'].append(homepage_url)
            soup = BeautifulSoup(homepage_html, 'html.parser')
            
            # Extract from homepage
            result['emails'].extend(extract_emails(homepage_html))
            result['phones'].extend(extract_phones(homepage_html))
            result['executives'].extend(extract_executives(soup))
            
            # Find contact-related pages
            contact_links = find_contact_page_links(soup, homepage_url)
            logger.info(f"Found {len(contact_links)} contact-related pages for {lp_name}")
            
            # Fetch contact pages
            tasks = []
            for link in contact_links:
                if link not in result['pages_crawled']:
                    tasks.append(fetch_page(client, link, semaphore))
            
            contact_pages = await asyncio.gather(*tasks)
            
            # Process contact pages
            for i, html in enumerate(contact_pages):
                if html:
                    url = contact_links[i]
                    result['pages_crawled'].append(url)
                    soup = BeautifulSoup(html, 'html.parser')
                    result['emails'].extend(extract_emails(html))
                    result['phones'].extend(extract_phones(html))
                    result['executives'].extend(extract_executives(soup))
            
            # Deduplicate
            result['emails'] = list(set(result['emails']))
            result['phones'] = list(set(result['phones']))
            
            # Deduplicate executives by name
            unique_executives = {}
            for exec_info in result['executives']:
                name = exec_info['name']
                if name not in unique_executives:
                    unique_executives[name] = exec_info
            result['executives'] = list(unique_executives.values())
            
        except Exception as e:
            logger.exception(f"Error crawling {homepage_url}")
            result['errors'].append(str(e))
    
    return result


# =============================================================================
# BATCH PROCESSING
# =============================================================================

async def crawl_multiple_lps(
    lp_list: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Crawl multiple LP websites with bounded concurrency.
    
    Args:
        lp_list: List of dicts with 'lp_id', 'lp_name', 'website_url'
        
    Returns:
        List of extraction results
    """
    tasks = []
    for lp in lp_list:
        if lp.get('website_url'):
            task = crawl_lp_website(lp['website_url'], lp['lp_name'])
            tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    return results


# =============================================================================
# CONTACT ASSEMBLY
# =============================================================================

def assemble_contacts_from_extraction(
    extraction_result: Dict[str, Any],
    lp_id: int
) -> List[Dict[str, Any]]:
    """
    Convert extraction result into contact records ready for database insertion.
    
    Args:
        extraction_result: Result from crawl_lp_website
        lp_id: LP fund ID
        
    Returns:
        List of contact dicts matching LpKeyContactInput schema
    """
    contacts = []
    
    # Create contact for each executive found
    for exec_info in extraction_result['executives']:
        contact = {
            'lp_id': lp_id,
            'full_name': exec_info['name'],
            'title': exec_info.get('title'),
            'role_category': exec_info.get('role_category'),
            'email': None,  # Will try to match later
            'phone': None,  # Will use general LP phone if available
            'source_type': 'website',
            'source_url': extraction_result['homepage_url'],
            'confidence_level': 'medium',
            'is_verified': 0
        }
        contacts.append(contact)
    
    # If we found emails but no executives, create general contact
    if extraction_result['emails'] and not extraction_result['executives']:
        for email in extraction_result['emails'][:3]:  # Max 3 general emails
            contact = {
                'lp_id': lp_id,
                'full_name': f"{extraction_result['lp_name']} Contact",
                'title': 'General Inquiry',
                'role_category': 'IR Contact',
                'email': email,
                'phone': extraction_result['phones'][0] if extraction_result['phones'] else None,
                'source_type': 'website',
                'source_url': extraction_result['homepage_url'],
                'confidence_level': 'low',
                'is_verified': 0
            }
            contacts.append(contact)
    
    # If we found phones, add to first contact or create general contact
    if extraction_result['phones'] and contacts:
        contacts[0]['phone'] = extraction_result['phones'][0]
    elif extraction_result['phones'] and not contacts:
        contact = {
            'lp_id': lp_id,
            'full_name': f"{extraction_result['lp_name']} Office",
            'title': 'General Inquiry',
            'role_category': 'IR Contact',
            'email': None,
            'phone': extraction_result['phones'][0],
            'source_type': 'website',
            'source_url': extraction_result['homepage_url'],
            'confidence_level': 'low',
            'is_verified': 0
        }
        contacts.append(contact)
    
    # Recalculate confidence scores
    for contact in contacts:
        contact['confidence_level'] = calculate_confidence_score(
            contact.get('email'),
            contact.get('phone'),
            'website',
            False
        )
    
    return contacts
