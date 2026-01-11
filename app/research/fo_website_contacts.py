"""
Family Office Website Contact Extraction
Extracts contact information from official family office websites with strict privacy controls.
"""
import asyncio
import httpx
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse
from datetime import date, datetime
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from app.research.contact_validation import (
    validate_contact_data,
    validate_email,
    validate_phone,
    standardize_phone,
    extract_executive_title,
    is_likely_executive_title,
    determine_role_from_title,
    should_skip_page,
    calculate_confidence_level
)

logger = logging.getLogger(__name__)


# Maximum pages to crawl per family office (prevent over-crawling)
MAX_PAGES_PER_FO = 3

# Pages that are likely to contain contact information
CONTACT_PAGE_KEYWORDS = [
    'contact', 'about', 'team', 'people', 'leadership', 'our-team',
    'about-us', 'contact-us', 'staff', 'management', 'executives'
]

# User agent identifying as respectful research bot
USER_AGENT = "NexdataResearch/1.0 (research@nexdata.com; respectful research bot)"


class FamilyOfficeWebsiteExtractor:
    """Extract contacts from family office websites with strict rate limiting and privacy controls."""
    
    def __init__(self, max_concurrency: int = 1, delay_seconds: float = 5.0):
        """
        Initialize extractor.
        
        Args:
            max_concurrency: Maximum concurrent requests (default 1 for family offices)
            delay_seconds: Delay between requests (default 5 seconds)
        """
        self.max_concurrency = max_concurrency
        self.delay_seconds = delay_seconds
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[httpx.AsyncClient] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0),
            headers={'User-Agent': USER_AGENT},
            follow_redirects=True
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.aclose()
    
    async def extract_contacts_from_fo(
        self,
        family_office_id: int,
        family_office_name: str,
        website_url: str
    ) -> List[Dict]:
        """
        Extract contacts from a single family office website.
        
        Args:
            family_office_id: Database ID of family office
            family_office_name: Name of family office
            website_url: Website URL
        
        Returns:
            List of contact dictionaries
        """
        logger.info(f"Extracting contacts from {family_office_name} ({website_url})")
        
        contacts = []
        pages_visited = 0
        
        try:
            # Fetch homepage
            async with self.semaphore:
                homepage_html = await self._fetch_page(website_url)
                await asyncio.sleep(self.delay_seconds)
            
            if not homepage_html:
                logger.warning(f"Could not fetch homepage for {family_office_name}")
                return contacts
            
            # Check if page should be skipped
            should_skip, reason = should_skip_page(homepage_html, website_url)
            if should_skip:
                logger.info(f"Skipping {family_office_name}: {reason}")
                return contacts
            
            # Extract contacts from homepage
            homepage_contacts = self._extract_contacts_from_html(
                homepage_html, website_url, family_office_id, family_office_name
            )
            contacts.extend(homepage_contacts)
            pages_visited += 1
            
            # Find contact-related pages
            contact_pages = self._find_contact_pages(homepage_html, website_url)
            
            # Visit up to MAX_PAGES_PER_FO pages
            for page_url in contact_pages[:MAX_PAGES_PER_FO - 1]:
                if pages_visited >= MAX_PAGES_PER_FO:
                    break
                
                async with self.semaphore:
                    page_html = await self._fetch_page(page_url)
                    await asyncio.sleep(self.delay_seconds)
                
                if page_html:
                    # Check if page should be skipped
                    should_skip, reason = should_skip_page(page_html, page_url)
                    if should_skip:
                        logger.info(f"Skipping page {page_url}: {reason}")
                        continue
                    
                    page_contacts = self._extract_contacts_from_html(
                        page_html, page_url, family_office_id, family_office_name
                    )
                    contacts.extend(page_contacts)
                    pages_visited += 1
            
            logger.info(f"Found {len(contacts)} contacts for {family_office_name}")
            
        except Exception as e:
            logger.error(f"Error extracting contacts from {family_office_name}: {e}")
        
        return contacts
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch a single page with error handling.
        
        Args:
            url: URL to fetch
        
        Returns:
            HTML content or None
        """
        try:
            response = await self.session.get(url)
            response.raise_for_status()
            return response.text
        except httpx.HTTPError as e:
            logger.warning(f"HTTP error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def _find_contact_pages(self, html: str, base_url: str) -> List[str]:
        """
        Find links to contact/team/about pages.
        
        Args:
            html: HTML content
            base_url: Base URL for resolving relative links
        
        Returns:
            List of contact page URLs
        """
        soup = BeautifulSoup(html, 'html.parser')
        contact_pages = []
        
        # Find all links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text().lower().strip()
            
            # Check if link text or href contains contact keywords
            is_contact_page = any(
                keyword in href.lower() or keyword in text
                for keyword in CONTACT_PAGE_KEYWORDS
            )
            
            if is_contact_page:
                # Resolve relative URLs
                full_url = urljoin(base_url, href)
                
                # Only include URLs from same domain
                if urlparse(full_url).netloc == urlparse(base_url).netloc:
                    if full_url not in contact_pages:
                        contact_pages.append(full_url)
        
        return contact_pages
    
    def _extract_contacts_from_html(
        self,
        html: str,
        source_url: str,
        family_office_id: int,
        family_office_name: str
    ) -> List[Dict]:
        """
        Extract contact information from HTML.
        
        Args:
            html: HTML content
            source_url: Source URL
            family_office_id: Database ID
            family_office_name: FO name
        
        Returns:
            List of contact dictionaries
        """
        soup = BeautifulSoup(html, 'html.parser')
        contacts = []
        
        # Strategy 1: Look for structured team/people sections
        team_sections = soup.find_all(['div', 'section'], class_=re.compile(
            r'(team|people|staff|leadership|executive|management)', re.I
        ))
        
        for section in team_sections:
            section_contacts = self._extract_from_team_section(section, source_url, family_office_id)
            contacts.extend(section_contacts)
        
        # Strategy 2: Look for general contact information
        if len(contacts) == 0:
            general_contacts = self._extract_general_contacts(soup, source_url, family_office_id)
            contacts.extend(general_contacts)
        
        # Validate and filter contacts
        validated_contacts = []
        for contact in contacts:
            is_valid, errors = validate_contact_data(
                full_name=contact.get('full_name', ''),
                email=contact.get('email'),
                phone=contact.get('phone'),
                title=contact.get('title')
            )
            
            if is_valid:
                validated_contacts.append(contact)
            else:
                logger.debug(f"Invalid contact for {family_office_name}: {errors}")
        
        return validated_contacts
    
    def _extract_from_team_section(
        self,
        section: BeautifulSoup,
        source_url: str,
        family_office_id: int
    ) -> List[Dict]:
        """Extract contacts from a team/people section."""
        contacts = []
        
        # Look for individual person cards/entries
        person_elements = section.find_all(['div', 'article', 'li'], class_=re.compile(
            r'(person|member|profile|card|bio)', re.I
        ))
        
        for element in person_elements:
            contact = self._extract_person_info(element, source_url, family_office_id)
            if contact:
                contacts.append(contact)
        
        return contacts
    
    def _extract_person_info(
        self,
        element: BeautifulSoup,
        source_url: str,
        family_office_id: int
    ) -> Optional[Dict]:
        """Extract information about a single person."""
        try:
            # Extract name
            name = None
            for name_tag in element.find_all(['h2', 'h3', 'h4', 'h5', 'strong', 'b']):
                text = name_tag.get_text().strip()
                # Check if looks like a name (2+ words, reasonable length)
                if 2 <= len(text.split()) <= 5 and len(text) < 50:
                    name = text
                    break
            
            if not name:
                return None
            
            # Extract title
            title = None
            for title_tag in element.find_all(['p', 'span', 'div'], class_=re.compile(
                r'(title|position|role)', re.I
            )):
                text = title_tag.get_text().strip()
                if text and len(text) < 100:
                    title = text
                    break
            
            # Only proceed if person has an executive title
            if title and is_likely_executive_title(title):
                # Extract email
                email = None
                email_links = element.find_all('a', href=re.compile(r'mailto:', re.I))
                if email_links:
                    email = email_links[0].get('href', '').replace('mailto:', '').strip()
                
                # Extract phone
                phone = None
                phone_links = element.find_all('a', href=re.compile(r'tel:', re.I))
                if phone_links:
                    phone = phone_links[0].get('href', '').replace('tel:', '').strip()
                    phone = standardize_phone(phone)
                
                # Must have at least one contact method
                if email or phone:
                    role = determine_role_from_title(title)
                    is_executive = is_likely_executive_title(title)
                    confidence = calculate_confidence_level(
                        source_type='website',
                        has_email=bool(email),
                        has_phone=bool(phone),
                        has_title=bool(title),
                        is_executive=is_executive
                    )
                    
                    return {
                        'family_office_id': family_office_id,
                        'full_name': name,
                        'title': title,
                        'role': role,
                        'email': email,
                        'phone': phone,
                        'data_source': f'website:{source_url}',
                        'confidence_level': confidence,
                        'collected_date': date.today(),
                        'is_primary_contact': is_executive
                    }
        
        except Exception as e:
            logger.error(f"Error extracting person info: {e}")
        
        return None
    
    def _extract_general_contacts(
        self,
        soup: BeautifulSoup,
        source_url: str,
        family_office_id: int
    ) -> List[Dict]:
        """Extract general/firm-level contact information."""
        contacts = []
        
        # Look for general email addresses
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = []
        
        # Find mailto links
        for link in soup.find_all('a', href=re.compile(r'mailto:', re.I)):
            email = link.get('href', '').replace('mailto:', '').strip()
            if email:
                is_valid, _ = validate_email(email)
                if is_valid:
                    emails.append(email)
        
        # Find emails in text
        text_content = soup.get_text()
        found_emails = re.findall(email_pattern, text_content)
        for email in found_emails:
            is_valid, _ = validate_email(email)
            if is_valid and email not in emails:
                emails.append(email)
        
        # Find phone numbers
        phones = []
        for link in soup.find_all('a', href=re.compile(r'tel:', re.I)):
            phone = link.get('href', '').replace('tel:', '').strip()
            if phone:
                is_valid, _ = validate_phone(phone)
                if is_valid:
                    phones.append(standardize_phone(phone))
        
        # Create a general contact if we found anything
        if emails or phones:
            contacts.append({
                'family_office_id': family_office_id,
                'full_name': 'General Inquiry',
                'title': 'Contact',
                'role': 'General',
                'email': emails[0] if emails else None,
                'phone': phones[0] if phones else None,
                'data_source': f'website:{source_url}',
                'confidence_level': 'low',
                'collected_date': date.today(),
                'is_primary_contact': False
            })
        
        return contacts


async def extract_contacts_for_family_offices(
    db: Session,
    office_ids: Optional[List[int]] = None,
    max_concurrency: int = 1,
    delay_seconds: float = 5.0
) -> Dict:
    """
    Extract contacts from family office websites.
    
    Args:
        db: Database session
        office_ids: Optional list of office IDs to process (None = all with websites)
        max_concurrency: Max concurrent requests
        delay_seconds: Delay between requests
    
    Returns:
        Summary statistics
    """
    # Query family offices with websites
    if office_ids:
        query = text("""
            SELECT id, name, website
            FROM family_offices
            WHERE website IS NOT NULL
            AND id = ANY(:office_ids)
        """)
        result = db.execute(query, {'office_ids': office_ids})
    else:
        query = text("""
            SELECT id, name, website
            FROM family_offices
            WHERE website IS NOT NULL
        """)
        result = db.execute(query)
    
    family_offices = result.fetchall()
    
    logger.info(f"Processing {len(family_offices)} family offices")
    
    all_contacts = []
    
    async with FamilyOfficeWebsiteExtractor(max_concurrency, delay_seconds) as extractor:
        for fo_id, fo_name, website in family_offices:
            contacts = await extractor.extract_contacts_from_fo(
                family_office_id=fo_id,
                family_office_name=fo_name,
                website_url=website
            )
            all_contacts.extend(contacts)
    
    # Insert contacts into database
    inserted_count = 0
    duplicate_count = 0
    error_count = 0
    
    for contact in all_contacts:
        try:
            # Check for duplicates
            check_query = text("""
                SELECT COUNT(*) FROM family_office_contacts
                WHERE family_office_id = :fo_id
                AND (
                    (email IS NOT NULL AND email = :email)
                    OR (full_name = :name)
                )
            """)
            dup_result = db.execute(check_query, {
                'fo_id': contact['family_office_id'],
                'email': contact.get('email'),
                'name': contact['full_name']
            }).scalar()
            
            if dup_result > 0:
                duplicate_count += 1
                logger.debug(f"Duplicate contact: {contact['full_name']}")
                continue
            
            # Insert contact
            insert_query = text("""
                INSERT INTO family_office_contacts (
                    family_office_id, full_name, title, role, email, phone,
                    data_source, confidence_level, collected_date, is_primary_contact,
                    status, created_at
                ) VALUES (
                    :family_office_id, :full_name, :title, :role, :email, :phone,
                    :data_source, :confidence_level, :collected_date, :is_primary_contact,
                    'Active', NOW()
                )
            """)
            db.execute(insert_query, contact)
            inserted_count += 1
        
        except Exception as e:
            error_count += 1
            logger.error(f"Error inserting contact {contact.get('full_name')}: {e}")
    
    db.commit()
    
    return {
        'family_offices_processed': len(family_offices),
        'contacts_found': len(all_contacts),
        'contacts_inserted': inserted_count,
        'duplicates_skipped': duplicate_count,
        'errors': error_count
    }
