"""
Configuration for people collection module.

Contains:
- Rate limiting settings per domain/source
- LLM configuration and prompts
- Title normalization mappings
- URL patterns for leadership pages
"""

from typing import Dict, List
from pydantic import BaseModel
import os


# =============================================================================
# RATE LIMITING
# =============================================================================


class RateLimitConfig(BaseModel):
    """Rate limit configuration for a source type."""

    requests_per_second: float
    retry_attempts: int = 3
    retry_delay_seconds: float = 2.0
    timeout_seconds: float = 30.0


RATE_LIMITS: Dict[str, RateLimitConfig] = {
    "website": RateLimitConfig(
        requests_per_second=0.5,  # 1 request per 2 seconds per domain
        retry_attempts=3,
        retry_delay_seconds=2.0,
        timeout_seconds=30.0,
    ),
    "sec_edgar": RateLimitConfig(
        requests_per_second=10.0,  # SEC allows 10/second
        retry_attempts=3,
        retry_delay_seconds=1.0,
        timeout_seconds=60.0,
    ),
    "news": RateLimitConfig(
        requests_per_second=0.2,  # 1 request per 5 seconds
        retry_attempts=2,
        retry_delay_seconds=5.0,
        timeout_seconds=30.0,
    ),
    "linkedin_google": RateLimitConfig(
        requests_per_second=0.033,  # 1 request per 30 seconds
        retry_attempts=2,
        retry_delay_seconds=30.0,
        timeout_seconds=30.0,
    ),
}


# =============================================================================
# LLM CONFIGURATION
# =============================================================================


class LLMConfig(BaseModel):
    """Configuration for LLM extraction."""

    provider: str = "anthropic"  # anthropic or openai
    model: str = "claude-3-5-sonnet-20241022"
    temperature: float = 0.1  # Low for consistent extraction
    max_tokens: int = 4000
    retry_attempts: int = 3
    retry_delay_seconds: float = 2.0


def get_llm_config() -> LLMConfig:
    """Get LLM configuration from environment."""
    provider = "anthropic" if os.getenv("ANTHROPIC_API_KEY") else "openai"

    if provider == "anthropic":
        model = "claude-3-5-sonnet-20241022"
    else:
        model = "gpt-4o"

    return LLMConfig(provider=provider, model=model)


# =============================================================================
# LLM PROMPTS
# =============================================================================

LEADERSHIP_EXTRACTION_PROMPT = """You are extracting leadership information from a company webpage.

Company: {company_name}
Page URL: {page_url}

Extract ALL people mentioned who appear to be in leadership/management roles.
For each person, provide the following in JSON format:

{{
  "people": [
    {{
      "full_name": "First Last",
      "title": "Their exact title as shown",
      "title_normalized": "Standardized title (CEO, CFO, VP Sales, etc.)",
      "title_level": "c_suite|president|evp|svp|vp|director|manager|board|unknown",
      "department": "Department if mentioned (Sales, Finance, Operations, etc.)",
      "bio": "Brief bio if available (1-2 sentences max)",
      "is_board_member": true/false,
      "is_executive": true/false,
      "reports_to": "Name of person they report to if inferable",
      "linkedin_url": "LinkedIn URL if visible on page",
      "email": "Email if visible on page",
      "photo_url": "Photo URL if visible"
    }}
  ],
  "extraction_confidence": "high|medium|low",
  "page_type": "leadership|team|about|board",
  "notes": "Any issues or observations"
}}

Rules:
1. Only include people who appear to be employees/leaders, not testimonials or clients
2. Infer department from title if not explicit (e.g., "VP of Sales" -> department: "Sales")
3. Set title_level based on title keywords:
   - c_suite: CEO, CFO, COO, CTO, CMO, CHRO, CRO, etc.
   - president: President, President & COO
   - evp: Executive Vice President
   - svp: Senior Vice President
   - vp: Vice President
   - director: Director
   - manager: Manager
   - board: Board Member, Board Director, Chairman
4. If bio is very long, summarize to 1-2 sentences
5. reports_to can be inferred from page structure (e.g., if someone is under CEO section)
6. Set extraction_confidence based on page clarity
7. Return valid JSON only, no other text

Page HTML:
{html_content}"""


PRESS_RELEASE_EXTRACTION_PROMPT = """You are extracting leadership change information from a press release.

Company: {company_name}
Press Release Date: {date}

Extract any leadership changes (appointments, promotions, departures) mentioned.
Return JSON format:

{{
  "changes": [
    {{
      "person_name": "Full name",
      "change_type": "hire|promotion|departure|retirement|board_appointment|board_departure|interim",
      "new_title": "New title (null if departure)",
      "old_title": "Previous title (null if new hire)",
      "old_company": "Previous company if external hire",
      "effective_date": "YYYY-MM-DD if mentioned, null otherwise",
      "is_c_suite": true/false,
      "is_board": true/false,
      "reason": "Brief reason if mentioned",
      "successor_name": "Name of successor if mentioned",
      "predecessor_name": "Name of predecessor if mentioned"
    }}
  ],
  "extraction_confidence": "high|medium|low"
}}

Rules:
1. Only extract actual changes, not mentions of existing roles
2. "Appointed", "named", "joins" = hire or promotion
3. "Retires", "steps down", "resigns", "departs" = departure/retirement
4. "Promoted to", "elevated to" = promotion
5. Board appointments are separate from executive appointments
6. effective_date should be in YYYY-MM-DD format
7. Return valid JSON only

Press Release Text:
{text}"""


BIO_PARSING_PROMPT = """You are parsing an executive bio to extract structured information.

Person: {person_name}
Current Company: {company_name}

Extract the following from the bio text:

{{
  "experience": [
    {{
      "company": "Company name",
      "title": "Title held",
      "start_year": 2020,
      "end_year": 2023,
      "is_current": false,
      "description": "Brief description if notable"
    }}
  ],
  "education": [
    {{
      "institution": "University name",
      "degree": "MBA, BS, etc.",
      "field": "Field of study",
      "graduation_year": 2005
    }}
  ],
  "board_positions": [
    {{
      "organization": "Board org name",
      "role": "Board Member, Director, etc.",
      "is_current": true
    }}
  ],
  "certifications": ["CPA", "CFA", etc.],
  "military_service": "Branch and rank if mentioned",
  "notable_achievements": ["Key achievement 1"]
}}

Rules:
1. Extract all work experience mentioned, ordered by recency
2. If only years are mentioned, use those (not full dates)
3. Education should include all degrees mentioned
4. Board positions include corporate boards, nonprofits, advisory
5. Only include certifications that are professional credentials
6. Return valid JSON only

Bio Text:
{bio_text}"""


SEC_PROXY_EXTRACTION_PROMPT = """Extract executive and board member names from this SEC proxy filing for {company_name}.

Look for:
1. Named Executive Officers (NEOs) - usually listed with titles like CEO, CFO, COO, President, VP
2. Board of Directors - directors, chairman, independent directors

Return ONLY valid JSON:
{{
  "executives": [
    {{"full_name": "John Smith", "title": "Chief Executive Officer", "age": 55}},
    {{"full_name": "Jane Doe", "title": "Chief Financial Officer", "age": 48}}
  ],
  "board_members": [
    {{"full_name": "Bob Wilson", "title": "Chairman", "is_independent": false}},
    {{"full_name": "Mary Johnson", "title": "Independent Director", "is_independent": true}}
  ],
  "extraction_confidence": "high"
}}

IMPORTANT:
- Extract ALL executives and board members mentioned
- Names should be in "First Last" format
- Return valid JSON only, no other text
- If age is mentioned, include it; otherwise omit
- Look for patterns like "Name, age X, has served as Title"

SEC Filing Sections:
{filing_text}"""


# =============================================================================
# URL PATTERNS FOR LEADERSHIP PAGES
# =============================================================================

LEADERSHIP_URL_PATTERNS: List[str] = [
    # Standard about/leadership patterns
    "/about/leadership",
    "/about/team",
    "/about/our-team",
    "/about/management",
    "/about/executives",
    "/about/people",
    "/about/staff",
    "/about/our-people",
    "/about/our-leadership",
    "/about/executive-team",
    "/about/management-team",
    "/about/leadership-team",
    # About-us variants
    "/about-us/leadership",
    "/about-us/team",
    "/about-us/management",
    "/about-us/our-team",
    "/about-us/people",
    "/about-us/executive-team",
    "/about-us/leadership-team",
    "/about-us/management-team",
    # Company section patterns
    "/company/leadership",
    "/company/team",
    "/company/management",
    "/company/about/leadership",
    "/company/about/team",
    "/company/about-us/team",
    "/company/about-us/leadership",
    "/company/people",
    "/company/our-team",
    # Corporate patterns
    "/corporate/leadership",
    "/corporate/management",
    "/corporate/team",
    "/corporate/about/leadership",
    "/corporate/executive-team",
    # Root-level patterns
    "/leadership",
    "/leadership-team",
    "/our-leadership",
    "/our-team",
    "/team",
    "/management",
    "/management-team",
    "/executives",
    "/executive-team",
    "/people",
    "/staff",
    "/our-people",
    "/meet-the-team",
    "/meet-our-team",
    # Who we are patterns
    "/who-we-are/leadership",
    "/who-we-are/team",
    "/who-we-are/our-team",
    "/who-we-are/people",
    "/who-we-are/management",
    # Board/governance patterns
    "/about/board",
    "/about/board-of-directors",
    "/corporate/board",
    "/corporate/board-of-directors",
    "/governance/board",
    "/governance/board-of-directors",
    "/investors/corporate-governance/board-of-directors",
    "/investor-relations/corporate-governance/board",
    "/investor-relations/leadership",
    "/ir/governance/board",
    "/board",
    "/board-of-directors",
    "/directors",
    # International/localized patterns
    "/en/about/team",
    "/en/about/leadership",
    "/en/company/team",
    "/en/team",
    "/en/leadership",
    "/en-us/about/team",
    "/en-us/about/leadership",
    "/us/about/team",
    "/us/about/leadership",
    "/en-gb/about/team",
    # SaaS/tech company patterns
    "/company",  # Many SaaS sites put team on /company
    "/about",  # Sometimes team is on main about page
    # NOTE: /careers/team and /careers/leadership removed - they match career pages, not leadership team pages
    # Founders/partners patterns (for smaller companies)
    "/founders",
    "/partners",
    "/our-founders",
    "/about/founders",
    # Additional variants
    "/info/leadership",
    "/info/team",
    "/pages/leadership",
    "/pages/team",
    "/site/leadership",
    "/our-company/leadership",
    "/our-company/team",
    # Investor relations patterns (important for public companies)
    "/investors/governance",
    "/investors/leadership",
    "/investors/management",
    "/investors/corporate-governance",
    "/investors/corporate-governance/leadership",
    "/investors/corporate-governance/management",
    "/investors/corporate-governance/executive-officers",
    "/investor-relations/governance",
    "/investor-relations/corporate-governance",
    "/investor-relations/management",
    "/investor-relations/executive-team",
    "/ir/governance",
    "/ir/leadership",
    "/ir/management",
    "/ir/corporate-governance",
    # Industrial/manufacturing company patterns
    "/about/senior-leadership",
    "/about/officers",
    "/about/executive-officers",
    "/about/corporate-officers",
    "/corporate/officers",
    "/corporate/executive-officers",
    "/corporate/senior-leadership",
    "/company/officers",
    "/company/executive-officers",
    # More root-level patterns
    "/senior-leadership",
    "/officers",
    "/executive-officers",
    "/corporate-officers",
    "/our-leaders",
    "/meet-our-leaders",
    "/meet-leadership",
    # Alternative "about" spellings
    "/aboutus/leadership",
    "/aboutus/team",
    "/about_us/leadership",
    "/about_us/team",
    # Governance patterns (common for large corps like ABB)
    "/about/governance",
    "/about/governance/executive-committee",
    "/about/governance/board-of-directors",
    "/about/our-management",
    "/about/our-executives",
    "/company/governance",
    "/corporate-governance",
    "/corporate-governance/board-of-directors",
    "/corporate-governance/leadership",
    "/corporate-governance/executive-officers",
    "/executive-committee",
    "/our-management",
    # .html suffix patterns (many corporate sites use these)
    "/about/leadership.html",
    "/about/team.html",
    "/about/management.html",
    "/about/board-of-directors.html",
    "/company/leadership.html",
    "/leadership.html",
    "/board-of-directors.html",
    # .aspx patterns (common for investor relations)
    "/governance/leadership-team/default.aspx",
    "/governance/board-of-directors/default.aspx",
    "/corporate-governance/default.aspx",
]

LEADERSHIP_LINK_PATTERNS: List[str] = [
    # Primary leadership keywords
    r"leadership",
    r"leadership team",
    r"executive team",
    r"executive leadership",
    r"senior leadership",
    r"management team",
    r"management",
    r"executives",
    # Team keywords
    r"team",
    r"our team",
    r"the team",
    r"meet the team",
    r"meet our team",
    # People keywords
    r"our people",
    r"people",
    r"staff",
    r"our staff",
    # About keywords
    r"about us",
    r"about$",  # Just "About" link
    r"who we are",
    r"company",
    # Board keywords
    r"board of directors",
    r"board",
    r"directors",
    r"governance",
    # Founders/partners
    r"founders",
    r"partners",
    r"our founders",
]


# =============================================================================
# TITLE NORMALIZATION
# =============================================================================

TITLE_NORMALIZATIONS: Dict[str, str] = {
    # CEO variants
    "chief executive officer": "CEO",
    "ceo": "CEO",
    "president and ceo": "President & CEO",
    "president & ceo": "President & CEO",
    "president/ceo": "President & CEO",
    "ceo and president": "President & CEO",
    "chief executive": "CEO",
    # CFO variants
    "chief financial officer": "CFO",
    "cfo": "CFO",
    "vp finance": "VP Finance",
    "vice president of finance": "VP Finance",
    "vice president, finance": "VP Finance",
    "svp finance": "SVP Finance",
    "evp & cfo": "EVP & CFO",
    # COO variants
    "chief operating officer": "COO",
    "coo": "COO",
    "president and coo": "President & COO",
    "president & coo": "President & COO",
    # CTO variants
    "chief technology officer": "CTO",
    "cto": "CTO",
    "chief information officer": "CIO",
    "cio": "CIO",
    "chief digital officer": "CDO",
    # Sales leadership
    "chief revenue officer": "CRO",
    "cro": "CRO",
    "chief sales officer": "CSO",
    "chief commercial officer": "CCO",
    "vp sales": "VP Sales",
    "vp of sales": "VP Sales",
    "vice president of sales": "VP Sales",
    "vice president, sales": "VP Sales",
    "svp sales": "SVP Sales",
    "evp sales": "EVP Sales",
    "head of sales": "Head of Sales",
    "sales director": "Director of Sales",
    # Marketing
    "chief marketing officer": "CMO",
    "cmo": "CMO",
    "vp marketing": "VP Marketing",
    "vice president of marketing": "VP Marketing",
    # Operations
    "vp operations": "VP Operations",
    "vice president of operations": "VP Operations",
    "vice president, operations": "VP Operations",
    "director of operations": "Director of Operations",
    "svp operations": "SVP Operations",
    # HR
    "chief human resources officer": "CHRO",
    "chro": "CHRO",
    "chief people officer": "CPO",
    "cpo": "CPO",
    "vp human resources": "VP Human Resources",
    "vp hr": "VP Human Resources",
    "vice president of human resources": "VP Human Resources",
    "head of hr": "Head of HR",
    # Legal
    "general counsel": "General Counsel",
    "chief legal officer": "CLO",
    "clo": "CLO",
    "vp legal": "VP Legal",
    # Board
    "chairman": "Chairman",
    "chairman of the board": "Chairman",
    "chair": "Chairman",
    "vice chairman": "Vice Chairman",
    "board member": "Board Member",
    "director": "Board Director",
    "independent director": "Independent Director",
    "lead independent director": "Lead Independent Director",
}

TITLE_LEVEL_KEYWORDS: Dict[str, List[str]] = {
    "c_suite": [
        "chief",
        "ceo",
        "cfo",
        "coo",
        "cto",
        "cmo",
        "cio",
        "cro",
        "chro",
        "clo",
        "cpo",
        "cdo",
    ],
    "president": ["president"],
    "evp": ["executive vice president", "evp"],
    "svp": ["senior vice president", "svp"],
    "vp": ["vice president", "vp"],
    "director": ["director"],
    "manager": ["manager"],
    "board": ["chairman", "board member", "board director", "trustee"],
}


# =============================================================================
# DEPARTMENT KEYWORDS
# =============================================================================

DEPARTMENT_KEYWORDS: Dict[str, List[str]] = {
    "finance": ["finance", "financial", "cfo", "accounting", "treasury", "controller"],
    "operations": [
        "operations",
        "coo",
        "manufacturing",
        "production",
        "supply chain",
        "logistics",
    ],
    "sales": ["sales", "revenue", "commercial", "cro"],
    "marketing": ["marketing", "cmo", "brand", "communications", "pr"],
    "hr": ["human resources", "hr", "people", "talent", "chro"],
    "it": ["information technology", "it", "cio", "technology", "digital", "cto"],
    "legal": ["legal", "general counsel", "clo", "compliance"],
    "engineering": ["engineering", "r&d", "research", "development", "product"],
    "strategy": ["strategy", "corporate development", "m&a", "business development"],
}


# =============================================================================
# COLLECTION SETTINGS
# =============================================================================


class CollectionSettings(BaseModel):
    """Global settings for collection jobs."""

    max_concurrent_companies: int = 5
    max_pages_per_company: int = 10
    html_max_length: int = 500000  # Max HTML size to send to LLM
    min_confidence_to_save: str = "low"  # Save all extractions
    dedupe_threshold: float = 0.85  # Name similarity threshold for dedup


COLLECTION_SETTINGS = CollectionSettings()
