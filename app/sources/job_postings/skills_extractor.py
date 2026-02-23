"""
Job Posting Skills Extractor — regex-based extraction engine.

Parses job description text to extract:
- Technical skills (languages, frameworks, tools, platforms)
- Soft skills (leadership, communication, etc.)
- Certifications (CPA, PMP, AWS, etc.)
- Education requirements (degree level, field)
- Years of experience

Results populate the `requirements` JSONB column on job_postings.
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Skills taxonomy — ordered by category
# ---------------------------------------------------------------------------

# Technical skills: (display_name, [regex patterns])
# Patterns use word boundaries to avoid false positives.
TECH_SKILLS: list[tuple[str, list[str]]] = [
    # --- Programming languages ---
    ("Python", [r"\bpython\b"]),
    ("JavaScript", [r"\bjavascript\b", r"\bJS\b"]),
    ("TypeScript", [r"\btypescript\b", r"\bTS\b"]),
    ("Java", [r"\bjava\b(?!script)"]),
    ("C++", [r"\bc\+\+\b", r"\bcpp\b"]),
    ("C#", [r"\bc#\b", r"\bcsharp\b", r"\bc\s*sharp\b"]),
    ("Go", [r"\bgolang\b", r"\bgo\s+lang\b", r"\bgo\b(?=.*(?:backend|microservice|api|cloud|grpc|goroutine))"]),
    ("Rust", [r"\brust\b(?=.*(?:system|performance|low.level|memory|safe))"]),
    ("Ruby", [r"\bruby\b"]),
    ("PHP", [r"\bphp\b"]),
    ("Swift", [r"\bswift\b(?=.*(?:ios|apple|mobile|xcode))"]),
    ("Kotlin", [r"\bkotlin\b"]),
    ("Scala", [r"\bscala\b"]),
    ("R", [r"\bR\b(?=.*(?:statistic|data\s+science|ggplot|tidyverse|cran))"]),
    ("SQL", [r"\bsql\b"]),
    ("Shell/Bash", [r"\bbash\b", r"\bshell\s+script"]),
    ("Perl", [r"\bperl\b"]),
    ("Lua", [r"\blua\b"]),
    ("Dart", [r"\bdart\b"]),
    ("Elixir", [r"\belixir\b"]),
    ("Clojure", [r"\bclojure\b"]),
    ("Haskell", [r"\bhaskell\b"]),

    # --- Web frameworks ---
    ("React", [r"\breact(?:\.js|js)?\b"]),
    ("Angular", [r"\bangular(?:\.js|js)?\b"]),
    ("Vue.js", [r"\bvue(?:\.js|js)?\b"]),
    ("Next.js", [r"\bnext\.?js\b"]),
    ("Node.js", [r"\bnode\.?js\b", r"\bnodejs\b"]),
    ("Django", [r"\bdjango\b"]),
    ("Flask", [r"\bflask\b"]),
    ("FastAPI", [r"\bfastapi\b"]),
    ("Spring", [r"\bspring\s*(?:boot|framework|mvc)?\b"]),
    ("Rails", [r"\bruby\s+on\s+rails\b", r"\brails\b"]),
    ("Express", [r"\bexpress(?:\.js|js)?\b"]),
    ("ASP.NET", [r"\basp\.?net\b"]),
    ("Laravel", [r"\blaravel\b"]),
    ("Svelte", [r"\bsvelte\b"]),
    ("Remix", [r"\bremix\b(?=.*(?:react|web|framework))"]),
    ("GraphQL", [r"\bgraphql\b"]),
    ("REST", [r"\brest\s*(?:ful|api)?\b"]),

    # --- Data & ML ---
    ("TensorFlow", [r"\btensorflow\b"]),
    ("PyTorch", [r"\bpytorch\b"]),
    ("Pandas", [r"\bpandas\b"]),
    ("NumPy", [r"\bnumpy\b"]),
    ("Scikit-learn", [r"\bscikit[\s-]?learn\b", r"\bsklearn\b"]),
    ("Spark", [r"\bapache\s+spark\b", r"\bpyspark\b", r"\bspark\b"]),
    ("Hadoop", [r"\bhadoop\b"]),
    ("Kafka", [r"\bkafka\b"]),
    ("Airflow", [r"\bairflow\b"]),
    ("dbt", [r"\bdbt\b"]),
    ("Snowflake", [r"\bsnowflake\b"]),
    ("BigQuery", [r"\bbigquery\b", r"\bbig\s+query\b"]),
    ("Redshift", [r"\bredshift\b"]),
    ("Databricks", [r"\bdatabricks\b"]),
    ("Tableau", [r"\btableau\b"]),
    ("Power BI", [r"\bpower\s*bi\b"]),
    ("Looker", [r"\blooker\b"]),
    ("LLM/GenAI", [r"\bllm\b", r"\blarge\s+language\s+model", r"\bgenerative\s+ai\b", r"\bgen\s*ai\b"]),
    ("NLP", [r"\bnlp\b", r"\bnatural\s+language\s+processing\b"]),
    ("Computer Vision", [r"\bcomputer\s+vision\b", r"\bcv\b(?=.*(?:image|vision|detect))"]),
    ("Machine Learning", [r"\bmachine\s+learning\b", r"\bml\b(?=.*(?:model|pipeline|train|deploy))"]),
    ("Deep Learning", [r"\bdeep\s+learning\b"]),

    # --- Cloud & DevOps ---
    ("AWS", [r"\baws\b", r"\bamazon\s+web\s+services\b"]),
    ("Azure", [r"\bazure\b", r"\bmicrosoft\s+azure\b"]),
    ("GCP", [r"\bgcp\b", r"\bgoogle\s+cloud\b"]),
    ("Docker", [r"\bdocker\b"]),
    ("Kubernetes", [r"\bkubernetes\b", r"\bk8s\b"]),
    ("Terraform", [r"\bterraform\b"]),
    ("Ansible", [r"\bansible\b"]),
    ("Jenkins", [r"\bjenkins\b"]),
    ("GitHub Actions", [r"\bgithub\s+actions\b"]),
    ("CI/CD", [r"\bci\s*/\s*cd\b", r"\bcontinuous\s+(?:integration|delivery|deployment)\b"]),
    ("Linux", [r"\blinux\b"]),
    ("Nginx", [r"\bnginx\b"]),
    ("Serverless", [r"\bserverless\b", r"\blambda\b(?=.*(?:aws|function|serverless))"]),
    ("Helm", [r"\bhelm\b(?=.*(?:chart|kubernetes|k8s|deploy))"]),
    ("ArgoCD", [r"\bargocd\b", r"\bargo\s+cd\b"]),

    # --- Databases ---
    ("PostgreSQL", [r"\bpostgres(?:ql)?\b"]),
    ("MySQL", [r"\bmysql\b"]),
    ("MongoDB", [r"\bmongodb\b", r"\bmongo\b"]),
    ("Redis", [r"\bredis\b"]),
    ("Elasticsearch", [r"\belasticsearch\b", r"\belastic\s+search\b"]),
    ("DynamoDB", [r"\bdynamodb\b"]),
    ("Cassandra", [r"\bcassandra\b"]),
    ("Neo4j", [r"\bneo4j\b"]),
    ("SQLite", [r"\bsqlite\b"]),
    ("Oracle DB", [r"\boracle\s+(?:db|database)\b"]),
    ("SQL Server", [r"\bsql\s+server\b", r"\bmssql\b"]),

    # --- Mobile ---
    ("iOS", [r"\bios\b(?!.*(?:cisco))"]),
    ("Android", [r"\bandroid\b"]),
    ("React Native", [r"\breact\s+native\b"]),
    ("Flutter", [r"\bflutter\b"]),

    # --- Tools & Practices ---
    ("Git", [r"\bgit\b(?!hub|lab)"]),
    ("Agile/Scrum", [r"\bagile\b", r"\bscrum\b"]),
    ("Jira", [r"\bjira\b"]),
    ("Figma", [r"\bfigma\b"]),
    ("Webpack", [r"\bwebpack\b"]),
    ("gRPC", [r"\bgrpc\b"]),
    ("Microservices", [r"\bmicroservice\b"]),
    ("OAuth", [r"\boauth\b"]),
    ("SAML", [r"\bsaml\b"]),
    ("SSO", [r"\bsso\b", r"\bsingle\s+sign[\s-]?on\b"]),

    # --- Industry-specific ---
    ("SAP", [r"\bsap\b(?=.*(?:erp|hana|s4|module|abap|fico|mm|sd))"]),
    ("Salesforce", [r"\bsalesforce\b", r"\bsfdc\b"]),
    ("ServiceNow", [r"\bservicenow\b"]),
    ("Workday", [r"\bworkday\b(?=.*(?:hcm|erp|integration|report))"]),
]

# Soft skills
SOFT_SKILLS: list[tuple[str, list[str]]] = [
    ("Leadership", [r"\bleadership\b", r"\blead\s+(?:a\s+)?team"]),
    ("Communication", [r"\bcommunication\s+skills\b", r"\bexcellent\s+communicat"]),
    ("Problem Solving", [r"\bproblem[\s-]solving\b"]),
    ("Teamwork", [r"\bteamwork\b", r"\bcollaborat(?:e|ion|ive)\b"]),
    ("Project Management", [r"\bproject\s+management\b"]),
    ("Analytical", [r"\banalytical\s+(?:skills|thinking|mind)\b"]),
    ("Attention to Detail", [r"\battention\s+to\s+detail\b"]),
    ("Time Management", [r"\btime\s+management\b"]),
    ("Critical Thinking", [r"\bcritical\s+thinking\b"]),
    ("Mentoring", [r"\bmentor(?:ing|ship)?\b"]),
    ("Stakeholder Management", [r"\bstakeholder\s+management\b"]),
    ("Cross-functional", [r"\bcross[\s-]functional\b"]),
    ("Self-starter", [r"\bself[\s-]starter\b", r"\bself[\s-]motivated\b"]),
]

# Certifications
CERTIFICATIONS: list[tuple[str, list[str]]] = [
    # Cloud
    ("AWS Certified", [r"\baws\s+certif", r"\baws\s+(?:solutions?\s+architect|developer|sysops|devops)"]),
    ("Azure Certified", [r"\bazure\s+certif", r"\baz-\d{3}\b"]),
    ("GCP Certified", [r"\bgcp\s+certif", r"\bgoogle\s+cloud\s+certif"]),
    # Security
    ("CISSP", [r"\bcissp\b"]),
    ("CISM", [r"\bcism\b"]),
    ("CEH", [r"\bceh\b"]),
    ("CompTIA Security+", [r"\bsecurity\+\b", r"\bcomptia\s+security\b"]),
    ("SOC 2", [r"\bsoc\s*2\b"]),
    # Project/Process
    ("PMP", [r"\bpmp\b"]),
    ("Scrum Master", [r"\bscrum\s+master\b", r"\bcsm\b"]),
    ("Six Sigma", [r"\bsix\s+sigma\b"]),
    ("ITIL", [r"\bitil\b"]),
    # Data
    ("CPA", [r"\bcpa\b"]),
    ("CFA", [r"\bcfa\b"]),
    ("FRM", [r"\bfrm\b"]),
    # Other
    ("PE License", [r"\bpe\s+licen", r"\bprofessional\s+engineer\b"]),
    ("Series 7", [r"\bseries\s+7\b"]),
    ("Series 63", [r"\bseries\s+63\b"]),
]

# Education levels (ordered from highest to lowest)
EDUCATION_PATTERNS: list[tuple[str, list[str]]] = [
    ("phd", [r"\bph\.?d\b", r"\bdoctorate\b", r"\bdoctoral\b"]),
    ("masters", [r"\bmaster'?s?\s+degree\b", r"\bm\.?s\.?\b(?=.*(?:degree|comput|engineer|science|business))", r"\bmba\b", r"\bm\.?a\.?\b(?=.*degree)"]),
    ("bachelors", [r"\bbachelor'?s?\s+degree\b", r"\bb\.?s\.?\b(?=.*(?:degree|comput|engineer|science))", r"\bb\.?a\.?\b(?=.*degree)", r"\bundergraduate\s+degree\b"]),
    ("associate", [r"\bassociate'?s?\s+degree\b"]),
]

# Experience patterns — extract years
EXPERIENCE_PATTERNS = [
    r"(\d+)\+?\s*(?:years?|yrs?)[\s.]*(?:of\s+)?(?:relevant\s+)?(?:professional\s+)?(?:experience|exp\.?)\b",
    r"(?:minimum|at\s+least|over)\s+(\d+)\s*(?:years?|yrs?)",
    r"(\d+)\s*-\s*\d+\s*(?:years?|yrs?)[\s.]*(?:of\s+)?experience",
]

# Section header patterns to identify requirements sections (weighted higher)
REQUIREMENTS_SECTION_PATTERNS = [
    r"(?:^|\n)\s*(?:##?\s*)?(?:requirements?|qualifications?|what\s+you(?:'ll)?\s+(?:need|bring)|must\s+have|minimum\s+qualifications?|basic\s+qualifications?|required\s+skills?)\s*[:.\n]",
    r"(?:^|\n)\s*(?:##?\s*)?(?:nice\s+to\s+have|preferred\s+qualifications?|bonus\s+(?:skills?|points?)|desired\s+skills?|plus|ideal\s+candidate)\s*[:.\n]",
]


# ---------------------------------------------------------------------------
# Main extraction functions
# ---------------------------------------------------------------------------

def extract_skills(description: str, title: str = "") -> dict:
    """Extract structured skills/requirements from a job description.

    Returns a dict suitable for the `requirements` JSONB column:
    {
        "skills": ["Python", "AWS", ...],
        "soft_skills": ["Leadership", ...],
        "certifications": ["AWS Certified", ...],
        "education": "bachelors",
        "years_experience": 5,
        "years_experience_raw": "5+ years of experience",
        "skill_count": 12,
    }
    """
    if not description:
        return {}

    text = _prepare_text(description)
    title_lower = title.lower() if title else ""

    # Extract from full description
    skills = _extract_matches(text, TECH_SKILLS)
    soft = _extract_matches(text, SOFT_SKILLS)
    certs = _extract_matches(text, CERTIFICATIONS)
    education = _extract_education(text)
    years, years_raw = _extract_experience(text)

    # Boost skills that appear in the title
    if title_lower:
        title_skills = _extract_matches(title_lower, TECH_SKILLS)
        # Title skills go first
        skills = list(dict.fromkeys(title_skills + skills))

    result = {}
    if skills:
        result["skills"] = skills
    if soft:
        result["soft_skills"] = soft
    if certs:
        result["certifications"] = certs
    if education:
        result["education"] = education
    if years is not None:
        result["years_experience"] = years
    if years_raw:
        result["years_experience_raw"] = years_raw

    result["skill_count"] = len(skills) + len(soft) + len(certs)

    return result


def extract_skills_batch(postings: list[dict]) -> list[dict]:
    """Extract skills for a batch of postings. Modifies dicts in-place.

    Each posting dict should have 'description_text' and optionally 'title'.
    Sets 'requirements' key on each posting.
    """
    extracted = 0
    for posting in postings:
        desc = posting.get("description_text", "")
        title = posting.get("title", "")
        if desc:
            reqs = extract_skills(desc, title)
            if reqs and reqs.get("skill_count", 0) > 0:
                posting["requirements"] = reqs
                extracted += 1
    logger.info(f"Extracted skills for {extracted}/{len(postings)} postings")
    return postings


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _prepare_text(raw: str) -> str:
    """Clean HTML artifacts and normalize text for matching."""
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", raw)
    # Decode common entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&nbsp;", " ").replace("&#39;", "'").replace("&quot;", '"')
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)
    return text.lower()


def _extract_matches(text: str, taxonomy: list[tuple[str, list[str]]]) -> list[str]:
    """Find all matching skills from a taxonomy in the text."""
    found = []
    for display_name, patterns in taxonomy:
        for pat in patterns:
            try:
                if re.search(pat, text, re.IGNORECASE):
                    found.append(display_name)
                    break  # one match per skill is enough
            except re.error:
                continue
    return found


def _extract_education(text: str) -> Optional[str]:
    """Extract highest education requirement mentioned."""
    for level, patterns in EDUCATION_PATTERNS:
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                return level
    return None


def _extract_experience(text: str) -> tuple[Optional[int], Optional[str]]:
    """Extract years of experience. Returns (years_int, raw_match_text)."""
    for pat in EXPERIENCE_PATTERNS:
        match = re.search(pat, text, re.IGNORECASE)
        if match:
            try:
                years = int(match.group(1))
                if 0 < years <= 30:  # sanity check
                    return years, match.group(0).strip()
            except (ValueError, IndexError):
                continue
    return None, None
