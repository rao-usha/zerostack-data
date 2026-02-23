"""
Job Posting Intelligence — table schemas and normalization helpers.
"""

import re
from typing import Optional

# ---------------------------------------------------------------------------
# SQL table definitions
# ---------------------------------------------------------------------------

def generate_create_job_postings_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS job_postings (
        id              SERIAL PRIMARY KEY,
        company_id      INTEGER REFERENCES industrial_companies(id),
        external_job_id TEXT,
        title           TEXT NOT NULL,
        title_normalized TEXT,
        department      TEXT,
        team            TEXT,
        location        TEXT,
        locations_all   JSONB,
        employment_type TEXT,
        workplace_type  TEXT,
        seniority_level TEXT,
        salary_min      NUMERIC(12,2),
        salary_max      NUMERIC(12,2),
        salary_currency TEXT DEFAULT 'USD',
        salary_interval TEXT,
        description_text TEXT,
        requirements    JSONB,
        source_url      TEXT,
        ats_type        TEXT,
        status          TEXT DEFAULT 'open',
        first_seen_at   TIMESTAMP DEFAULT NOW(),
        last_seen_at    TIMESTAMP DEFAULT NOW(),
        closed_at       TIMESTAMP,
        posted_date     TIMESTAMP,
        created_at      TIMESTAMP DEFAULT NOW(),
        UNIQUE(company_id, external_job_id)
    );

    CREATE INDEX IF NOT EXISTS idx_jp_company_id     ON job_postings(company_id);
    CREATE INDEX IF NOT EXISTS idx_jp_ats_type       ON job_postings(ats_type);
    CREATE INDEX IF NOT EXISTS idx_jp_status         ON job_postings(status);
    CREATE INDEX IF NOT EXISTS idx_jp_department     ON job_postings(department);
    CREATE INDEX IF NOT EXISTS idx_jp_location       ON job_postings(location);
    CREATE INDEX IF NOT EXISTS idx_jp_seniority      ON job_postings(seniority_level);
    CREATE INDEX IF NOT EXISTS idx_jp_employment     ON job_postings(employment_type);
    """


def generate_create_company_ats_config_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS company_ats_config (
        id                    SERIAL PRIMARY KEY,
        company_id            INTEGER UNIQUE REFERENCES industrial_companies(id),
        ats_type              TEXT,
        board_token           TEXT,
        careers_url           TEXT,
        api_url               TEXT,
        last_crawled_at       TIMESTAMP,
        last_successful_crawl TIMESTAMP,
        total_postings        INTEGER,
        crawl_status          TEXT DEFAULT 'pending',
        error_message         TEXT,
        created_at            TIMESTAMP DEFAULT NOW(),
        updated_at            TIMESTAMP DEFAULT NOW()
    );
    """


def generate_create_job_posting_snapshots_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS job_posting_snapshots (
        id                SERIAL PRIMARY KEY,
        company_id        INTEGER REFERENCES industrial_companies(id),
        snapshot_date     DATE,
        total_open        INTEGER,
        new_postings      INTEGER,
        closed_postings   INTEGER,
        by_department     JSONB,
        by_location       JSONB,
        by_seniority      JSONB,
        by_employment_type JSONB,
        created_at        TIMESTAMP DEFAULT NOW(),
        UNIQUE(company_id, snapshot_date)
    );
    """


def generate_create_job_posting_alerts_sql() -> str:
    return """
    CREATE TABLE IF NOT EXISTS job_posting_alerts (
        id              SERIAL PRIMARY KEY,
        company_id      INTEGER REFERENCES industrial_companies(id),
        alert_type      TEXT NOT NULL,
        severity        TEXT DEFAULT 'medium',
        snapshot_date   DATE NOT NULL,
        current_total   INTEGER,
        previous_total  INTEGER,
        change_pct      NUMERIC(8,2),
        change_abs      INTEGER,
        department      TEXT,
        details         JSONB,
        acknowledged    BOOLEAN DEFAULT FALSE,
        created_at      TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_jpa_company_id ON job_posting_alerts(company_id);
    CREATE INDEX IF NOT EXISTS idx_jpa_alert_type ON job_posting_alerts(alert_type);
    CREATE INDEX IF NOT EXISTS idx_jpa_severity ON job_posting_alerts(severity);
    CREATE INDEX IF NOT EXISTS idx_jpa_snapshot_date ON job_posting_alerts(snapshot_date);
    """


# ---------------------------------------------------------------------------
# Dataset registry info
# ---------------------------------------------------------------------------

DATASET_INFO = {
    "job_postings": {
        "dataset_id": "job_postings",
        "display_name": "Job Postings",
        "description": "Job posting intelligence collected from corporate ATS platforms",
        "table_name": "job_postings",
    },
    "company_ats_config": {
        "dataset_id": "company_ats_config",
        "display_name": "Company ATS Config",
        "description": "ATS detection results and crawl configuration per company",
        "table_name": "company_ats_config",
    },
    "job_posting_snapshots": {
        "dataset_id": "job_posting_snapshots",
        "display_name": "Job Posting Snapshots",
        "description": "Daily aggregate snapshots for hiring trend analysis",
        "table_name": "job_posting_snapshots",
    },
}


# ---------------------------------------------------------------------------
# Title normalization and seniority detection
# ---------------------------------------------------------------------------

# Ordered from highest to lowest so first match wins
SENIORITY_PATTERNS: list[tuple[str, str]] = [
    (r"\b(chief|ceo|cfo|cto|coo|cmo|cio|ciso|cpo|cro)\b", "c_suite"),
    (r"\b(president|chairman)\b", "c_suite"),
    (r"\bvice\s*president\b", "vp"),
    (r"\b(svp|evp|avp)\b", "vp"),
    (r"\bvp\b", "vp"),
    (r"\bdirector\b", "director"),
    (r"\b(head of|principal)\b", "director"),
    (r"\b(lead|staff|team lead)\b", "lead"),
    (r"\bmanager\b", "lead"),
    (r"\bsenior\b", "senior"),
    (r"\bsr\.?\b", "senior"),
    (r"\bjunior\b", "entry"),
    (r"\bjr\.?\b", "entry"),
    (r"\bentry[\s-]?level\b", "entry"),
    (r"\bintern\b", "entry"),
    (r"\bco-op\b", "entry"),
    (r"\bassociate\b", "mid"),
]


def detect_seniority(title: str) -> Optional[str]:
    """Detect seniority level from a job title."""
    t = title.lower()
    for pattern, level in SENIORITY_PATTERNS:
        if re.search(pattern, t):
            return level
    return "mid"  # default


EMPLOYMENT_ALIASES = {
    "full-time": "full_time",
    "full time": "full_time",
    "fulltime": "full_time",
    "part-time": "part_time",
    "part time": "part_time",
    "parttime": "part_time",
    "contract": "contract",
    "contractor": "contract",
    "temporary": "contract",
    "temp": "contract",
    "intern": "intern",
    "internship": "intern",
    "co-op": "intern",
    "coop": "intern",
    "freelance": "contract",
}


def normalize_employment_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return EMPLOYMENT_ALIASES.get(raw.lower().strip(), raw.lower().strip())


WORKPLACE_ALIASES = {
    "remote": "remote",
    "fully remote": "remote",
    "on-site": "onsite",
    "onsite": "onsite",
    "in-office": "onsite",
    "in office": "onsite",
    "hybrid": "hybrid",
}


def normalize_workplace_type(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    return WORKPLACE_ALIASES.get(raw.lower().strip(), raw.lower().strip())


def normalize_title(title: str) -> str:
    """Light normalization: strip extra whitespace, collapse Roman numerals, etc."""
    t = re.sub(r"\s+", " ", title.strip())
    # Remove trailing comma / dash artifacts
    t = re.sub(r"[,\-–—]+\s*$", "", t).strip()
    return t
