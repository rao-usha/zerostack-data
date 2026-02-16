"""
Settings API — manage external source API keys.

Separate from api_keys.py which handles Nexdata's own nxd_* API keys.
This manages the keys used to call external data sources (FRED, EIA, Census, etc.).
"""
import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.database import get_db
from app.core.models import SourceAPIKey

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/settings", tags=["Settings"])


# ---------------------------------------------------------------------------
# Encryption helpers
# ---------------------------------------------------------------------------

def _get_fernet() -> Fernet:
    """
    Derive a Fernet key from ENCRYPTION_KEY env var, falling back to DATABASE_URL.
    The key is derived via SHA-256 and base64-encoded to 32 bytes (Fernet requirement).
    """
    import base64
    secret = os.environ.get("ENCRYPTION_KEY") or get_settings().database_url
    key_bytes = hashlib.sha256(secret.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return Fernet(fernet_key)


def _encrypt(plain: str) -> str:
    return _get_fernet().encrypt(plain.encode()).decode()


def _decrypt(token: str) -> str:
    return _get_fernet().decrypt(token.encode()).decode()


def _mask_key(key: str) -> str:
    """Return a masked version like 'sk-proj-...****1234'."""
    if not key:
        return ""
    if len(key) <= 8:
        return "****" + key[-2:]
    prefix = key[:7]
    suffix = key[-4:]
    return f"{prefix}...****{suffix}"


# ---------------------------------------------------------------------------
# Source metadata (reuses _API_KEY_MAP from config for single source of truth)
# ---------------------------------------------------------------------------

# Category groupings for UI display
SOURCE_CATEGORIES = {
    "Government Data": ["census", "fred", "eia", "bls", "noaa", "bea", "bts", "fbi_crime", "data_commons", "uspto"],
    "LLM / AI": ["openai", "anthropic"],
    "Location & Foot Traffic": ["safegraph", "placer", "foursquare", "yelp"],
}

SOURCE_DESCRIPTIONS = {
    "census": "U.S. Census Bureau demographics and housing data",
    "fred": "Federal Reserve Economic Data - 800K+ time series",
    "eia": "Energy Information Administration - energy prices and production",
    "bls": "Bureau of Labor Statistics - employment and CPI data",
    "noaa": "NOAA weather and climate observations",
    "bea": "Bureau of Economic Analysis - GDP and income data",
    "bts": "Bureau of Transportation Statistics - freight and border data",
    "fbi_crime": "FBI Crime Data Explorer (via data.gov key)",
    "data_commons": "Google Data Commons - unified public data",
    "yelp": "Yelp Fusion - business listings and reviews",
    "safegraph": "SafeGraph foot traffic patterns",
    "placer": "Placer.ai retail analytics",
    "foursquare": "Foursquare Places POI data",
    "openai": "OpenAI GPT models for LLM extraction",
    "anthropic": "Anthropic Claude models",
    "uspto": "USPTO PatentsView patent data",
}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class APIKeyInfo(BaseModel):
    source: str
    description: str
    signup_url: str
    category: str
    masked_key: Optional[str] = None
    configured: bool = False
    source_type: str = "db"  # "db" or "env"


class APIKeySave(BaseModel):
    source: str
    key: str


class APIKeyTestResult(BaseModel):
    source: str
    success: bool
    message: str


# ---------------------------------------------------------------------------
# Cache for DB key lookups (invalidated on writes)
# ---------------------------------------------------------------------------

_db_key_cache: dict[str, str] = {}
_cache_valid = False


def invalidate_key_cache():
    global _db_key_cache, _cache_valid
    _db_key_cache = {}
    _cache_valid = False


def get_cached_db_key(source: str, db: Session) -> Optional[str]:
    """Get decrypted key from DB with caching."""
    global _db_key_cache, _cache_valid

    if _cache_valid and source in _db_key_cache:
        return _db_key_cache[source]

    if not _cache_valid:
        # Reload full cache
        _db_key_cache.clear()
        rows = db.query(SourceAPIKey).all()
        for row in rows:
            try:
                _db_key_cache[row.source] = _decrypt(row.encrypted_key)
            except (InvalidToken, Exception) as e:
                logger.warning(f"Failed to decrypt key for {row.source}: {e}")
        _cache_valid = True

    return _db_key_cache.get(source)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/api-keys", response_model=list[APIKeyInfo])
def list_api_keys(db: Session = Depends(get_db)):
    """
    List all configurable source API keys with their status.
    Returns masked values - never the full key.
    """
    settings = get_settings()
    key_map = settings._API_KEY_MAP

    # Load all DB keys
    db_rows = {row.source: row for row in db.query(SourceAPIKey).all()}

    # Build category reverse-lookup
    source_to_category = {}
    for cat, sources in SOURCE_CATEGORIES.items():
        for s in sources:
            source_to_category[s] = cat

    results = []
    for source, (field_name, signup_url) in sorted(key_map.items()):
        # Check DB first, then env
        masked = None
        configured = False
        source_type = "env"

        if source in db_rows:
            try:
                plain = _decrypt(db_rows[source].encrypted_key)
                masked = _mask_key(plain)
                configured = True
                source_type = "db"
            except (InvalidToken, Exception):
                pass

        if not configured:
            env_val = getattr(settings, field_name, None)
            if env_val:
                masked = _mask_key(env_val)
                configured = True
                source_type = "env"

        results.append(APIKeyInfo(
            source=source,
            description=SOURCE_DESCRIPTIONS.get(source, f"API key for {source}"),
            signup_url=signup_url,
            category=source_to_category.get(source, "Other"),
            masked_key=masked,
            configured=configured,
            source_type=source_type,
        ))

    return results


@router.put("/api-keys")
def save_api_key(body: APIKeySave, db: Session = Depends(get_db)):
    """Save or update an API key for a source. Stored encrypted in DB."""
    settings = get_settings()
    if body.source.lower() not in settings._API_KEY_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown source: {body.source}")

    source = body.source.lower()
    encrypted = _encrypt(body.key)

    existing = db.query(SourceAPIKey).filter_by(source=source).first()
    if existing:
        existing.encrypted_key = encrypted
        existing.updated_at = datetime.utcnow()
    else:
        db.add(SourceAPIKey(source=source, encrypted_key=encrypted))

    db.commit()
    invalidate_key_cache()

    return {"status": "saved", "source": source, "masked_key": _mask_key(body.key)}


@router.delete("/api-keys/{source}")
def delete_api_key(source: str, db: Session = Depends(get_db)):
    """Remove a stored API key. The source will fall back to .env if available."""
    row = db.query(SourceAPIKey).filter_by(source=source.lower()).first()
    if not row:
        raise HTTPException(status_code=404, detail=f"No DB key found for {source}")

    db.delete(row)
    db.commit()
    invalidate_key_cache()

    return {"status": "deleted", "source": source}


@router.post("/api-keys/{source}/test", response_model=APIKeyTestResult)
async def test_api_key(source: str, db: Session = Depends(get_db)):
    """
    Test an API key by making a lightweight request to the source.
    Uses the DB key if stored, otherwise falls back to env.
    """
    source = source.lower()
    settings = get_settings()

    if source not in settings._API_KEY_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown source: {source}")

    # Resolve the key (DB first, then env)
    key = get_cached_db_key(source, db)
    if not key:
        field_name = settings._API_KEY_MAP[source][0]
        key = getattr(settings, field_name, None)

    if not key:
        return APIKeyTestResult(source=source, success=False, message="No API key configured")

    # Source-specific test calls
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            result = await _test_source_key(client, source, key)
            return result
    except httpx.TimeoutException:
        return APIKeyTestResult(source=source, success=False, message="Request timed out")
    except Exception as e:
        return APIKeyTestResult(source=source, success=False, message=str(e))


async def _test_source_key(client, source: str, key: str) -> APIKeyTestResult:
    """Run a lightweight test request for a specific source."""
    tests = {
        "fred": (
            "https://api.stlouisfed.org/fred/series?series_id=GNPCA&api_key={key}&file_type=json",
            200
        ),
        "eia": (
            "https://api.eia.gov/v2/?api_key={key}",
            200
        ),
        "census": (
            "https://api.census.gov/data/2023/acs/acs5?get=NAME&for=state:01&key={key}",
            200
        ),
        "bls": (
            "https://api.bls.gov/publicAPI/v2/timeseries/data/LNS14000000?registrationkey={key}&latest=true",
            200
        ),
        "noaa": (
            "https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets?limit=1",
            200
        ),
        "bea": (
            "https://apps.bea.gov/api/data/?method=GETDATASETLIST&UserID={key}&ResultFormat=JSON",
            200
        ),
        "data_commons": (
            "https://api.datacommons.org/v2/node?key={key}&nodes=country/USA&property=name",
            200
        ),
        "openai": (
            "https://api.openai.com/v1/models",
            200
        ),
        "anthropic": (
            "https://api.anthropic.com/v1/models",
            200
        ),
    }

    if source in tests:
        url_template, expected = tests[source]
        url = url_template.format(key=key)

        headers = {}
        if source == "noaa":
            headers["token"] = key
        elif source == "openai":
            headers["Authorization"] = f"Bearer {key}"
            url = "https://api.openai.com/v1/models"
        elif source == "anthropic":
            headers["x-api-key"] = key
            headers["anthropic-version"] = "2023-06-01"
            url = "https://api.anthropic.com/v1/models"

        resp = await client.get(url, headers=headers)
        if resp.status_code == expected:
            return APIKeyTestResult(source=source, success=True, message="Key is valid")
        elif resp.status_code in (401, 403):
            return APIKeyTestResult(source=source, success=False, message="Invalid or expired key")
        elif resp.status_code == 429:
            return APIKeyTestResult(source=source, success=True, message="Key is valid (rate limited)")
        else:
            return APIKeyTestResult(source=source, success=False, message=f"Unexpected status: {resp.status_code}")

    # Sources without a quick test endpoint
    return APIKeyTestResult(source=source, success=True, message="Key saved (no test endpoint available)")
