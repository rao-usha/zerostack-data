"""
Run pre-flight checks shared by the job runner and the dataset status API.

``api_key_preflight`` used to live in ``app/api/v1/jobs.py`` as the private
``_check_api_key_preflight``, called only once a job was already running.
SPEC_124 needs the same answer *before* anything runs, to explain a dead
source (``blocked``) and to refuse ``POST /datasets/{key}/run``, so it lives
here and ``jobs._check_api_key_preflight`` delegates to it.
"""

from typing import Optional


def api_key_preflight(source: str) -> Optional[str]:
    """Return an error message if a REQUIRED API key is missing, else None.

    ``source`` may carry a dataset suffix (``job_postings:all``); only the
    base source is looked up. Sources absent from ``API_REGISTRY`` or whose
    key is optional/recommended always pass.
    """
    from app.core.api_registry import API_REGISTRY, APIKeyRequirement
    from app.core.config import get_settings

    base_source = (source or "").split(":")[0]

    api_config = API_REGISTRY.get(base_source)
    if not api_config or api_config.api_key_requirement != APIKeyRequirement.REQUIRED:
        return None

    settings = get_settings()
    try:
        settings.get_api_key(base_source, required=True)
        return None
    except Exception:
        return f"API key required for '{base_source}' but not configured"
