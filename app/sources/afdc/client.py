"""
AFDC (Alternative Fuels Data Center) API client.

Wraps the NREL AFDC REST API v1 for EV charging station data.
API docs: https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/
"""

import logging
from typing import Any, Dict, Optional

from app.core.http_client import BaseAPIClient
from app.core.api_errors import FatalError, RetryableError
from app.core.api_registry import get_api_config

logger = logging.getLogger(__name__)

# Fallback key for unauthenticated testing (NREL-provided demo key)
_DEMO_KEY = "DEMO_KEY"


class AFDCClient(BaseAPIClient):
    """HTTP client for the NREL AFDC API."""

    SOURCE_NAME = "afdc"
    BASE_URL = "https://developer.nrel.gov/api/alt-fuel-stations/v1"

    def __init__(self, api_key: Optional[str] = None, max_concurrency: int = 2):
        config = get_api_config("afdc")
        super().__init__(
            api_key=api_key or _DEMO_KEY,
            max_concurrency=max_concurrency,
            max_retries=config.max_retries,
            backoff_factor=2.0,
            timeout=config.timeout_seconds,
            connect_timeout=config.connect_timeout_seconds,
            rate_limit_interval=config.get_rate_limit_interval(),
        )
        if not api_key or api_key == _DEMO_KEY:
            logger.warning(
                "AFDC client using DEMO_KEY — rate-limited (50 req/day). "
                "Set DATA_GOV_API for production use."
            )

    def _add_auth_to_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        params["api_key"] = self.api_key
        return params

    def _check_api_error(
        self, data: Dict[str, Any], resource_id: str
    ) -> Optional[Exception]:
        if "error" in data:
            msg = data["error"].get("message", str(data["error"]))
            status = data["error"].get("code", 500)
            if status in (400, 401, 403, 404):
                return FatalError(
                    message=f"AFDC API error {status}: {msg}",
                    source=self.SOURCE_NAME,
                    status_code=status,
                    response_data=data,
                )
            return RetryableError(
                message=f"AFDC API error {status}: {msg}",
                source=self.SOURCE_NAME,
                status_code=status,
                response_data=data,
            )
        return None

    async def get_ev_station_counts_by_state(self) -> Dict[str, Any]:
        """
        Fetch EV charging station counts grouped by state.

        Returns dict with 'total' and 'state_counts' mapping state → count.
        Queries the AFDC v1.json endpoint once per state with limit=0 to get
        total_results without downloading station records.
        """
        us_states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC",
        ]

        state_counts: Dict[str, Any] = {}
        grand_total = 0

        for state in us_states:
            params = {"fuel_type": "ELEC", "status": "E", "state": state, "limit": 0}
            try:
                raw = await self.get(
                    "https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
                    params=params,
                    resource_id=f"ev_stations_{state}",
                )
                count = raw.get("total_results", 0) or 0
                state_counts[state] = count
                grand_total += count
            except Exception as exc:
                logger.warning("AFDC: failed to fetch station count for %s: %s", state, exc)
                state_counts[state] = None

        return {
            "total": grand_total,
            "state_counts": state_counts,
        }
