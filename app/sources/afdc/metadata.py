"""
AFDC metadata: table schemas, dataset definitions, data parsing.
"""

import logging
from datetime import date
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------

DATASETS = {
    "ev_stations": {
        "table": "afdc_ev_stations",
        "display_name": "EV Charging Stations by State",
        "description": (
            "Count of electric vehicle (EV) charging stations by US state, "
            "sourced from the NREL Alternative Fuels Data Center (AFDC). "
            "Includes Level 1, Level 2, and DC Fast Charge station counts."
        ),
    },
}


def get_table_name(dataset: str) -> str:
    if dataset not in DATASETS:
        available = ", ".join(DATASETS)
        raise ValueError(f"Unknown AFDC dataset: {dataset}. Available: {available}")
    return DATASETS[dataset]["table"]


def get_dataset_info(dataset: str) -> Dict[str, str]:
    if dataset not in DATASETS:
        raise ValueError(f"Unknown AFDC dataset: {dataset}")
    return DATASETS[dataset]


# ---------------------------------------------------------------------------
# Table SQL
# ---------------------------------------------------------------------------

EV_STATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS afdc_ev_stations (
    id              SERIAL PRIMARY KEY,
    state           TEXT NOT NULL,
    total_stations  INTEGER,
    ev_level1       INTEGER,
    ev_level2       INTEGER,
    ev_dc_fast      INTEGER,
    as_of_date      DATE NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW(),
    CONSTRAINT afdc_ev_stations_unique UNIQUE (state, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_afdc_ev_stations_state ON afdc_ev_stations (state);
CREATE INDEX IF NOT EXISTS idx_afdc_ev_stations_date  ON afdc_ev_stations (as_of_date);
"""


def get_create_table_sql(dataset: str) -> str:
    if dataset == "ev_stations":
        return EV_STATIONS_TABLE_SQL
    raise ValueError(f"No SQL defined for dataset: {dataset}")


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_ev_stations_response(
    api_response: Dict[str, Any],
    as_of: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """
    Parse AFDC count-by-state response into DB rows.

    AFDC returns:
      {
        "total": 64000,
        "state_counts": {
          "CA": {"total": 15000, "ev_level1": 100, "ev_level2": 12000, "ev_dc_fast": 2900},
          ...
        }
      }

    Falls back to flat count if detailed breakdown not present.
    """
    if as_of is None:
        from datetime import date as _date
        as_of = _date.today()

    rows: List[Dict[str, Any]] = []
    state_counts = api_response.get("state_counts", {})

    for state, counts in state_counts.items():
        if not state or state == "None":
            continue

        if isinstance(counts, dict):
            row = {
                "state": state,
                "total_stations": counts.get("total"),
                "ev_level1": counts.get("ev_level1"),
                "ev_level2": counts.get("ev_level2"),
                "ev_dc_fast": counts.get("ev_dc_fast"),
                "as_of_date": as_of.isoformat(),
            }
        else:
            # Flat count format: state_counts = {"CA": 15000, ...}
            row = {
                "state": state,
                "total_stations": int(counts) if counts else None,
                "ev_level1": None,
                "ev_level2": None,
                "ev_dc_fast": None,
                "as_of_date": as_of.isoformat(),
            }

        rows.append(row)

    logger.info(f"Parsed {len(rows)} state records from AFDC ev_stations response")
    return rows
