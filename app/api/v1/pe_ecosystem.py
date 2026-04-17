"""
PE Intelligence Platform — Ecosystem Seed API (PLAN_060 Phase 0).

Endpoints to seed, query, and purge the PE demo data ecosystem.
"""

from typing import Dict

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.services.pe_ecosystem_seed import PEEcosystemSeeder

router = APIRouter(prefix="/pe/ecosystem", tags=["PE Ecosystem"])


@router.post("/seed", summary="Seed PE ecosystem with synthetic data")
def seed_ecosystem(seed: int = 42, db: Session = Depends(get_db)) -> Dict:
    """
    Populate all PE tables with realistic, internally-consistent synthetic data.
    Idempotent — purges prior seed data before re-inserting.
    """
    seeder = PEEcosystemSeeder(db)
    stats = seeder.seed(seed=seed)
    return {"status": "seeded", "rows": stats}


@router.get("/status", summary="PE table row counts")
def ecosystem_status(db: Session = Depends(get_db)) -> Dict:
    """Current row counts for all PE tables."""
    return PEEcosystemSeeder(db).status()


@router.delete("/seed", summary="Remove seeded PE data")
def purge_ecosystem(db: Session = Depends(get_db)) -> Dict:
    """Remove all rows tagged with data_source='pe_ecosystem_seed'."""
    counts = PEEcosystemSeeder(db).purge()
    return {"status": "purged", "rows_deleted": counts}
