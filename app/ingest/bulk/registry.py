"""
Registry of bulk sources (SPEC_107).

    @register_bulk_source
    class FormDDataSets(BulkSource):
        name = "sec_form_d"

Source modules live in ``app/ingest/bulk/<name>/`` and are imported by
``load_all()`` so decorators run.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from typing import Dict, List, Type

from app.ingest.bulk.base import BulkSource

logger = logging.getLogger(__name__)

BULK_SOURCES: Dict[str, Type[BulkSource]] = {}
_loaded = False


def register_bulk_source(cls: Type[BulkSource]) -> Type[BulkSource]:
    if not getattr(cls, "name", ""):
        raise ValueError(f"{cls.__name__} must set a non-empty `name`")
    existing = BULK_SOURCES.get(cls.name)
    if existing is not None and existing is not cls:
        raise ValueError(f"Duplicate bulk source name {cls.name!r}: {existing.__name__} vs {cls.__name__}")
    BULK_SOURCES[cls.name] = cls
    return cls


def load_all() -> None:
    """Import every subpackage of app.ingest.bulk so sources register themselves."""
    global _loaded
    if _loaded:
        return
    import app.ingest.bulk as pkg

    for mod in pkgutil.iter_modules(pkg.__path__):
        if mod.ispkg:
            try:
                importlib.import_module(f"app.ingest.bulk.{mod.name}")
            except Exception as e:
                logger.error(f"Failed to import bulk source package {mod.name}: {e}")
    _loaded = True


def list_sources() -> List[str]:
    load_all()
    return sorted(BULK_SOURCES)


def get_source(name: str) -> BulkSource:
    load_all()
    if name not in BULK_SOURCES:
        raise KeyError(f"Unknown bulk source {name!r}. Available: {sorted(BULK_SOURCES)}")
    return BULK_SOURCES[name]()
