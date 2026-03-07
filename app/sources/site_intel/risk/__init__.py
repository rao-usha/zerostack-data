"""
Risk & Environmental Collectors.

Data sources:
- FEMA: Flood zones, National Risk Index
- USGS: Seismic hazard, faults
- NOAA: Climate data
- EPA: Environmental facilities
- USFWS: Wetlands
"""

from app.sources.site_intel.risk.fema_collector import FEMARiskCollector
from app.sources.site_intel.risk.usgs_earthquake_collector import (
    USGSEarthquakeCollector,
)
from app.sources.site_intel.risk.epa_envirofacts_collector import (
    EPAEnvirofactsCollector,
)
from app.sources.site_intel.risk.fema_nfhl_collector import FEMANFHLFloodCollector
from app.sources.site_intel.risk.epa_acres_collector import EPAACRESCollector
from app.sources.site_intel.risk.nwi_wetland_collector import NWIWetlandCollector
from app.sources.site_intel.risk.usgs_elevation_collector import USGS3DEPElevationCollector

__all__ = [
    "FEMARiskCollector",
    "USGSEarthquakeCollector",
    "EPAEnvirofactsCollector",
    "FEMANFHLFloodCollector",
    "EPAACRESCollector",
    "NWIWetlandCollector",
    "USGS3DEPElevationCollector",
]
