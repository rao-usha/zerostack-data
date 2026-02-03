"""
Freight & Logistics Collectors.

Data sources:
- USDA AMS: Agricultural truck rates
- FMCSA: Motor carrier registry and safety data
- Freightos: FBX container freight rates
- Drewry: World Container Index
- SCFI: Shanghai Containerized Freight Index
- USACE/BTS: Port throughput data
- BTS T-100: Air cargo statistics
- Census: Trade gateway import/export data
- LoopNet: Warehouse property listings
- Transport Topics: 3PL company directory
"""

# Import all collectors to register them
from app.sources.site_intel.logistics.usda_truck_collector import UsdaTruckCollector
from app.sources.site_intel.logistics.fmcsa_collector import FMCSACollector
from app.sources.site_intel.logistics.freightos_collector import FreightosCollector
from app.sources.site_intel.logistics.drewry_collector import DrewryCollector
from app.sources.site_intel.logistics.scfi_collector import SCFICollector
from app.sources.site_intel.logistics.port_throughput_collector import PortThroughputCollector
from app.sources.site_intel.logistics.air_cargo_collector import AirCargoCollector
from app.sources.site_intel.logistics.census_trade_collector import CensusTradeCollector
from app.sources.site_intel.logistics.warehouse_listing_collector import WarehouseListingCollector
from app.sources.site_intel.logistics.three_pl_collector import ThreePLCollector

__all__ = [
    "UsdaTruckCollector",
    "FMCSACollector",
    "FreightosCollector",
    "DrewryCollector",
    "SCFICollector",
    "PortThroughputCollector",
    "AirCargoCollector",
    "CensusTradeCollector",
    "WarehouseListingCollector",
    "ThreePLCollector",
]
