"""
Water & Utilities Domain Collectors.

Collects data for industrial site selection related to water availability,
public water systems, natural gas infrastructure, and utility electricity rates.

Sources:
- USGS Water Data: Streamflow, groundwater monitoring
- EPA SDWIS: Public water systems, violations
- EIA Natural Gas: Pipelines, storage facilities
- OpenEI URDB: Utility rate database
- EIA Electricity: Electricity prices and consumption
"""

from app.sources.site_intel.water_utilities.usgs_water_collector import (
    USGSWaterCollector,
)
from app.sources.site_intel.water_utilities.epa_sdwis_collector import EPASDWISCollector
from app.sources.site_intel.water_utilities.eia_gas_collector import EIAGasCollector
from app.sources.site_intel.water_utilities.openei_rates_collector import (
    OpenEIRatesCollector,
)
from app.sources.site_intel.water_utilities.eia_electricity_collector import (
    EIAElectricityCollector,
)

__all__ = [
    "USGSWaterCollector",
    "EPASDWISCollector",
    "EIAGasCollector",
    "OpenEIRatesCollector",
    "EIAElectricityCollector",
]
