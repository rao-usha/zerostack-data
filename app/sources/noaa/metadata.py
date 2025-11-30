"""
NOAA dataset metadata and configuration.

Defines available NOAA datasets and their characteristics.

NOAA CDO API Dataset IDs:
- GHCND: Global Historical Climatology Network - Daily
- GSOM: Global Summary of the Month
- GSOY: Global Summary of the Year
- NEXRAD2: Weather Radar (Level II)
- NEXRAD3: Weather Radar (Level III)
- NORMAL_DLY: Normals Daily
- NORMAL_HLY: Normals Hourly
- NORMAL_MLY: Normals Monthly
- PRECIP_15: Precipitation 15 Minute
- PRECIP_HLY: Precipitation Hourly

Reference: https://www.ncdc.noaa.gov/cdo-web/datasets
"""
from typing import Dict, List, Any
from datetime import date


class NOAADataset:
    """Configuration for a NOAA dataset."""
    
    def __init__(
        self,
        dataset_id: str,
        name: str,
        description: str,
        data_types: List[str],
        table_name: str,
        start_date: date,
        end_date: date,
        update_frequency: str = "daily"
    ):
        self.dataset_id = dataset_id
        self.name = name
        self.description = description
        self.data_types = data_types
        self.table_name = table_name
        self.start_date = start_date
        self.end_date = end_date
        self.update_frequency = update_frequency
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database storage."""
        return {
            "dataset_id": self.dataset_id,
            "name": self.name,
            "description": self.description,
            "data_types": self.data_types,
            "table_name": self.table_name,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "update_frequency": self.update_frequency
        }


# NOAA Datasets Configuration
NOAA_DATASETS: Dict[str, NOAADataset] = {
    "ghcnd_daily": NOAADataset(
        dataset_id="GHCND",
        name="Global Historical Climatology Network - Daily",
        description="Daily climate observations from weather stations worldwide",
        data_types=[
            "TMAX",  # Maximum temperature
            "TMIN",  # Minimum temperature
            "TAVG",  # Average temperature
            "PRCP",  # Precipitation
            "SNOW",  # Snowfall
            "SNWD",  # Snow depth
            "AWND",  # Average wind speed
        ],
        table_name="noaa_ghcnd_daily",
        start_date=date(2020, 1, 1),  # Default start (configurable)
        end_date=date(2024, 12, 31),  # Default end (configurable)
        update_frequency="daily"
    ),
    "normal_daily": NOAADataset(
        dataset_id="NORMAL_DLY",
        name="Climate Normals - Daily",
        description="30-year climate normals (daily)",
        data_types=[
            "DLY-TMAX-NORMAL",  # Daily max temp normal
            "DLY-TMIN-NORMAL",  # Daily min temp normal
            "DLY-TAVG-NORMAL",  # Daily avg temp normal
            "DLY-PRCP-PCTALL-GE001HI",  # Precipitation probability
        ],
        table_name="noaa_normals_daily",
        start_date=date(2010, 1, 1),  # Normals reference period
        end_date=date(2010, 12, 31),
        update_frequency="decade"
    ),
    "normal_monthly": NOAADataset(
        dataset_id="NORMAL_MLY",
        name="Climate Normals - Monthly",
        description="30-year climate normals (monthly)",
        data_types=[
            "MLY-TMAX-NORMAL",  # Monthly max temp normal
            "MLY-TMIN-NORMAL",  # Monthly min temp normal
            "MLY-TAVG-NORMAL",  # Monthly avg temp normal
            "MLY-PRCP-NORMAL",  # Monthly precipitation normal
        ],
        table_name="noaa_normals_monthly",
        start_date=date(2010, 1, 1),
        end_date=date(2010, 12, 31),
        update_frequency="decade"
    ),
    "gsom": NOAADataset(
        dataset_id="GSOM",
        name="Global Summary of the Month",
        description="Monthly climate summaries from weather stations",
        data_types=[
            "TMAX",
            "TMIN",
            "TAVG",
            "PRCP",
            "EMXT",  # Extreme maximum temperature
            "EMNT",  # Extreme minimum temperature
        ],
        table_name="noaa_gsom",
        start_date=date(2020, 1, 1),
        end_date=date(2024, 12, 31),
        update_frequency="monthly"
    ),
    "precip_hourly": NOAADataset(
        dataset_id="PRECIP_HLY",
        name="Precipitation Hourly",
        description="Hourly precipitation data",
        data_types=[
            "HPCP",  # Hourly precipitation
        ],
        table_name="noaa_precip_hourly",
        start_date=date(2023, 1, 1),
        end_date=date(2024, 12, 31),
        update_frequency="hourly"
    )
}


# Common NOAA data type definitions (for reference)
NOAA_DATA_TYPES = {
    # Temperature (Celsius or Fahrenheit depending on units parameter)
    "TMAX": {"name": "Maximum temperature", "units": "°C/°F"},
    "TMIN": {"name": "Minimum temperature", "units": "°C/°F"},
    "TAVG": {"name": "Average temperature", "units": "°C/°F"},
    
    # Precipitation (millimeters or inches)
    "PRCP": {"name": "Precipitation", "units": "mm/in"},
    "SNOW": {"name": "Snowfall", "units": "mm/in"},
    "SNWD": {"name": "Snow depth", "units": "mm/in"},
    
    # Wind (meters per second or miles per hour)
    "AWND": {"name": "Average wind speed", "units": "m/s/mph"},
    "WSF2": {"name": "Fastest 2-minute wind speed", "units": "m/s/mph"},
    "WSF5": {"name": "Fastest 5-second wind speed", "units": "m/s/mph"},
    
    # Extremes
    "EMXT": {"name": "Extreme maximum temperature", "units": "°C/°F"},
    "EMNT": {"name": "Extreme minimum temperature", "units": "°C/°F"},
    
    # Precipitation probability (normals)
    "DLY-PRCP-PCTALL-GE001HI": {"name": "Probability of >=0.01in precipitation", "units": "%"},
}


def get_table_schema(dataset_key: str) -> Dict[str, str]:
    """
    Generate SQL schema for a NOAA dataset table.
    
    Args:
        dataset_key: Key in NOAA_DATASETS dictionary
        
    Returns:
        Dictionary mapping column names to SQL types
    """
    dataset = NOAA_DATASETS.get(dataset_key)
    if not dataset:
        raise ValueError(f"Unknown dataset: {dataset_key}")
    
    # Base columns common to all NOAA data
    schema = {
        "date": "DATE NOT NULL",
        "datatype": "VARCHAR(50) NOT NULL",
        "station": "VARCHAR(50) NOT NULL",
        "value": "NUMERIC",
        "attributes": "VARCHAR(10)",  # Quality flags
        "location_id": "VARCHAR(50)",
        "location_name": "TEXT",
        "latitude": "NUMERIC",
        "longitude": "NUMERIC",
        "elevation": "NUMERIC",
        "ingestion_timestamp": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    }
    
    return schema


def get_primary_key(dataset_key: str) -> List[str]:
    """
    Get primary key columns for a dataset table.
    
    Args:
        dataset_key: Key in NOAA_DATASETS dictionary
        
    Returns:
        List of column names forming the primary key
    """
    return ["date", "datatype", "station"]




