"""Tests for EPAACRESCollector – datacenter site selection risk layer."""

import pytest
from unittest.mock import MagicMock
import xml.etree.ElementTree as ET

from app.sources.site_intel.risk.epa_acres_collector import EPAACRESCollector


SAMPLE_XML = """<?xml version="1.0" encoding="utf-8"?>
<frs_program_facilityList>
  <frs_program_facility>
    <STATE_NAME>TEXAS</STATE_NAME>
    <LOCATION_ADDRESS>123 MAIN ST</LOCATION_ADDRESS>
    <CITY_NAME>HOUSTON</CITY_NAME>
    <COUNTY_NAME>HARRIS</COUNTY_NAME>
    <REGISTRY_ID>110071970770</REGISTRY_ID>
    <PRIMARY_NAME>OLD CHEMICAL PLANT</PRIMARY_NAME>
    <POSTAL_CODE>77001</POSTAL_CODE>
    <STATE_CODE>TX</STATE_CODE>
    <PGM_SYS_ID>185125</PGM_SYS_ID>
    <PGM_SYS_ACRNM>ACRES</PGM_SYS_ACRNM>
    <SOURCE_OF_DATA>ACRES</SOURCE_OF_DATA>
    <EPA_REGION_CODE>06</EPA_REGION_CODE>
    <PUBLIC_IND>Y</PUBLIC_IND>
  </frs_program_facility>
  <frs_program_facility>
    <STATE_NAME>TEXAS</STATE_NAME>
    <LOCATION_ADDRESS>456 INDUSTRIAL BLVD</LOCATION_ADDRESS>
    <CITY_NAME>DALLAS</CITY_NAME>
    <COUNTY_NAME>DALLAS</COUNTY_NAME>
    <REGISTRY_ID>110039062313</REGISTRY_ID>
    <PRIMARY_NAME>REFINERY SITE</PRIMARY_NAME>
    <POSTAL_CODE>75201</POSTAL_CODE>
    <STATE_CODE>TX</STATE_CODE>
    <PGM_SYS_ID>97502</PGM_SYS_ID>
    <PGM_SYS_ACRNM>ACRES</PGM_SYS_ACRNM>
    <SOURCE_OF_DATA>ACRES</SOURCE_OF_DATA>
    <EPA_REGION_CODE>06</EPA_REGION_CODE>
    <PUBLIC_IND>Y</PUBLIC_IND>
  </frs_program_facility>
</frs_program_facilityList>"""


@pytest.mark.unit
class TestEPAACRESCollector:
    """Unit tests for EPA ACRES brownfield site collector."""

    def setup_method(self):
        self.mock_db = MagicMock()
        self.collector = EPAACRESCollector(db=self.mock_db)

    def test_parse_xml_valid(self):
        """Valid XML parses into brownfield records."""
        records = self.collector._parse_xml(SAMPLE_XML, "TX")
        assert len(records) == 2
        assert records[0]["acres_id"] == "185125"
        assert records[0]["site_name"] == "OLD CHEMICAL PLANT"
        assert records[0]["city"] == "HOUSTON"
        assert records[0]["county"] == "HARRIS"
        assert records[0]["state"] == "TX"
        assert records[0]["zip_code"] == "77001"

    def test_parse_xml_second_record(self):
        """Second record in XML is parsed correctly."""
        records = self.collector._parse_xml(SAMPLE_XML, "TX")
        assert records[1]["acres_id"] == "97502"
        assert records[1]["site_name"] == "REFINERY SITE"
        assert records[1]["city"] == "DALLAS"

    def test_parse_xml_sets_source(self):
        """All parsed records have source='epa_acres'."""
        records = self.collector._parse_xml(SAMPLE_XML, "TX")
        for r in records:
            assert r["source"] == "epa_acres"

    def test_parse_xml_invalid(self):
        """Invalid XML returns empty list."""
        records = self.collector._parse_xml("not xml at all", "TX")
        assert records == []

    def test_parse_xml_empty(self):
        """Empty facility list returns empty list."""
        xml = '<?xml version="1.0"?><frs_program_facilityList></frs_program_facilityList>'
        records = self.collector._parse_xml(xml, "TX")
        assert records == []

    def test_transform_facility_missing_id(self):
        """Facility with no PGM_SYS_ID or REGISTRY_ID returns None."""
        elem = ET.fromstring("""
        <frs_program_facility>
            <PRIMARY_NAME>Unknown Site</PRIMARY_NAME>
            <CITY_NAME>Houston</CITY_NAME>
            <PGM_SYS_ID>None</PGM_SYS_ID>
            <REGISTRY_ID>None</REGISTRY_ID>
        </frs_program_facility>
        """)
        result = self.collector._transform_facility(elem, "TX")
        assert result is None

    def test_transform_facility_falls_back_to_registry_id(self):
        """When PGM_SYS_ID is None, REGISTRY_ID is used as acres_id."""
        elem = ET.fromstring("""
        <frs_program_facility>
            <PRIMARY_NAME>Test Site</PRIMARY_NAME>
            <CITY_NAME>Austin</CITY_NAME>
            <PGM_SYS_ID>None</PGM_SYS_ID>
            <REGISTRY_ID>REG123</REGISTRY_ID>
            <LOCATION_ADDRESS>100 Main St</LOCATION_ADDRESS>
            <COUNTY_NAME>Travis</COUNTY_NAME>
            <POSTAL_CODE>73301</POSTAL_CODE>
        </frs_program_facility>
        """)
        result = self.collector._transform_facility(elem, "TX")
        assert result is not None
        assert result["acres_id"] == "REG123"

    def test_get_text_none_value(self):
        """_get_text returns None for XML elements with text 'None'."""
        elem = ET.fromstring("<root><field>None</field></root>")
        assert self.collector._get_text(elem, "field") is None

    def test_get_text_valid_value(self):
        """_get_text returns stripped text for valid elements."""
        elem = ET.fromstring("<root><field>  Houston  </field></root>")
        assert self.collector._get_text(elem, "field") == "Houston"

    def test_get_text_missing_element(self):
        """_get_text returns None for missing elements."""
        elem = ET.fromstring("<root></root>")
        assert self.collector._get_text(elem, "field") is None

    def test_collector_attributes(self):
        """Collector has correct domain and source attributes."""
        from app.sources.site_intel.types import SiteIntelDomain, SiteIntelSource
        assert self.collector.domain == SiteIntelDomain.RISK
        assert self.collector.source == SiteIntelSource.EPA_ACRES
